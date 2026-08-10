package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"stablerail/eventbus"
)

type fakeReader struct {
	message kafka.Message
	commits int
	fetched bool
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if !r.fetched {
		r.fetched = true
		return r.message, nil
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}
func (r *fakeReader) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	r.commits++
	return nil
}
func (*fakeReader) Close() error { return nil }

type fakeProcessor struct {
	attempts int
	err      error
	cancel   context.CancelFunc
}

func (p *fakeProcessor) Process(_ context.Context, _ string, _ eventbus.Event) (bool, error) {
	p.attempts++
	if p.attempts == 2 && p.cancel != nil {
		p.cancel()
	}
	if p.attempts == 1 {
		return false, p.err
	}
	return true, nil
}

func TestLoopRetriesBeforeCommit(t *testing.T) {
	e := eventbus.Event{ID: "e", Type: "t", Version: 1, AggregateID: "p", AggregateType: "payment", OccurredAt: time.Now(), Payload: json.RawMessage(`{}`)}
	value, _ := json.Marshal(e)
	ctx, cancel := context.WithCancel(context.Background())
	p := &fakeProcessor{err: errors.New("temporary"), cancel: cancel}
	r := &fakeReader{message: kafka.Message{Value: value}}
	l := Loop{Reader: r, Processor: p, Consumer: "test", RetryBackoff: time.Millisecond}
	if err := l.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p.attempts != 2 || r.commits != 1 {
		t.Fatalf("attempts=%d commits=%d", p.attempts, r.commits)
	}
}

func TestLoopCommitsPermanentDecodeFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &fakeReader{message: kafka.Message{Value: []byte("bad")}}
	p := &fakeProcessor{}
	l := Loop{Reader: r, Processor: p, Consumer: "test", OnPermanent: func(error) { cancel() }}
	_ = l.Run(ctx)
	if r.commits != 1 {
		t.Fatalf("commits=%d", r.commits)
	}
}
