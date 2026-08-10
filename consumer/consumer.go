// Package consumer provides the reusable Kafka consumption runtime.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"stablerail/eventbus"
)

type Reader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}
type Processor interface {
	Process(context.Context, string, eventbus.Event) (bool, error)
}

type permanentError struct{ error }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}
func IsPermanent(err error) bool { var target permanentError; return errors.As(err, &target) }

type Loop struct {
	Reader       Reader
	Processor    Processor
	Consumer     string
	RetryBackoff time.Duration
	OnPermanent  func(error)
}

func (l *Loop) Run(ctx context.Context) error {
	if l.Reader == nil || l.Processor == nil || l.Consumer == "" {
		return errors.New("consumer reader, processor, and name are required")
	}
	backoff := l.RetryBackoff
	if backoff <= 0 {
		backoff = time.Second
	}
	defer l.Reader.Close()
	for {
		message, err := l.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("fetch Kafka message: %w", err)
		}
		var event eventbus.Event
		err = json.Unmarshal(message.Value, &event)
		if err != nil {
			err = Permanent(fmt.Errorf("decode event: %w", err))
		} else if validateErr := event.Validate(); validateErr != nil {
			err = Permanent(fmt.Errorf("validate event: %w", validateErr))
		}
		for err == nil || !IsPermanent(err) {
			_, err = l.Processor.Process(ctx, l.Consumer, event)
			if err == nil || IsPermanent(err) {
				break
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		permanentErr := err
		if err := l.Reader.CommitMessages(ctx, message); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("commit Kafka message: %w", err)
		}
		if permanentErr != nil && l.OnPermanent != nil {
			l.OnPermanent(permanentErr)
		}
	}
}
