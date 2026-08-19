package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
)

type recordingProducer struct {
	topics []eventbus.Topic
	events []eventbus.Event
	err    error
}

func (p *recordingProducer) Publish(_ context.Context, topic eventbus.Topic, event eventbus.Event) error {
	if p.err != nil && topic != eventbus.DeadLetterTopic {
		return p.err
	}
	p.topics = append(p.topics, topic)
	p.events = append(p.events, event)
	return nil
}

func (p *recordingProducer) Close() error { return nil }

func TestRelayOncePublishesAndMarksClaimedEvents(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	producer := &recordingProducer{}
	relay := testRelay(t, db, producer, 2)
	occurredAt := time.Date(2026, time.August, 8, 10, 0, 0, 0, time.UTC)
	publishedAt := relay.now()
	createdAt := occurredAt.Add(-time.Minute)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT candidate.id").
		WithArgs(2, publishedAt).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "topic", "event_type", "event_version", "aggregate_id",
			"aggregate_type", "payload", "occurred_at", "attempt_count", "created_at",
		}).AddRow(
			"evt-1", "payout-events", "payment.created", 1, "pay-1",
			"payment", []byte(`{"amount_minor":2500}`), occurredAt, 0, createdAt,
		).AddRow(
			"evt-2", "payout-events", "payment.created", 1, "pay-2",
			"payment", []byte(`{"amount_minor":5000}`), occurredAt, 0, createdAt,
		))
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(publishedAt, "evt-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(publishedAt, "evt-2").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	count, err := relay.RelayOnce(context.Background())
	if err != nil {
		t.Fatalf("RelayOnce returned error: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 relayed events, got %d", count)
	}
	if len(producer.events) != 2 || producer.events[0].ID != "evt-1" || producer.events[1].ID != "evt-2" {
		t.Fatalf("unexpected published events: %+v", producer.events)
	}
	if producer.topics[0] != eventbus.PayoutEventsTopic {
		t.Fatalf("unexpected topic: %q", producer.topics[0])
	}
	if !json.Valid(producer.events[0].Payload) {
		t.Fatalf("invalid published payload: %s", producer.events[0].Payload)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestRelayOnceSchedulesRetryWhenPublishFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	producer := &recordingProducer{err: errors.New("broker unavailable")}
	relay := testRelay(t, db, producer, 10)
	relay.jitter = func(delay time.Duration) time.Duration { return delay }
	now := relay.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT candidate.id").
		WithArgs(10, now).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "topic", "event_type", "event_version", "aggregate_id",
			"aggregate_type", "payload", "occurred_at", "attempt_count", "created_at",
		}).AddRow(
			"evt-1", "payout-events", "payment.created", 1, "pay-1",
			"payment", []byte(`{}`), now, 1, now.Add(-time.Minute),
		))
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(2, "broker unavailable", now.Add(2*time.Second), "evt-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	count, err := relay.RelayOnce(context.Background())
	if err != nil || count != 0 {
		t.Fatalf("RelayOnce = (%d, %v), want (0, nil)", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestRelayOnceCommitsEmptyBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	relay := testRelay(t, db, &recordingProducer{}, 5)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT candidate.id").WithArgs(5, relay.now()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "topic", "event_type", "event_version", "aggregate_id",
			"aggregate_type", "payload", "occurred_at", "attempt_count", "created_at",
		}))
	mock.ExpectCommit()

	count, err := relay.RelayOnce(context.Background())
	if err != nil || count != 0 {
		t.Fatalf("RelayOnce = (%d, %v), want (0, nil)", count, err)
	}
}

func TestNewRelayValidatesDependenciesAndAppliesDefaults(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	producer := &recordingProducer{}
	if _, err := NewRelay(nil, producer, Config{}); err == nil {
		t.Fatal("expected missing database error")
	}
	if _, err := NewRelay(db, nil, Config{}); err == nil {
		t.Fatal("expected missing producer error")
	}
	relay, err := NewRelay(db, producer, Config{})
	if err != nil {
		t.Fatalf("NewRelay returned error: %v", err)
	}
	if relay.batchSize != defaultBatchSize || relay.pollInterval != defaultPollInterval {
		t.Fatalf("defaults not applied: batch=%d interval=%s", relay.batchSize, relay.pollInterval)
	}
	if relay.initialBackoff != defaultInitialBackoff || relay.maxBackoff != defaultMaxBackoff ||
		relay.maxAttempts != defaultMaxAttempts || relay.maxAge != defaultMaxAge {
		t.Fatal("retry defaults not applied")
	}
}

func TestRelayOnceMarksEventFailedAtAttemptLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()
	relay := testRelay(t, db, &recordingProducer{err: errors.New("rejected")}, 1)
	relay.maxAttempts = 3
	now := relay.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT candidate.id").WithArgs(1, now).
		WillReturnRows(sqlmock.NewRows([]string{"id", "topic", "event_type", "event_version", "aggregate_id", "aggregate_type", "payload", "occurred_at", "attempt_count", "created_at"}).
			AddRow("evt-1", "payout-events", "payment.created", 1, "pay-1", "payment", []byte(`{}`), now, 2, now.Add(-time.Minute)))
	mock.ExpectExec("UPDATE outbox_events").WithArgs(3, "rejected", now, "evt-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if count, err := relay.RelayOnce(context.Background()); err != nil || count != 0 {
		t.Fatalf("RelayOnce = (%d, %v), want (0, nil)", count, err)
	}
	producer := relay.producer.(*recordingProducer)
	if len(producer.events) != 1 || producer.topics[0] != eventbus.DeadLetterTopic {
		t.Fatalf("unexpected dead-letter publications: topics=%v events=%v", producer.topics, producer.events)
	}
	var payload DeadLetterPayload
	if err := json.Unmarshal(producer.events[0].Payload, &payload); err != nil {
		t.Fatalf("decode dead-letter payload: %v", err)
	}
	if payload.OriginalEvent.ID != "evt-1" || payload.OriginalTopic != "payout-events" || payload.AttemptCount != 3 || payload.LastError != "rejected" {
		t.Fatalf("unexpected dead-letter payload: %+v", payload)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestRedriveResetsDeadLetteredEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()
	relay := testRelay(t, db, &recordingProducer{}, 1)
	mock.ExpectExec("UPDATE outbox_events").WithArgs(relay.now(), "evt-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := relay.Redrive(context.Background(), "evt-1"); err != nil {
		t.Fatalf("Redrive returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestRedriveRejectsEventThatIsNotDeadLettered(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()
	relay := testRelay(t, db, &recordingProducer{}, 1)
	mock.ExpectExec("UPDATE outbox_events").WithArgs(relay.now(), "evt-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := relay.Redrive(context.Background(), "evt-1"); !errors.Is(err, ErrEventNotDeadLettered) {
		t.Fatalf("Redrive error = %v, want ErrEventNotDeadLettered", err)
	}
}

func testRelay(t *testing.T, db *sql.DB, producer eventbus.Producer, batchSize int) *Relay {
	t.Helper()
	relay, err := NewRelay(db, producer, Config{BatchSize: batchSize, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("NewRelay returned error: %v", err)
	}
	fixedTime := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return fixedTime }
	return relay
}
