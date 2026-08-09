package inbox

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
)

func TestProcessRecordsEventAndCommitsHandlerState(t *testing.T) {
	processor, mock, closeDB := testProcessor(t)
	defer closeDB()
	event := validEvent()
	receivedAt := processor.now()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO inbox_events").
		WithArgs("settlement", event.ID, event.Type, event.Version, event.AggregateID, event.AggregateType, event.OccurredAt, receivedAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE settlements").WithArgs(event.AggregateID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	called := false
	processed, err := processor.Process(context.Background(), "settlement", event, func(ctx context.Context, tx *sql.Tx, got eventbus.Event) error {
		called = true
		if got.ID != event.ID {
			t.Fatalf("handler event ID = %q, want %q", got.ID, event.ID)
		}
		_, err := tx.ExecContext(ctx, "UPDATE settlements SET applied = TRUE WHERE payment_id = $1", got.AggregateID)
		return err
	})
	if err != nil || !processed || !called {
		t.Fatalf("Process = (%v, %v), handler called=%v", processed, err, called)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestProcessSkipsDuplicateForSameConsumer(t *testing.T) {
	processor, mock, closeDB := testProcessor(t)
	defer closeDB()
	event := validEvent()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO inbox_events").
		WithArgs("settlement", event.ID, event.Type, event.Version, event.AggregateID, event.AggregateType, event.OccurredAt, processor.now()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	processed, err := processor.Process(context.Background(), "settlement", event, func(context.Context, *sql.Tx, eventbus.Event) error {
		t.Fatal("handler called for duplicate event")
		return nil
	})
	if err != nil || processed {
		t.Fatalf("Process = (%v, %v), want (false, nil)", processed, err)
	}
}

func TestProcessRollsBackInboxRecordWhenHandlerFails(t *testing.T) {
	processor, mock, closeDB := testProcessor(t)
	defer closeDB()
	event := validEvent()
	handlerErr := errors.New("state update failed")

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO inbox_events").
		WithArgs("settlement", event.ID, event.Type, event.Version, event.AggregateID, event.AggregateType, event.OccurredAt, processor.now()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	processed, err := processor.Process(context.Background(), "settlement", event, func(context.Context, *sql.Tx, eventbus.Event) error {
		return handlerErr
	})
	if processed || !errors.Is(err, handlerErr) {
		t.Fatalf("Process = (%v, %v), want handler failure", processed, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestProcessAllowsSameEventForDifferentConsumers(t *testing.T) {
	processor, mock, closeDB := testProcessor(t)
	defer closeDB()
	event := validEvent()
	for _, consumer := range []string{"settlement", "notification"} {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO inbox_events").
			WithArgs(consumer, event.ID, event.Type, event.Version, event.AggregateID, event.AggregateType, event.OccurredAt, processor.now()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		processed, err := processor.Process(context.Background(), consumer, event, func(context.Context, *sql.Tx, eventbus.Event) error { return nil })
		if err != nil || !processed {
			t.Fatalf("Process for %s = (%v, %v)", consumer, processed, err)
		}
	}
}

func testProcessor(t *testing.T) (*Processor, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	processor, err := NewProcessor(db)
	if err != nil {
		db.Close()
		t.Fatalf("NewProcessor returned error: %v", err)
	}
	fixedTime := time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC)
	processor.now = func() time.Time { return fixedTime }
	return processor, mock, func() { db.Close() }
}

func validEvent() eventbus.Event {
	return eventbus.Event{
		ID: "evt-1", Type: "payment.processing", Version: 1,
		AggregateID: "pay-1", AggregateType: "payment",
		OccurredAt: time.Date(2026, time.August, 9, 11, 0, 0, 0, time.UTC),
		Payload:    []byte(`{"state":"processing"}`),
	}
}
