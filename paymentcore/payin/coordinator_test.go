package payin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"stablerail/eventbus"
	"stablerail/saga"
)

func TestPayinSagaStartsWithPolicyCommand(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	coordinator, _ := NewSagaCoordinator(db)
	coordinator.now = func() time.Time { return now }
	ids := []string{"psaga_1", "corr_1", "evt_command"}
	coordinator.newID = func(string) (string, error) {
		id := ids[0]
		ids = ids[1:]
		return id, nil
	}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO payin_sagas").WithArgs("psaga_1", "pin_1", "corr_1", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_command", saga.CommandTopic, "payin.policy.evaluate", eventbus.PayinPolicyEvaluateVersion, "pin_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	event := eventbus.Event{ID: "evt_created", Type: "payin.created", Version: 1, AggregateID: "pin_1", AggregateType: "payin", OccurredAt: now, Payload: json.RawMessage(`{"payin_id":"pin_1"}`)}
	if err := coordinator.Handle(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPayinSagaRecordsSucceededResult(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	coordinator, _ := NewSagaCoordinator(db)
	coordinator.now = func() time.Time { return now }
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id,correlation_id,state FROM payin_sagas").WithArgs("pin_1").WillReturnRows(sqlmock.NewRows([]string{"id", "correlation_id", "state"}).AddRow("psaga_1", "corr_1", "awaiting_ledger"))
	mock.ExpectExec("UPDATE payin_sagas SET state").WithArgs("completed", "", now, "psaga_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	event := eventbus.Event{ID: "evt_succeeded", Type: "payin.succeeded", Version: 1, AggregateID: "pin_1", AggregateType: "payin", OccurredAt: now, Payload: json.RawMessage(`{"correlation_id":"corr_1"}`)}
	if err := coordinator.Handle(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPayinReceivedEnqueuesLedgerCommand(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	coordinator, _ := NewSagaCoordinator(db)
	coordinator.now = func() time.Time { return now }
	coordinator.newID = func(string) (string, error) { return "evt_ledger", nil }
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id,correlation_id,state FROM payin_sagas").WithArgs("pin_1").WillReturnRows(sqlmock.NewRows([]string{"id", "correlation_id", "state"}).AddRow("psaga_1", "corr_1", "processing"))
	mock.ExpectExec("UPDATE payin_sagas SET state").WithArgs("awaiting_ledger", "", now, "psaga_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_ledger", saga.CommandTopic, "payin.ledger.record", eventbus.PayinLedgerRecordVersion, "pin_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	event := eventbus.Event{ID: "evt_received", Type: "payin.received", Version: 1, AggregateID: "pin_1", AggregateType: "payin", OccurredAt: now, Payload: json.RawMessage(`{"correlation_id":"corr_1"}`)}
	if err := coordinator.Handle(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
