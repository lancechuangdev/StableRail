package payin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"stablerail/eventbus"
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
	ids := []string{"evt_command"}
	coordinator.newID = func(string) (string, error) {
		id := ids[0]
		ids = ids[1:]
		return id, nil
	}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO settlement_sagas").WithArgs("pay_1", now.Add(10*time.Minute), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_command", eventbus.SettlementCommandsTopic, "payin.policy.evaluate", eventbus.PayinPolicyEvaluateVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	event := eventbus.Event{ID: "evt_created", Type: "payin.created", Version: 1, AggregateID: "pay_1", AggregateType: "payin", OccurredAt: now, Payload: json.RawMessage(`{"payin_id":"pay_1"}`)}
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
	mock.ExpectQuery("SELECT state FROM settlement_sagas").WithArgs("pay_1").WillReturnRows(sqlmock.NewRows([]string{"state"}).AddRow("awaiting_ledger"))
	mock.ExpectExec("UPDATE settlement_sagas SET state").WithArgs("completed", nil, "", now, "pay_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	event := eventbus.Event{ID: "evt_succeeded", Type: "payin.succeeded", Version: 1, AggregateID: "pay_1", AggregateType: "payin", OccurredAt: now, Payload: json.RawMessage(`{}`)}
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
	mock.ExpectQuery("SELECT state FROM settlement_sagas").WithArgs("pay_1").WillReturnRows(sqlmock.NewRows([]string{"state"}).AddRow("processing"))
	mock.ExpectExec("UPDATE settlement_sagas SET state").WithArgs("awaiting_ledger", now.Add(10*time.Minute), "", now, "pay_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_ledger", eventbus.SettlementCommandsTopic, "payin.ledger.record", eventbus.PayinLedgerRecordVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	event := eventbus.Event{ID: "evt_received", Type: "payin.received", Version: 1, AggregateID: "pay_1", AggregateType: "payin", OccurredAt: now, Payload: json.RawMessage(`{}`)}
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

func TestPayinLedgerTimeoutRetriesLedgerCommand(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	coordinator, _ := NewSagaCoordinator(db)
	coordinator.now = func() time.Time { return now }
	coordinator.newID = func(string) (string, error) { return "evt_retry", nil }
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT s.payment_id,s.state FROM settlement_sagas").WithArgs(now, 100).WillReturnRows(sqlmock.NewRows([]string{"payment_id", "state"}).AddRow("pay_1", "awaiting_ledger"))
	mock.ExpectExec("UPDATE settlement_sagas SET deadline_at").WithArgs(now.Add(10*time.Minute), "awaiting_ledger timeout", now, "pay_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_retry", eventbus.SettlementCommandsTopic, "payin.ledger.record", eventbus.PayinLedgerRecordVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	count, err := coordinator.ExpireOnce(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ExpireOnce = (%d, %v)", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
