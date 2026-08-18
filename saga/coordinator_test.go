package saga

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
)

func TestWorkflowTransitions(t *testing.T) {
	c := &Coordinator{ledgerTimeout: time.Minute, settlementTimeout: 10 * time.Minute, complianceTimeout: 24 * time.Hour}
	tests := []struct {
		state       State
		event       string
		wantState   State
		wantCommand string
	}{
		{StateAwaitingPolicy, "policy.approved", StateAwaitingLedger, "ledger.reserve"},
		{StateAwaitingPolicy, "policy.rejected", StateFailed, "payment.fail"},
		{StateAwaitingLedger, "ledger.reserved", StateAwaitingSettlement, "settlement.execute"},
		{StateAwaitingLedger, "ledger.failed", StateFailed, "payment.fail"},
		{StateAwaitingSettlement, "settlement.completed", StateSettlingPayment, "payment.settle"},
		{StateAwaitingSettlement, "settlement.on_hold", StateOnHold, ""},
		{StateOnHold, "settlement.completed", StateSettlingPayment, "payment.settle"},
		{StateOnHold, "settlement.failed", StateFailed, "payment.fail_reserved"},
		{StateOnHold, "settlement.returned", StateReturning, "ledger.release"},
		{StateSettlingPayment, "payment.succeeded", StateCompleted, ""},
		{StateAwaitingSettlement, "settlement.failed", StateFailed, "payment.fail_reserved"},
		{StateAwaitingSettlement, "settlement.returned", StateReturning, "ledger.release"},
		{StateFailed, "settlement.returned", StateReturning, "ledger.release"},
		{StateReleasingLedger, "ledger.released", StateLedgerReleased, "payment.fail"},
		{StateReturning, "ledger.released", StateReturned, "payment.return"},
	}
	for _, tt := range tests {
		next, command, _, _, err := c.transition(tt.state, tt.event, "")
		if err != nil || next != tt.wantState || command != tt.wantCommand {
			t.Errorf("transition(%s, %s) = (%s, %s, %v)", tt.state, tt.event, next, command, err)
		}
	}
	if _, _, _, _, err := c.transition(StateCompleted, "policy.approved", ""); err == nil {
		t.Fatal("expected invalid transition error")
	}
	next, command, _, _, err := c.transition(StateAwaitingSettlement, "settlement.failed", "submission_failed")
	if err != nil || next != StateFailed || command != "payment.fail" {
		t.Fatalf("submission failure transition = (%s, %s, %v)", next, command, err)
	}
}

func TestHandleStartsSagaAndEnqueuesPolicyCommand(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()
	c := testCoordinator(t, db)
	event := sagaEvent("payment.created", "evt-created", nil)
	now := c.now()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO payment_sagas").
		WithArgs("saga_1", "pay-1", "corr_2", StateAwaitingPolicy, now.Add(time.Minute), now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_3", CommandTopic, "policy.evaluate", eventbus.PolicyEvaluateVersion, "pay-1", sqlmock.AnyArg(), now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if err := c.Handle(context.Background(), tx, event); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestHandleRejectsMismatchedCorrelation(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()
	c := testCoordinator(t, db)
	event := sagaEvent("policy.approved", "evt-approved", map[string]string{"correlation_id": "wrong"})
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, correlation_id, state FROM payment_sagas").WithArgs("pay-1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "correlation_id", "state"}).AddRow("saga-1", "corr-1", StateAwaitingPolicy))
	mock.ExpectRollback()
	tx, _ := db.BeginTx(context.Background(), nil)
	err = c.Handle(context.Background(), tx, event)
	if err == nil {
		t.Fatal("expected correlation mismatch")
	}
	tx.Rollback()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestExpireOncePreservesReservedFundsAfterSettlementTimeout(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()
	c := testCoordinator(t, db)
	now := c.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, payment_id, correlation_id, state").WithArgs(now, 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "payment_id", "correlation_id", "state"}).
			AddRow("saga-1", "pay-1", "corr-1", StateAwaitingSettlement))
	mock.ExpectExec("UPDATE payment_sagas").
		WithArgs(StateFailed, nil, "awaiting_settlement timeout", now, "saga-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_1", CommandTopic, "payment.fail_reserved", eventbus.PaymentFailVersion, "pay-1", sqlmock.AnyArg(), now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	count, err := c.ExpireOnce(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ExpireOnce = (%d, %v)", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestExpireOnceRetriesReturnWhoseLedgerReleaseTimedOut(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := testCoordinator(t, db)
	now := c.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, payment_id, correlation_id, state").WithArgs(now, 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "payment_id", "correlation_id", "state"}).
			AddRow("saga-1", "pay-1", "corr-1", StateReturning))
	mock.ExpectExec("UPDATE payment_sagas").
		WithArgs(StateReturning, now.Add(time.Minute), "returning timeout", now, "saga-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_1", CommandTopic, "ledger.release", eventbus.LedgerReleaseVersion, "pay-1", sqlmock.AnyArg(), now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	count, err := c.ExpireOnce(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ExpireOnce = (%d, %v)", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResolveManualReviewReturnsPayment(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := testCoordinator(t, db)
	now := c.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id,correlation_id,state FROM payment_sagas").WithArgs("pay-1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "correlation_id", "state"}).AddRow("saga-1", "corr-1", StateManualReview))
	mock.ExpectExec("INSERT INTO saga_manual_review_actions").WithArgs("saga-1", "return", "alice", "provider confirmed return", now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE payment_sagas").WithArgs(StateReturning, now.Add(time.Minute), "provider confirmed return", now, "saga-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_1", CommandTopic, "ledger.release", eventbus.LedgerReleaseVersion, "pay-1", sqlmock.AnyArg(), now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := c.ResolveManualReview(context.Background(), "pay-1", "return", "alice", "provider confirmed return"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func testCoordinator(t *testing.T, db *sql.DB) *Coordinator {
	t.Helper()
	c, err := NewCoordinator(db, Config{TimeoutBatchSize: 10})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	c.now = func() time.Time { return time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC) }
	n := 0
	c.newID = func(prefix string) (string, error) { n++; return prefix + string(rune('0'+n)), nil }
	return c
}

func sagaEvent(eventType, id string, payload map[string]string) eventbus.Event {
	if payload == nil {
		payload = map[string]string{}
	}
	encoded, _ := json.Marshal(payload)
	return eventbus.Event{ID: id, Type: eventType, Version: 1, AggregateID: "pay-1", AggregateType: "payment", OccurredAt: time.Now().UTC(), Payload: encoded}
}
