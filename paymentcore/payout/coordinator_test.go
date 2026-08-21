package payout

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
	c := &SagaCoordinator{ledgerTimeout: time.Minute, settlementTimeout: 10 * time.Minute, complianceTimeout: 24 * time.Hour}
	tests := []struct {
		state       State
		event       string
		wantState   State
		wantCommand string
	}{
		{StateAwaitingPolicy, "payout.policy.approved", StateAwaitingLedger, "ledger.reserve"},
		{StateAwaitingPolicy, "payout.policy.rejected", StateFailed, "payment.fail"},
		{StateAwaitingLedger, "payout.funds_reserved", StateAwaitingSettlement, "settlement.execute"},
		{StateAwaitingLedger, "payout.ledger_failed", StateFailed, "payment.fail"},
		{StateAwaitingSettlement, "payout.provider_completed", StateSettlingPayment, "payment.settle"},
		{StateAwaitingSettlement, "payout.on_hold", StateOnHold, ""},
		{StateOnHold, "payout.provider_completed", StateSettlingPayment, "payment.settle"},
		{StateOnHold, "payout.provider_failed", StateFailed, "payment.fail_reserved"},
		{StateOnHold, "payout.provider_returned", StateReturning, "ledger.release"},
		{StateSettlingPayment, "payout.completed", StateCompleted, ""},
		{StateSettlingPayment, "payout.provider_completed", StateSettlingPayment, ""},
		{StateAwaitingSettlement, "payout.provider_failed", StateFailed, "payment.fail_reserved"},
		{StateAwaitingSettlement, "payout.provider_returned", StateReturning, "ledger.release"},
		{StateFailed, "payout.provider_returned", StateReturning, "ledger.release"},
		{StateFailed, "payout.failed", StateFailed, ""},
		{StateReleasingLedger, "payout.funds_released", StateLedgerReleased, "payment.fail"},
		{StateReturning, "payout.funds_released", StateReturned, "payment.return"},
	}
	for _, tt := range tests {
		next, command, _, _, err := c.transition(tt.state, tt.event, "")
		if err != nil || next != tt.wantState || command != tt.wantCommand {
			t.Errorf("transition(%s, %s) = (%s, %s, %v)", tt.state, tt.event, next, command, err)
		}
	}
	if _, _, _, _, err := c.transition(StateCompleted, "payout.policy.approved", ""); err == nil {
		t.Fatal("expected invalid transition error")
	}
	next, command, _, _, err := c.transition(StateAwaitingSettlement, "payout.provider_failed", "submission_failed")
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
	c := testSagaCoordinator(t, db)
	event := sagaEvent("payout.created", "evt-created", nil)
	now := c.now()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO settlement_sagas").
		WithArgs("saga_1", "pay-1", "corr_2", StateAwaitingPolicy, now.Add(time.Minute), now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_3", eventbus.SettlementCommandsTopic, "policy.evaluate", eventbus.PolicyEvaluateVersion, "pay-1", sqlmock.AnyArg(), now).
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
	c := testSagaCoordinator(t, db)
	event := sagaEvent("payout.policy.approved", "evt-approved", map[string]string{"correlation_id": "wrong"})
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, correlation_id, state FROM settlement_sagas").WithArgs("pay-1").
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

func TestExpireOnceRetriesSettlementWithReservedFunds(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()
	c := testSagaCoordinator(t, db)
	now := c.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, payment_id, correlation_id, state").WithArgs(now, 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "payment_id", "correlation_id", "state"}).
			AddRow("saga-1", "pay-1", "corr-1", StateAwaitingSettlement))
	mock.ExpectExec("UPDATE settlement_sagas").
		WithArgs(StateAwaitingSettlement, now.Add(c.settlementTimeout), "awaiting_settlement timeout", now, "saga-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_1", eventbus.SettlementCommandsTopic, "settlement.execute", eventbus.SettlementExecuteVersion, "pay-1", sqlmock.AnyArg(), now).
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
	c := testSagaCoordinator(t, db)
	now := c.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, payment_id, correlation_id, state").WithArgs(now, 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "payment_id", "correlation_id", "state"}).
			AddRow("saga-1", "pay-1", "corr-1", StateReturning))
	mock.ExpectExec("UPDATE settlement_sagas").
		WithArgs(StateReturning, now.Add(time.Minute), "returning timeout", now, "saga-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_1", eventbus.SettlementCommandsTopic, "ledger.release", eventbus.LedgerReleaseVersion, "pay-1", sqlmock.AnyArg(), now).
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
	c := testSagaCoordinator(t, db)
	now := c.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id,correlation_id,state FROM settlement_sagas").WithArgs("pay-1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "correlation_id", "state"}).AddRow("saga-1", "corr-1", StateManualReview))
	mock.ExpectExec("INSERT INTO saga_manual_review_actions").WithArgs("saga-1", "return", "alice", "provider confirmed return", now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE settlement_sagas").WithArgs(StateReturning, now.Add(time.Minute), "provider confirmed return", now, "saga-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_1", eventbus.SettlementCommandsTopic, "ledger.release", eventbus.LedgerReleaseVersion, "pay-1", sqlmock.AnyArg(), now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := c.ResolveManualReview(context.Background(), "pay-1", "return", "alice", "provider confirmed return"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func testSagaCoordinator(t *testing.T, db *sql.DB) *SagaCoordinator {
	t.Helper()
	c, err := NewSagaCoordinator(db, SagaConfig{TimeoutBatchSize: 10})
	if err != nil {
		t.Fatalf("NewSagaCoordinator: %v", err)
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
	return eventbus.Event{ID: id, Type: eventType, Version: 1, AggregateID: "pay-1", AggregateType: "payout", OccurredAt: time.Now().UTC(), Payload: encoded}
}
