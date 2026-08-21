package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
	"stablerail/ledger"
	"stablerail/paymentcore"
	"stablerail/paymentcore/payin"
	"stablerail/policy"
)

type submissionFailurePayoutService struct {
	appliedPaymentID, appliedCommandID string
	appliedResult                      paymentcore.ExecutionResult
}

func (submissionFailurePayoutService) ExecutePayout(context.Context, string) (paymentcore.ExecutionResult, error) {
	return paymentcore.ExecutionResult{}, &paymentcore.ProviderError{Message: "insufficient balance", Code: "submission_failed", Retryable: false}
}
func (s *submissionFailurePayoutService) ApplyResult(_ context.Context, _ *sql.Tx, paymentID, commandID string, result paymentcore.ExecutionResult, _ time.Time) error {
	s.appliedPaymentID, s.appliedCommandID, s.appliedResult = paymentID, commandID, result
	return nil
}

type unusedLedgerService struct{}
type unusedPayinService struct{}

func (unusedPayinService) ExecutePayin(context.Context, string) (payin.ExecuteResult, error) {
	return payin.ExecuteResult{}, errors.New("unused")
}
func (unusedPayinService) ApplyResult(context.Context, *sql.Tx, string, payin.ExecuteResult, time.Time) error {
	return errors.New("unused")
}

type recordingPayinService struct {
	executedID, appliedID string
}

func (s *recordingPayinService) ExecutePayin(_ context.Context, id string) (payin.ExecuteResult, error) {
	s.executedID = id
	return payin.ExecuteResult{ExecutionResult: paymentcore.ExecutionResult{ProviderReference: "pi_1", Status: paymentcore.ExecutionPending}}, nil
}
func (s *recordingPayinService) ApplyResult(_ context.Context, _ *sql.Tx, id string, _ payin.ExecuteResult, _ time.Time) error {
	s.appliedID = id
	return nil
}

func (unusedLedgerService) Reserve(context.Context, *sql.Tx, ledger.ReservationRequest) error {
	return nil
}
func (unusedLedgerService) Release(context.Context, *sql.Tx, ledger.ReleaseRequest) error { return nil }
func (unusedLedgerService) Settle(context.Context, *sql.Tx, ledger.SettlementRequest) error {
	return nil
}
func (unusedLedgerService) RecordPayin(context.Context, *sql.Tx, ledger.PayinReceiptRequest) error {
	return nil
}
func (unusedLedgerService) RecordReturn(context.Context, *sql.Tx, ledger.ReturnRequest) error {
	return nil
}

func TestSettlementSubmissionFailureDelegatesResultApplication(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 17, 12, 0, 0, 0, time.UTC)
	payouts := &submissionFailurePayoutService{}
	handler := NewCommandHandler(policy.DeterministicEvaluator{}, unusedLedgerService{}, payouts, unusedPayinService{})
	handler.now = func() time.Time { return now }
	handler.newID = func() (string, error) { return "evt_result", nil }
	payload, _ := json.Marshal(commandPayload{PaymentID: "pay_1"})
	event := eventbus.Event{ID: "evt_command", Type: "settlement.execute", Version: 1, AggregateID: "pay_1", AggregateType: "payment", OccurredAt: now, Payload: payload}

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if payouts.appliedPaymentID != "pay_1" || payouts.appliedCommandID != "evt_command" || payouts.appliedResult.Status != paymentcore.ExecutionFailed || payouts.appliedResult.FailureCode != "submission_failed" {
		t.Fatalf("applied payout result = %+v", payouts)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPayinExecuteUsesPayinApplicationService(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service := &recordingPayinService{}
	handler := NewCommandHandler(policy.DeterministicEvaluator{}, unusedLedgerService{}, &submissionFailurePayoutService{}, service)
	payload, _ := json.Marshal(commandPayload{PayinID: "pay_1"})
	event := eventbus.Event{ID: "evt_command", Type: "payin.execute", Version: 1, AggregateID: "pay_1", AggregateType: "payin", OccurredAt: time.Now().UTC(), Payload: payload}
	mock.ExpectBegin()
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	if err := handler.Handle(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if service.executedID != "pay_1" || service.appliedID != "pay_1" {
		t.Fatalf("service=%+v", service)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPayinPolicyApprovalEnqueuesExecutionProgress(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	handler := NewCommandHandler(policy.DeterministicEvaluator{}, unusedLedgerService{}, &submissionFailurePayoutService{}, unusedPayinService{})
	handler.now = func() time.Time { return now }
	handler.newID = func() (string, error) { return "evt_policy_result", nil }
	payload, _ := json.Marshal(commandPayload{PayinID: "pay_1"})
	event := eventbus.Event{ID: "evt_policy", Type: "payin.policy.evaluate", Version: 1, AggregateID: "pay_1", AggregateType: "payin", OccurredAt: now, Payload: payload}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT source_amount_minor,source_currency FROM payins").WithArgs("pay_1").WillReturnRows(sqlmock.NewRows([]string{"amount", "currency"}).AddRow(int64(1000), "USD"))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_policy_result", eventbus.PayinEventsTopic, "payin.policy.approved", eventbus.PayinPolicyApprovedVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	if err := handler.Handle(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLateReturnRecordsReleasedPaymentReturn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	handler := NewCommandHandler(policy.DeterministicEvaluator{}, unusedLedgerService{}, &submissionFailurePayoutService{}, unusedPayinService{})
	handler.now = func() time.Time { return now }
	handler.newID = func() (string, error) { return "evt_returned", nil }
	payload, _ := json.Marshal(commandPayload{PaymentID: "pay_1", Reason: "provider confirmed return"})
	event := eventbus.Event{ID: "evt_return", Type: "payment.return", Version: 1, AggregateID: "pay_1", AggregateType: "payment", OccurredAt: now, Payload: payload}
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE payments SET payment_status=.*payment_status='failed' AND EXISTS").WithArgs(paymentcore.PaymentStatusFailed, now, "pay_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WithArgs("pay_1", "returned", "provider confirmed return", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_1", paymentcore.PaymentStatusFailed, "provider confirmed return", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_returned", eventbus.PayoutEventsTopic, "payout.funds_returned", eventbus.PayoutFundsReturnedVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	if err := handler.Handle(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
