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
	"stablerail/paymentcore/payout"
	"stablerail/policy"
)

type submissionFailurePayoutService struct {
	appliedPaymentID, appliedCommandID, appliedCorrelationID string
	appliedResult                                            payout.Result
}

func (submissionFailurePayoutService) ExecutePayout(context.Context, string, string) (payout.Result, error) {
	return payout.Result{}, &payout.ProviderError{Message: "insufficient balance", Code: "submission_failed", Retryable: false}
}
func (s *submissionFailurePayoutService) ApplyResult(_ context.Context, _ *sql.Tx, paymentID, commandID, correlationID string, result payout.Result, _ time.Time) error {
	s.appliedPaymentID, s.appliedCommandID, s.appliedCorrelationID, s.appliedResult = paymentID, commandID, correlationID, result
	return nil
}

type unusedLedgerService struct{}
type unusedPayinService struct{}

func (unusedPayinService) ExecutePayin(context.Context, string) (payin.ExecuteResult, error) {
	return payin.ExecuteResult{}, errors.New("unused")
}
func (unusedPayinService) ApplyResult(context.Context, *sql.Tx, string, string, payin.ExecuteResult, time.Time) error {
	return errors.New("unused")
}

type recordingPayinService struct {
	executedID, appliedID, correlation string
}

func (s *recordingPayinService) ExecutePayin(_ context.Context, id string) (payin.ExecuteResult, error) {
	s.executedID = id
	return payin.ExecuteResult{ProviderPayinID: "pi_1", Status: payin.StatusProcessing}, nil
}
func (s *recordingPayinService) ApplyResult(_ context.Context, _ *sql.Tx, id, correlation string, _ payin.ExecuteResult, _ time.Time) error {
	s.appliedID, s.correlation = id, correlation
	return nil
}

func (unusedLedgerService) Reserve(context.Context, *sql.Tx, ledger.ReservationRequest) error {
	return nil
}
func (unusedLedgerService) Release(context.Context, *sql.Tx, ledger.ReleaseRequest) error { return nil }
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
	payload, _ := json.Marshal(commandPayload{CorrelationID: "corr_1", PaymentID: "pay_1"})
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
	if payouts.appliedPaymentID != "pay_1" || payouts.appliedCommandID != "evt_command" || payouts.appliedCorrelationID != "corr_1" || payouts.appliedResult.Status != payout.StatusFailed || payouts.appliedResult.FailureCode != "submission_failed" {
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
	payload, _ := json.Marshal(commandPayload{CorrelationID: "corr_1", PayinID: "pin_1"})
	event := eventbus.Event{ID: "evt_command", Type: "payin.execute", Version: 1, AggregateID: "pin_1", AggregateType: "payin", OccurredAt: time.Now().UTC(), Payload: payload}
	mock.ExpectBegin()
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	if err := handler.Handle(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if service.executedID != "pin_1" || service.appliedID != "pin_1" || service.correlation != "corr_1" {
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
	payload, _ := json.Marshal(commandPayload{CorrelationID: "corr_1", PayinID: "pin_1"})
	event := eventbus.Event{ID: "evt_policy", Type: "payin.policy.evaluate", Version: 1, AggregateID: "pin_1", AggregateType: "payin", OccurredAt: now, Payload: payload}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT source_amount_minor,source_currency FROM payins").WithArgs("pin_1").WillReturnRows(sqlmock.NewRows([]string{"amount", "currency"}).AddRow(int64(1000), "USD"))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_policy_result", eventbus.PayinEventsTopic, "payin.policy.approved", eventbus.PayinPolicyApprovedVersion, "pin_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
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

func TestLateReturnMovesFailedReservedPaymentToReturned(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	handler := NewCommandHandler(policy.DeterministicEvaluator{}, unusedLedgerService{}, &submissionFailurePayoutService{}, unusedPayinService{})
	handler.now = func() time.Time { return now }
	handler.newID = func() (string, error) { return "evt_returned", nil }
	payload, _ := json.Marshal(commandPayload{CorrelationID: "corr_1", PaymentID: "pay_1", Reason: "provider confirmed return"})
	event := eventbus.Event{ID: "evt_return", Type: "payment.return", Version: 1, AggregateID: "pay_1", AggregateType: "payment", OccurredAt: now, Payload: payload}
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE payments SET payment_status=.*payment_status='failed' AND funds_status='reserved'").WithArgs(paymentcore.PaymentStatusFailed, paymentcore.FundsStatusReturned, now, "pay_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WithArgs("pay_1", "returned", "provider confirmed return", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_1", paymentcore.PaymentStatusFailed, "provider confirmed return", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_returned", eventbus.PayoutEventsTopic, "payout.funds_returned", eventbus.PayoutFundsReturnedVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_returned", eventbus.PaymentEventsTopic, "payment.funds_status_changed", eventbus.PaymentFundsStatusChangedVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
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
