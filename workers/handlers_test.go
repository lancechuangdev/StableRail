package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
	"stablerail/ledger"
	"stablerail/paymentcore"
	"stablerail/policy"
	"stablerail/settlement"
)

type submissionFailureProvider struct{}

func (submissionFailureProvider) Name() string { return "blindpay" }
func (submissionFailureProvider) ExecutePayout(context.Context, settlement.PayoutRequest) (settlement.OperationResult, error) {
	return settlement.OperationResult{}, &settlement.ProviderError{Message: "insufficient balance", Code: "submission_failed", Retryable: false}
}
func (submissionFailureProvider) ExecuteRefund(context.Context, settlement.RefundRequest) (settlement.OperationResult, error) {
	return settlement.OperationResult{}, &settlement.ProviderError{Message: "unsupported", Code: "unsupported", Retryable: false}
}

type unusedLedgerService struct{}

func (unusedLedgerService) Reserve(context.Context, *sql.Tx, ledger.ReservationRequest) error {
	return nil
}
func (unusedLedgerService) Release(context.Context, *sql.Tx, ledger.ReleaseRequest) error { return nil }
func (unusedLedgerService) RecordReturn(context.Context, *sql.Tx, ledger.ReturnRequest) error {
	return nil
}

func TestSettlementSubmissionFailureEmitsFailedResult(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 17, 12, 0, 0, 0, time.UTC)
	handler := NewCommandHandler(policy.DeterministicEvaluator{}, unusedLedgerService{}, submissionFailureProvider{})
	handler.now = func() time.Time { return now }
	handler.newID = func() (string, error) { return "evt_result", nil }
	payload, _ := json.Marshal(commandPayload{CorrelationID: "corr_1", PaymentID: "pay_1"})
	event := eventbus.Event{ID: "evt_command", Type: "settlement.execute", Version: 1, AggregateID: "pay_1", AggregateType: "payment", OccurredAt: now, Payload: payload}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT amount_minor, currency FROM payments").WithArgs("pay_1").WillReturnRows(sqlmock.NewRows([]string{"amount_minor", "currency"}).AddRow(1000, "USDC"))
	mock.ExpectQuery("SELECT kind,COALESCE").WithArgs("pay_1").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_result", paymentcore.PaymentEventsTopic, "settlement.failed", eventbus.SettlementFailedVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
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
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRefundExecutePostsLedgerOnlyAfterMockProviderSucceeds(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 17, 13, 0, 0, 0, time.UTC)
	provider := settlement.NewMockProvider(settlement.OperationResult{Status: settlement.StatusSucceeded, ProviderReference: "mock_refund_1"})
	handler := NewCommandHandler(policy.DeterministicEvaluator{}, unusedLedgerService{}, provider)
	handler.now = func() time.Time { return now }
	handler.newID = func() (string, error) { return "evt_refund_result", nil }
	payload, _ := json.Marshal(commandPayload{PaymentID: "pay_1", RefundID: "ref_1", AmountMinor: 500, Currency: "USD", Reason: "duplicate order"})
	event := eventbus.Event{ID: "evt_refund_command", Type: "refund.execute", Version: 1, AggregateID: "pay_1", AggregateType: "payment", OccurredAt: now, Payload: payload}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ledger_transactions").WithArgs("jrn_ref_1", "pay_1", "payment.refund.succeeded:ref_1", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").WithArgs("jrn_ref_1:debit", "jrn_ref_1", paymentcore.CashOperatingAccount, "debit", int64(500), "USD").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").WithArgs("jrn_ref_1:credit", "jrn_ref_1", paymentcore.SettlementAccount, "credit", int64(500), "USD").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payment_refunds SET status='succeeded'").WithArgs("mock_refund_1", "jrn_ref_1", now, "ref_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WithArgs("pay_1", "merchant refund succeeded: duplicate order", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_1", "merchant refund succeeded: duplicate order", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_refund_result", paymentcore.PaymentEventsTopic, "payment.refund.succeeded", eventbus.PaymentRefundSucceededVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
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

func TestRefundExecuteFailureDoesNotPostLedger(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	now := time.Date(2026, time.August, 17, 14, 0, 0, 0, time.UTC)
	provider := settlement.NewMockProvider(settlement.OperationResult{Status: settlement.StatusFailed, ProviderReference: "mock_refund_2", FailureCode: "declined", FailureMessage: "refund declined"})
	handler := NewCommandHandler(policy.DeterministicEvaluator{}, unusedLedgerService{}, provider)
	handler.now = func() time.Time { return now }
	handler.newID = func() (string, error) { return "evt_refund_failed", nil }
	payload, _ := json.Marshal(commandPayload{PaymentID: "pay_1", RefundID: "ref_2", AmountMinor: 500, Currency: "USD", Reason: "customer request"})
	event := eventbus.Event{ID: "evt_refund_command_2", Type: "refund.execute", Version: 1, AggregateID: "pay_1", AggregateType: "payment", OccurredAt: now, Payload: payload}
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE payment_refunds SET status='failed'").WithArgs("mock_refund_2", "refund declined", now, "ref_2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_refund_failed", paymentcore.PaymentEventsTopic, "payment.refund.failed", eventbus.PaymentRefundFailedVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
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
