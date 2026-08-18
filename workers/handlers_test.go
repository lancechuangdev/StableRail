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
	"stablerail/policy"
	"stablerail/settlement"
)

type submissionFailureProvider struct{}

func (submissionFailureProvider) Name() string { return "blindpay" }
func (submissionFailureProvider) CreatePayoutQuote(context.Context, settlement.PayoutQuoteRequest) (settlement.PayoutQuoteResult, error) {
	return settlement.PayoutQuoteResult{}, errors.New("not implemented")
}
func (submissionFailureProvider) ExecutePayout(context.Context, settlement.PayoutRequest) (settlement.PayoutResult, error) {
	return settlement.PayoutResult{}, &settlement.ProviderError{Message: "insufficient balance", Code: "submission_failed", Retryable: false}
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
