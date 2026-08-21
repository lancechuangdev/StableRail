package payout

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
	"stablerail/paymentcore"
)

type serviceProvider struct {
	request      paymentcore.ExecuteRequest
	result       paymentcore.ExecutionResult
	err          error
	quoteRequest QuoteRequest
	quoteResult  paymentcore.ProviderQuote
	quoteCalls   int
}

func (*serviceProvider) Name() string { return "blindpay" }
func (p *serviceProvider) CreatePayoutQuote(_ context.Context, r QuoteRequest) (paymentcore.ProviderQuote, error) {
	p.quoteRequest, p.quoteCalls = r, p.quoteCalls+1
	if p.quoteResult.ProviderQuoteID != "" {
		return p.quoteResult, nil
	}
	return paymentcore.ProviderQuote{ProviderQuoteID: "qu_provider", SourceCurrency: r.SourceCurrency, DestinationCurrency: r.DestinationCurrency, SenderAmountMinor: r.AmountMinor, ReceiverAmountMinor: r.AmountMinor, CommercialRate: "1", ProviderRate: "1", ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (p *serviceProvider) ExecutePayout(_ context.Context, r paymentcore.ExecuteRequest) (paymentcore.ExecutionResult, error) {
	p.request = r
	return p.result, p.err
}

func TestServiceCommitsAttemptBeforeProviderExecution(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	provider := &serviceProvider{result: paymentcore.ExecutionResult{ProviderReference: "po_test", Status: paymentcore.ExecutionPending, Payload: []byte(`{"id":"po_test"}`)}}
	service, _ := NewService(db, provider, provider)
	service.now = func() time.Time { return now }
	mock.ExpectQuery("SELECT idempotency_key FROM payments").WithArgs("pay_test").WillReturnRows(sqlmock.NewRows([]string{"idempotency_key"}).AddRow("idem_test"))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_quote_id").WithArgs("pay_test", "blindpay").WillReturnRows(quoteRows())
	mock.ExpectExec("INSERT INTO payouts").WithArgs("pay_test", "quote_test", "tenant_test", "account_test", "instrument_test", "ach", int64(100), "USDB", int64(90), "USD", "blindpay", "idem_test", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE payouts SET provider_payout_id").WithArgs("po_test", "processing", []byte(`{"id":"po_test"}`), now, "pay_test").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = service.ExecutePayout(context.Background(), "pay_test")
	if err != nil {
		t.Fatal(err)
	}
	if provider.request.ProviderQuoteID != "qu_provider" || provider.request.IdempotencyKey != "idem_test" {
		t.Fatalf("provider request=%+v", provider.request)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceAppliesPayoutResultInInboxTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	provider := &serviceProvider{}
	service, _ := NewService(db, provider, provider)
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO settlement_submissions").WithArgs("pay_test", "evt_test", "blindpay", "po_test", paymentcore.ExecutionPending, "", "", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ApplyResult(context.Background(), tx, "pay_test", "evt_test", paymentcore.ExecutionResult{ProviderReference: "po_test", Status: paymentcore.ExecutionPending}, now); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceAppliesFailedPayoutAndPublishesSagaEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	provider := &serviceProvider{}
	service, _ := NewService(db, provider, provider)
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO settlement_submissions").WithArgs("pay_test", "evt_test", "blindpay", "", paymentcore.ExecutionFailed, "submission_failed", "insufficient balance", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_test:result", eventbus.PayoutEventsTopic, "payout.provider_failed", eventbus.PayoutProviderFailedVersion, "pay_test", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	result := paymentcore.ExecutionResult{Status: paymentcore.ExecutionFailed, FailureCode: "submission_failed", FailureMessage: "insufficient balance"}
	if err := service.ApplyResult(context.Background(), tx, "pay_test", "evt_test", result, now); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceRejectsPayoutWithoutProviderResourceQuote(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	provider := &serviceProvider{result: paymentcore.ExecutionResult{ProviderReference: "po_test", Status: paymentcore.ExecutionPending}}
	service, _ := NewService(db, provider, provider)
	service.now = func() time.Time { return now }
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_quote_id").WithArgs("pay_test", "blindpay").WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	if _, err := service.executePayout(context.Background(), executionRequest{paymentID: "pay_test", idempotencyKey: "idem_test"}); err == nil {
		t.Fatal("expected missing route error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceRecordsAmbiguousProviderOutcome(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	provider := &serviceProvider{err: &paymentcore.ProviderError{Message: "connection reset", Retryable: true}}
	service, _ := NewService(db, provider, provider)
	service.now = func() time.Time { return now }
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_quote_id").WillReturnRows(quoteRows())
	mock.ExpectExec("INSERT INTO payouts").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE payouts SET settlement_status").WithArgs("unknown", "connection reset", now, "pay_test").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = service.executePayout(context.Background(), executionRequest{paymentID: "pay_test", idempotencyKey: "idem_test"})
	if !errors.Is(err, ErrSubmissionUnknown) {
		t.Fatalf("error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func quoteRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "provider_quote_id", "tenant_id", "source_account_id", "destination_instrument_id", "method", "sender_amount_minor", "source_currency", "receiver_amount_minor", "destination_currency"}).AddRow("quote_test", "qu_provider", "tenant_test", "account_test", "instrument_test", "ach", int64(100), "USDB", int64(90), "USD")
}

func TestServiceCreatesAndPersistsPayoutQuote(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	expires := now.Add(5 * time.Minute)
	provider := &serviceProvider{quoteResult: paymentcore.ProviderQuote{ProviderQuoteID: "qu_provider", SourceCurrency: "USDB", DestinationCurrency: "USD", SenderAmountMinor: 100, ReceiverAmountMinor: 95, CommercialRate: "0.95", ProviderRate: "0.96", FlatFeeMinor: 1, ExpiresAt: expires, Payload: []byte(`{"id":"qu_provider"}`)}}
	service, _ := NewService(db, provider, provider)
	service.now = func() time.Time { return now }
	service.newID = func(string) (string, error) { return "pqi_test", nil }
	request := QuoteRequest{QuoteRequest: paymentcore.QuoteRequest{IdempotencyKey: "idem_quote", TenantID: "tenant_test", FundingMethod: "bank", SourceCurrency: "USDB", DestinationCurrency: "USD", CurrencyType: "sender", AmountMinor: 100}, SourceAccountID: "account_test", DestinationInstrumentID: "instrument_test"}
	mock.ExpectQuery("SELECT id,provider,provider_quote_id").WithArgs("tenant_test", "idem_quote").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO payment_quotes").WithArgs("pqi_test", "blindpay", "qu_provider", "tenant_test", "idem_quote", "account_test", "instrument_test", "bank", "USDB", "USD", "sender", false, int64(100), int64(100), int64(95), "0.95", "0.96", int64(1), int64(0), nil, expires, []byte(`{"id":"qu_provider"}`), []byte(`{}`), now).WillReturnResult(sqlmock.NewResult(0, 1))

	quote, err := service.CreateQuote(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if quote.ID != "pqi_test" || quote.ProviderQuoteID != "qu_provider" || provider.quoteCalls != 1 {
		t.Fatalf("quote=%+v calls=%d", quote, provider.quoteCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
