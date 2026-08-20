package payout

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type serviceProvider struct {
	request      Request
	result       Result
	err          error
	quoteRequest QuoteRequest
	quoteResult  QuoteResult
	quoteCalls   int
}

func (*serviceProvider) Name() string { return "blindpay" }
func (p *serviceProvider) CreatePayoutQuote(_ context.Context, r QuoteRequest) (QuoteResult, error) {
	p.quoteRequest, p.quoteCalls = r, p.quoteCalls+1
	if p.quoteResult.ProviderQuoteID != "" {
		return p.quoteResult, nil
	}
	return QuoteResult{ProviderQuoteID: "qu_provider", SourceCurrency: r.SourceCurrency, DestinationCurrency: r.DestinationCurrency, SenderAmountMinor: r.RequestAmountMinor, ReceiverAmountMinor: r.RequestAmountMinor, CommercialRate: "1", ProviderRate: "1", ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (p *serviceProvider) ExecutePayout(_ context.Context, r Request) (Result, error) {
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
	provider := &serviceProvider{result: Result{ProviderReference: "po_test", Status: StatusPending, Payload: []byte(`{"id":"po_test"}`)}}
	service, _ := NewService(db, provider)
	service.now = func() time.Time { return now }
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_quote_id").WithArgs("pay_test", "blindpay").WillReturnRows(quoteRows())
	mock.ExpectExec("INSERT INTO payouts").WithArgs("pay_test", "quote_test", "tenant_test", "account_test", "instrument_test", "ach", int64(100), "USDB", int64(90), "USD", "blindpay", "idem_test", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE payouts SET provider_payout_id").WithArgs("po_test", "processing", []byte(`{"id":"po_test"}`), now, "pay_test").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payments SET funds_status").WithArgs("reserved", now, "pay_test").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = service.ExecutePayout(context.Background(), Request{PaymentID: "pay_test", IdempotencyKey: "idem_test", AmountMinor: 100, Currency: "USDB"})
	if err != nil {
		t.Fatal(err)
	}
	if provider.request.ProviderQuoteID != "qu_provider" || provider.request.SourceAccountID != "account_test" || provider.request.TenantID != "tenant_test" {
		t.Fatalf("provider request=%+v", provider.request)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceExecutesQuoteLessPayoutThroughDurablePath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	provider := &serviceProvider{result: Result{ProviderReference: "po_test", Status: StatusPending}}
	service, _ := NewService(db, provider)
	service.now = func() time.Time { return now }
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_quote_id").WithArgs("pay_test", "blindpay").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT p.tenant_id,p.amount_minor,p.currency").WithArgs("pay_test").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "amount_minor", "currency", "method"}).AddRow("tenant_test", int64(100), "USD", "direct"))
	mock.ExpectExec("INSERT INTO payouts").WithArgs("pay_test", "", "tenant_test", "", "", "direct", int64(100), "USD", int64(100), "USD", "blindpay", "idem_test", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE payouts SET provider_payout_id").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payments SET funds_status").WillReturnResult(sqlmock.NewResult(0, 1))

	if _, err := service.ExecutePayout(context.Background(), Request{PaymentID: "pay_test", IdempotencyKey: "idem_test", AmountMinor: 100, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	if provider.request.TenantID != "tenant_test" || provider.request.QuoteID != "" || provider.request.AmountMinor != 100 {
		t.Fatalf("provider request=%+v", provider.request)
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
	provider := &serviceProvider{err: &ProviderError{Message: "connection reset", Retryable: true}}
	service, _ := NewService(db, provider)
	service.now = func() time.Time { return now }
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_quote_id").WillReturnRows(quoteRows())
	mock.ExpectExec("INSERT INTO payouts").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE payouts SET provider_status").WithArgs("unknown", "connection reset", now, "pay_test").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = service.ExecutePayout(context.Background(), Request{PaymentID: "pay_test", IdempotencyKey: "idem_test", AmountMinor: 100, Currency: "USDB"})
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
	provider := &serviceProvider{quoteResult: QuoteResult{ProviderQuoteID: "qu_provider", SourceCurrency: "USDB", DestinationCurrency: "USD", SenderAmountMinor: 100, ReceiverAmountMinor: 95, CommercialRate: "0.95", ProviderRate: "0.96", FlatFeeMinor: 1, ExpiresAt: expires, Payload: []byte(`{"id":"qu_provider"}`)}}
	service, _ := NewService(db, provider)
	service.now = func() time.Time { return now }
	service.newID = func(string) (string, error) { return "pqi_test", nil }
	request := QuoteRequest{IdempotencyKey: "idem_quote", TenantID: "tenant_test", SourceAccountID: "account_test", DestinationInstrumentID: "instrument_test", SourceCurrency: "USDB", DestinationCurrency: "USD", CurrencyType: "sender", RequestAmountMinor: 100}
	mock.ExpectQuery("SELECT id,provider,provider_quote_id").WithArgs("tenant_test", "idem_quote").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO payment_quotes").WithArgs("pqi_test", "blindpay", "qu_provider", "tenant_test", "idem_quote", "account_test", "instrument_test", "USDB", "USD", "sender", false, int64(100), int64(100), int64(95), "0.95", "0.96", int64(1), int64(0), nil, expires, []byte(`{"id":"qu_provider"}`), now).WillReturnResult(sqlmock.NewResult(0, 1))

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
