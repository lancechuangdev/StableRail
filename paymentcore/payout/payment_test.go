package payout

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/paymentcore"
)

type storageTestProvider struct{}

func (storageTestProvider) Name() string { return "test" }
func (storageTestProvider) CreatePayoutQuote(_ context.Context, request QuoteRequest) (ProviderQuote, error) {
	return ProviderQuote{ProviderQuoteID: "provider_quote_test", SourceCurrency: request.SourceCurrency, DestinationCurrency: request.DestinationCurrency, SenderAmountMinor: request.AmountMinor, ReceiverAmountMinor: request.AmountMinor, ExpiresAt: time.Date(2026, time.August, 7, 12, 1, 0, 0, time.UTC)}, nil
}
func (storageTestProvider) ExecutePayout(context.Context, ExecuteRequest) (ExecutionResult, error) {
	return ExecutionResult{}, nil
}

func TestCreatePaymentRequestSupportsQuotedAndDirectModes(t *testing.T) {
	base := CreatePaymentRequest{TenantID: "tenant-1", IdempotencyKey: "idem-1", ExternalReference: "order-1", Currency: "USD", AmountMinor: 100, FundingMethod: "bank"}
	tests := []struct {
		name    string
		request CreatePaymentRequest
		wantErr bool
	}{
		{name: "quoted", request: withPayoutRoute(base, "quote-1", "", "")},
		{name: "direct", request: withPayoutRoute(base, "", "account-1", "instrument-1")},
		{name: "mixed", request: withPayoutRoute(base, "quote-1", "account-1", "instrument-1"), wantErr: true},
		{name: "missing route", request: base, wantErr: true},
		{name: "partial direct route", request: withPayoutRoute(base, "", "account-1", ""), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.request.Validate(); (err != nil) != test.wantErr {
				t.Fatalf("Validate() error=%v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func withPayoutRoute(request CreatePaymentRequest, quoteID, sourceAccountID, destinationInstrumentID string) CreatePaymentRequest {
	request.QuoteID = quoteID
	request.SourceAccountID = sourceAccountID
	request.DestinationInstrumentID = destinationInstrumentID
	return request
}

func TestCreatePaymentRequiresProviderResourceQuote(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	service := deterministicPayoutService(db)
	if _, err := service.CreatePayment(context.Background(), CreatePaymentRequest{ExternalReference: "order-1", Currency: "USD", AmountMinor: 2500, TenantID: "tenant-1", IdempotencyKey: "idem-1"}); err == nil {
		t.Fatal("expected missing quote error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestCreateDirectPaymentCreatesImplicitProviderResourceQuote(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service := deterministicPayoutService(db)
	mock.ExpectQuery("SELECT id,provider,provider_quote_id,status").WithArgs("tenant-1", "idem-1:implicit-quote").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO payment_quotes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,source_currency,sender_amount_minor,status,expires_at").WithArgs("pqi_test").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "source_currency", "sender_amount_minor", "status", "expires_at"}).AddRow("tenant-1", "USD", int64(2500), "open", service.now().Add(time.Minute)))
	mock.ExpectExec("INSERT INTO payments").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payment_quotes SET status='accepted'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	payment, err := service.CreatePayment(context.Background(), CreatePaymentRequest{ExternalReference: "order-1", Currency: "USD", AmountMinor: 2500, TenantID: "tenant-1", IdempotencyKey: "idem-1", FundingMethod: "bank", SourceAccountID: "account-1", DestinationInstrumentID: "instrument-1"})
	if err != nil {
		t.Fatal(err)
	}
	if payment.QuoteID != "pqi_test" {
		t.Fatalf("QuoteID=%q", payment.QuoteID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCreateRollsBackWhenOutboxInsertFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	service := deterministicPayoutService(db)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,source_currency,sender_amount_minor,status,expires_at FROM payment_quotes").
		WithArgs("qu_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "source_currency", "sender_amount_minor", "status", "expires_at"}).AddRow("tenant-1", "USD", int64(2500), "open", service.now().Add(time.Minute)))
	mock.ExpectExec("INSERT INTO payments").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payment_quotes SET status='accepted'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WillReturnError(errors.New("database unavailable"))
	mock.ExpectRollback()

	_, err = service.CreatePayment(context.Background(), CreatePaymentRequest{ExternalReference: "order-1", Currency: "USD", AmountMinor: 2500, TenantID: "tenant-1", IdempotencyKey: "idem-1", QuoteID: "qu_test"})
	if err == nil {
		t.Fatal("expected outbox insert error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestPostgresCreateWithPayoutQuoteBindsQuoteAndPaymentAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	service := deterministicPayoutService(db)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,source_currency,sender_amount_minor,status,expires_at FROM payment_quotes").
		WithArgs("qu_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "source_currency", "sender_amount_minor", "status", "expires_at"}).
			AddRow("tenant-1", "USDB", int64(2500), "open", service.now().Add(time.Minute)))
	mock.ExpectExec("INSERT INTO payments").
		WithArgs("pay_test", PaymentDirectionPayout, "order-1", "USDB", int64(2500), "tenant-1", PaymentStatusCreated, FundsStatusAvailable, "idem-1", service.now()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payment_quotes SET status='accepted'").
		WithArgs("pay_test", service.now(), "qu_test").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	payment, err := service.CreatePayment(context.Background(), CreatePaymentRequest{ExternalReference: "order-1", Currency: "USDB", AmountMinor: 2500, TenantID: "tenant-1", IdempotencyKey: "idem-1", QuoteID: "qu_test"})
	if err != nil {
		t.Fatalf("CreatePayment returned error: %v", err)
	}
	if payment.QuoteID != "qu_test" {
		t.Fatalf("QuoteID = %q", payment.QuoteID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestPostgresCreateWithAcceptedPayoutQuoteReportsIdempotencyConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service := deterministicPayoutService(db)
	now := service.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,source_currency,sender_amount_minor,status,expires_at FROM payment_quotes").WithArgs("qu_test").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "source_currency", "sender_amount_minor", "status", "expires_at"}).AddRow("tenant-1", "USDB", int64(2500), "accepted", now.Add(time.Minute)))
	mock.ExpectQuery("SELECT id, direction, external_reference, currency, amount_minor").WithArgs("idem-1").WillReturnRows(sqlmock.NewRows([]string{"id", "direction", "external_reference", "currency", "amount_minor", "tenant_id", "payment_status", "funds_status", "idempotency_key", "created_at", "updated_at"}).AddRow("pay_existing", "payout", "order-1", "USDB", int64(2500), "tenant-1", PaymentStatusCreated, FundsStatusAvailable, "idem-1", now, now))
	mock.ExpectQuery("SELECT id FROM payment_quotes").WithArgs("pay_existing").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("qu_test"))
	mock.ExpectRollback()

	_, err = service.CreatePayment(context.Background(), CreatePaymentRequest{ExternalReference: "changed-order", Currency: "USDB", AmountMinor: 2500, TenantID: "tenant-1", IdempotencyKey: "idem-1", QuoteID: "qu_test"})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("error=%v, want ErrIdempotencyConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTransitionCommitsStateAndOutboxTogether(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	service := deterministicPayoutService(db)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT payment_status, amount_minor, currency FROM payments WHERE id = $1 FOR UPDATE`)).
		WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"state", "amount_minor", "currency"}).
			AddRow(PaymentStatusCreated, int64(2500), "USD"))
	mock.ExpectExec("UPDATE payments").
		WithArgs(PaymentStatusProcessing, FundsStatusReserved, service.now(), "pay_test").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_transactions").
		WithArgs("jrn_test", "pay_test", "payment.processing", service.now()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").
		WithArgs("jrn_test:debit", "jrn_test", paymentcore.CashOperatingAccount, paymentcore.EntryDebit, int64(2500), "USD").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").
		WithArgs("jrn_test:credit", "jrn_test", paymentcore.SettlementAccount, paymentcore.EntryCredit, int64(2500), "USD").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := service.Process(context.Background(), "pay_test"); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestPostgresTransitionRollsBackWhenLedgerInsertFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	service := deterministicPayoutService(db)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT payment_status, amount_minor, currency FROM payments").
		WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"state", "amount_minor", "currency"}).
			AddRow(PaymentStatusCreated, int64(2500), "USD"))
	mock.ExpectExec("UPDATE payments").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_transactions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").WillReturnError(errors.New("ledger unavailable"))
	mock.ExpectRollback()

	if err := service.Process(context.Background(), "pay_test"); err == nil {
		t.Fatal("expected ledger insert error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
	}
}

func TestPostgresGetPaymentAllowsMissingDestination(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repository, err := paymentcore.NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT id, direction, external_reference, currency, amount_minor").WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "direction", "external_reference", "currency", "amount_minor", "tenant_id", "payment_status", "funds_status", "idempotency_key", "created_at", "updated_at"}).
			AddRow("pay_test", "payout", "order-1", "USD", int64(2500), "tenant-1", PaymentStatusCreated, FundsStatusAvailable, "idem-1", now, now))
	mock.ExpectQuery("SELECT id FROM payment_quotes").WithArgs("pay_test").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT provider,COALESCE.*FROM payouts").WithArgs("pay_test").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT payment_status, occurred_at, note FROM payment_timeline_entries").WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"payment_status", "occurred_at", "note"}).AddRow(PaymentStatusCreated, now, "payment created"))

	payment, err := repository.GetPayment(context.Background(), "pay_test")
	if err != nil {
		t.Fatal(err)
	}
	if payment.ID != "pay_test" || len(payment.Timeline) != 1 {
		t.Fatalf("unexpected payment: %+v", payment)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func deterministicPayoutService(db *sql.DB) *Service {
	fixedTime := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	provider := storageTestProvider{}
	service, _ := NewService(db, provider, provider)
	service.now = func() time.Time { return fixedTime }
	service.newID = func(prefix string) (string, error) {
		return prefix + "test", nil
	}
	return service
}
