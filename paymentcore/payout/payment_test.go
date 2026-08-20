package payout

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
	"stablerail/paymentcore"
)

type storageTestProvider struct{}

func (storageTestProvider) Name() string { return "test" }
func (storageTestProvider) CreatePayoutQuote(context.Context, QuoteRequest) (QuoteResult, error) {
	return QuoteResult{}, nil
}
func (storageTestProvider) ExecutePayout(context.Context, Request) (Result, error) {
	return Result{}, nil
}

func TestCreatePaymentCommitsPaymentAndOutboxTogether(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	service := deterministicPayoutService(db)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO payments").
		WithArgs("pay_test", PaymentDirectionPayout, "order-1", "USD", int64(2500), "tenant-1", PaymentStatusCreated, FundsStatusAvailable, "idem-1", service.now()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").
		WithArgs("pay_test", "created", "payment intent created", service.now()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").
		WithArgs("pay_test", PaymentStatusCreated, "payment created", service.now()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_test", eventbus.PayoutEventsTopic, "payout.created", 1, "pay_test", "payout", sqlmock.AnyArg(), service.now()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_test", eventbus.PaymentEventsTopic, "payment.created", 1, "pay_test", "payment", sqlmock.AnyArg(), service.now()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	payment, err := service.CreatePayment(context.Background(), CreatePaymentRequest{ExternalReference: "order-1", Currency: "USD", AmountMinor: 2500, TenantID: "tenant-1", IdempotencyKey: "idem-1"})
	if err != nil {
		t.Fatalf("CreatePayment returned error: %v", err)
	}
	if payment.ID != "pay_test" || payment.PaymentStatus != PaymentStatusCreated || payment.FundsStatus != FundsStatusAvailable {
		t.Fatalf("unexpected payment: %+v", payment)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
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
	mock.ExpectExec("INSERT INTO payments").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WillReturnError(errors.New("database unavailable"))
	mock.ExpectRollback()

	_, err = service.CreatePayment(context.Background(), CreatePaymentRequest{ExternalReference: "order-1", Currency: "USD", AmountMinor: 2500, TenantID: "tenant-1", IdempotencyKey: "idem-1"})
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
	mock.ExpectQuery("SELECT kind,COALESCE").WithArgs("pay_existing").WillReturnError(sql.ErrNoRows)
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
	mock.ExpectQuery("SELECT kind,COALESCE").WithArgs("pay_test").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT id FROM payment_quotes").WithArgs("pay_test").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT provider,COALESCE.*FROM payouts").WithArgs("pay_test").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT payment_status, occurred_at, note FROM payment_timeline_entries").WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"payment_status", "occurred_at", "note"}).AddRow(PaymentStatusCreated, now, "payment created"))

	payment, err := repository.GetPayment(context.Background(), "pay_test")
	if err != nil {
		t.Fatal(err)
	}
	if payment.ID != "pay_test" || payment.Destination != nil || len(payment.Timeline) != 1 {
		t.Fatalf("unexpected payment: %+v", payment)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func deterministicPayoutService(db *sql.DB) *Service {
	fixedTime := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	service, _ := NewService(db, storageTestProvider{})
	service.now = func() time.Time { return fixedTime }
	service.newID = func(prefix string) (string, error) {
		return prefix + "test", nil
	}
	return service
}
