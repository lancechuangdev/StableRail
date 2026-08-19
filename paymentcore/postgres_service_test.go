package paymentcore

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
)

func TestPostgresCreateCommitsPaymentAndOutboxTogether(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	defer db.Close()

	service := deterministicPostgresService(db)
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
		WithArgs("evt_test", eventbus.PayoutEventsTopic, "payment.created", 1, "pay_test", "payment", sqlmock.AnyArg(), service.now()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	payment, err := service.CreatePayment(context.Background(), "order-1", "USD", 2500, "tenant-1", "idem-1")
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

	service := deterministicPostgresService(db)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO payments").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WillReturnError(errors.New("database unavailable"))
	mock.ExpectRollback()

	_, err = service.CreatePayment(context.Background(), "order-1", "USD", 2500, "tenant-1", "idem-1")
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

	service := deterministicPostgresService(db)
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
	mock.ExpectCommit()

	payment, err := service.CreatePaymentWithPayoutQuote(context.Background(), "order-1", "USDB", 2500, "tenant-1", "idem-1", "qu_test")
	if err != nil {
		t.Fatalf("CreatePaymentWithPayoutQuote returned error: %v", err)
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
	service := deterministicPostgresService(db)
	now := service.now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,source_currency,sender_amount_minor,status,expires_at FROM payment_quotes").WithArgs("qu_test").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "source_currency", "sender_amount_minor", "status", "expires_at"}).AddRow("tenant-1", "USDB", int64(2500), "accepted", now.Add(time.Minute)))
	mock.ExpectQuery("SELECT id, direction, external_reference, currency, amount_minor").WithArgs("idem-1").WillReturnRows(sqlmock.NewRows([]string{"id", "direction", "external_reference", "currency", "amount_minor", "tenant_id", "payment_status", "funds_status", "idempotency_key", "created_at", "updated_at"}).AddRow("pay_existing", "payout", "order-1", "USDB", int64(2500), "tenant-1", PaymentStatusCreated, FundsStatusAvailable, "idem-1", now, now))
	mock.ExpectQuery("SELECT id FROM payment_quotes").WithArgs("pay_existing").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("qu_test"))
	mock.ExpectQuery("SELECT kind,COALESCE").WithArgs("pay_existing").WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err = service.CreatePaymentWithPayoutQuote(context.Background(), "changed-order", "USDB", 2500, "tenant-1", "idem-1", "qu_test")
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

	service := deterministicPostgresService(db)
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
		WithArgs("jrn_test:debit", "jrn_test", CashOperatingAccount, EntryDebit, int64(2500), "USD").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").
		WithArgs("jrn_test:credit", "jrn_test", SettlementAccount, EntryCredit, int64(2500), "USD").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WillReturnResult(sqlmock.NewResult(1, 1))
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

	service := deterministicPostgresService(db)
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
	service := deterministicPostgresService(db)
	now := service.now()
	mock.ExpectQuery("SELECT id, direction, external_reference, currency, amount_minor").WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "direction", "external_reference", "currency", "amount_minor", "tenant_id", "payment_status", "funds_status", "idempotency_key", "created_at", "updated_at"}).
			AddRow("pay_test", "payout", "order-1", "USD", int64(2500), "tenant-1", PaymentStatusCreated, FundsStatusAvailable, "idem-1", now, now))
	mock.ExpectQuery("SELECT kind,COALESCE").WithArgs("pay_test").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT id FROM payment_quotes").WithArgs("pay_test").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT provider,COALESCE.*FROM payouts").WithArgs("pay_test").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT payment_status, occurred_at, note FROM payment_timeline_entries").WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"payment_status", "occurred_at", "note"}).AddRow(PaymentStatusCreated, now, "payment created"))

	payment, err := service.GetPayment(context.Background(), "pay_test")
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

func deterministicPostgresService(db *sql.DB) *PostgresService {
	fixedTime := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	service := NewPostgresService(db)
	service.now = func() time.Time { return fixedTime }
	service.newID = func(prefix string) (string, error) {
		return prefix + "test", nil
	}
	return service
}
