package paymentcore

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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
		WithArgs("pay_test", "order-1", "USD", int64(2500), "customer-1", StateCreated, int64(0), "idem-1", service.now()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").
		WithArgs("pay_test", "created", "payment intent created", service.now()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").
		WithArgs("pay_test", StateCreated, "payment created", service.now()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_test", PaymentEventsTopic, "payment.created", 1, "pay_test", "payment", sqlmock.AnyArg(), service.now()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	payment, err := service.CreatePayment(context.Background(), "order-1", "USD", 2500, "customer-1", "idem-1")
	if err != nil {
		t.Fatalf("CreatePayment returned error: %v", err)
	}
	if payment.ID != "pay_test" || payment.State != StateCreated {
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

	_, err = service.CreatePayment(context.Background(), "order-1", "USD", 2500, "customer-1", "idem-1")
	if err == nil {
		t.Fatal("expected outbox insert error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet database expectations: %v", err)
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT state, amount_minor FROM payments WHERE id = $1 FOR UPDATE`)).
		WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"state", "amount_minor"}).AddRow(StateCreated, int64(2500)))
	mock.ExpectExec("UPDATE payments").
		WithArgs(StateProcessing, int64(2500), service.now(), "pay_test").
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

func deterministicPostgresService(db *sql.DB) *PostgresService {
	fixedTime := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	service := NewPostgresService(db)
	service.now = func() time.Time { return fixedTime }
	service.newID = func(prefix string) (string, error) {
		return prefix + "test", nil
	}
	return service
}
