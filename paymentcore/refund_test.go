package paymentcore

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCreateRefundEnqueuesProviderCommandWithoutPostingLedger(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service := deterministicPostgresService(db)
	now := service.now()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,currency,amount_minor,payment_status,funds_status FROM payments").WithArgs("pay_1").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "currency", "amount_minor", "payment_status", "funds_status"}).AddRow("tenant_1", "USD", int64(2500), PaymentStatusSucceeded, FundsStatusConsumed))
	mock.ExpectQuery("SELECT id,payment_id,amount_minor,currency,status,reason,created_at,updated_at FROM payment_refunds").WithArgs("tenant_1", "refund-key").
		WillReturnRows(sqlmock.NewRows([]string{"id", "payment_id", "amount_minor", "currency", "status", "reason", "created_at", "updated_at"}))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_minor\\),0\\) FROM payment_refunds").WithArgs("pay_1").
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(int64(500)))
	mock.ExpectExec("INSERT INTO payment_refunds").WithArgs("ref_test", "pay_1", "tenant_1", "refund-key", int64(1000), "USD", "duplicate order", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_test", 1, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	refund, err := service.CreateRefund(context.Background(), "pay_1", "tenant_1", "refund-key", 1000, "duplicate order")
	if err != nil {
		t.Fatal(err)
	}
	if refund.ID != "ref_test" || refund.Status != "created" || refund.AmountMinor != 1000 {
		t.Fatalf("unexpected refund: %+v", refund)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateRefundRejectsAmountAboveRemainingBalance(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	service := deterministicPostgresService(db)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,currency,amount_minor,payment_status,funds_status FROM payments").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "currency", "amount_minor", "payment_status", "funds_status"}).AddRow("tenant_1", "USD", int64(2500), PaymentStatusSucceeded, FundsStatusConsumed))
	mock.ExpectQuery("SELECT id,payment_id,amount_minor,currency,status,reason,created_at,updated_at FROM payment_refunds").WillReturnRows(sqlmock.NewRows([]string{"id", "payment_id", "amount_minor", "currency", "status", "reason", "created_at", "updated_at"}))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_minor\\),0\\) FROM payment_refunds").WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(int64(2000)))
	mock.ExpectRollback()
	if _, err := service.CreateRefund(context.Background(), "pay_1", "tenant_1", "refund-key", 501, "too much"); err != ErrRefundAmountExceeded {
		t.Fatalf("error = %v", err)
	}
}
