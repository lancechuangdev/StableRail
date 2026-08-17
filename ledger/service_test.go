package ledger

import (
	"context"
	"database/sql/driver"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestReleaseWritesReversingJournal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT payment_status, amount_minor, currency FROM payments WHERE id=$1 FOR UPDATE`)).
		WithArgs("pay_1").WillReturnRows(sqlmock.NewRows([]string{"state", "amount_minor", "currency"}).AddRow("processing", 2500, "USD"))
	mock.ExpectExec("INSERT INTO ledger_transactions").WithArgs("jrn_pay_1_released", "pay_1", "payment.released", now).WillReturnResult(sqlmock.NewResult(0, 1))
	lines := []struct{ id, account, side string }{
		{"jrn_pay_1_released:debit", "settlement:payable", "debit"},
		{"jrn_pay_1_released:credit", "cash:operating", "credit"},
	}
	for _, line := range lines {
		mock.ExpectExec("INSERT INTO ledger_entries").WithArgs(line.id, "jrn_pay_1_released", line.account, line.side, int64(2500), "USD").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec("INSERT INTO payment_audit_events").WithArgs("pay_1", "released", "ledger reservation released", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_1", driver.Value("processing"), "ledger reservation released", now).WillReturnResult(sqlmock.NewResult(0, 1))

	if err := NewPostgresService().Release(context.Background(), tx, ReleaseRequest{PaymentID: "pay_1", At: now}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRecordReturnCreditsObligationWithoutChangingSucceededPayment(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, _ := db.Begin()
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT payment_status,funds_status,amount_minor,currency").WithArgs("pay_1").WillReturnRows(sqlmock.NewRows([]string{"payment_status", "funds_status", "amount_minor", "currency"}).AddRow("succeeded", "consumed", 2500, "USD"))
	mock.ExpectExec("INSERT INTO ledger_transactions").WithArgs("jrn_ret_1", "pay_1", now).WillReturnResult(sqlmock.NewResult(0, 1))
	for _, line := range []struct{ id, account, side string }{
		{"jrn_ret_1:debit", "cash:operating", "debit"},
		{"jrn_ret_1:credit", "settlement:payable", "credit"},
	} {
		mock.ExpectExec("INSERT INTO ledger_entries").WithArgs(line.id, "jrn_ret_1", line.account, line.side, int64(2500), "USD").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec("INSERT INTO payment_returns").WithArgs("ret_1", "pay_1", "blindpay", "msg_1", int64(2500), "USD", "recipient account frozen", "jrn_ret_1", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WithArgs("pay_1", "recipient account frozen", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_1", "recipient account frozen", now).WillReturnResult(sqlmock.NewResult(0, 1))

	err = NewPostgresService().RecordReturn(context.Background(), tx, ReturnRequest{ID: "ret_1", PaymentID: "pay_1", Provider: "blindpay", ProviderEventID: "msg_1", Reason: "recipient account frozen", At: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
