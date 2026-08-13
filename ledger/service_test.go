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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT state, amount_minor, currency FROM payments WHERE id=$1 FOR UPDATE`)).
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

func TestRecordRefundReversesSettlementJournal(t *testing.T) {
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT state, amount_minor, currency FROM payments WHERE id=$1 FOR UPDATE`)).
		WithArgs("pay_1").WillReturnRows(sqlmock.NewRows([]string{"state", "amount_minor", "currency"}).AddRow("settled", 2500, "USD"))
	mock.ExpectExec("INSERT INTO ledger_transactions").WithArgs("jrn_pay_1_refund_recorded", "pay_1", "payment.refund_recorded", now).WillReturnResult(sqlmock.NewResult(0, 1))
	lines := []struct{ id, account, side string }{
		{"jrn_pay_1_refund_recorded:debit", "cash:operating", "debit"},
		{"jrn_pay_1_refund_recorded:credit", "settlement:payable", "credit"},
	}
	for _, line := range lines {
		mock.ExpectExec("INSERT INTO ledger_entries").WithArgs(line.id, "jrn_pay_1_refund_recorded", line.account, line.side, int64(2500), "USD").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec("INSERT INTO payment_audit_events").WithArgs("pay_1", "refund_recorded", "provider refund recorded", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_1", driver.Value("settled"), "provider refund recorded", now).WillReturnResult(sqlmock.NewResult(0, 1))

	if err := NewPostgresService().RecordRefund(context.Background(), tx, ReleaseRequest{PaymentID: "pay_1", At: now}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
