package payin

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"stablerail/eventbus"
	"stablerail/paymentcore"
)

type unusedProvider struct{}

func (unusedProvider) Name() string { return "test" }
func (unusedProvider) CreatePayinQuote(context.Context, QuoteRequest) (QuoteResult, error) {
	return QuoteResult{}, nil
}
func (unusedProvider) ExecutePayin(context.Context, ExecuteRequest) (ExecuteResult, error) {
	return ExecuteResult{}, nil
}

func TestRecordLedgerCompletesReceivedPayin(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewService(db, unusedProvider{})
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT payment_id,status,destination_amount_minor").WithArgs("pin_1").WillReturnRows(sqlmock.NewRows([]string{"payment_id", "status", "amount", "currency"}).AddRow("pay_1", "received", int64(9900), "USDC"))
	mock.ExpectExec("INSERT INTO ledger_transactions").WithArgs("jrn_pin_1_succeeded", "pay_1", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").WithArgs("jrn_pin_1_succeeded:debit", "jrn_pin_1_succeeded", "cash:operating", "debit", int64(9900), "USDC").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").WithArgs("jrn_pin_1_succeeded:credit", "jrn_pin_1_succeeded", "settlement:payable", "credit", int64(9900), "USDC").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payins SET status='succeeded'").WithArgs(now, "pin_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payments SET payment_status='succeeded'").WithArgs(now, "pay_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_1", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_pin_1_succeeded", eventbus.PayinEventsTopic, "payin.succeeded", 1, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	if err := service.RecordLedger(context.Background(), tx, "pin_1", "corr_1", now); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFailureBeforeReceiptPreservesPendingFunds(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewService(db, unusedProvider{})
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT p.payment_id,pm.funds_status").WithArgs("pin_1").WillReturnRows(sqlmock.NewRows([]string{"payment_id", "funds_status"}).AddRow("pay_1", "pending"))
	mock.ExpectExec("UPDATE payins SET provider_payin_id").WithArgs("", StatusFailed, sqlmock.AnyArg(), sqlmock.AnyArg(), "policy rejected", now, "pin_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payments SET payment_status").WithArgs(paymentcore.PaymentStatusFailed, paymentcore.FundsStatusPending, now, "pay_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_1", paymentcore.PaymentStatusFailed, "payin.failed", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_pin_1_failed", eventbus.PayinEventsTopic, "payin.failed", eventbus.PayinFailedVersion, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, _ := db.BeginTx(context.Background(), nil)
	if err := service.ApplyResult(context.Background(), tx, "pin_1", "corr_1", ExecuteResult{Status: StatusFailed, FailureReason: "policy rejected"}, now); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
