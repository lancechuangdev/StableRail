package payin

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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
	mock.ExpectQuery("SELECT tenant_id,payment_id,status,destination_amount_minor").WithArgs("pin_1").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "payment_id", "status", "amount", "currency"}).AddRow("tenant_1", "pay_1", "received", int64(9900), "USDC"))
	mock.ExpectExec("INSERT INTO ledger_transactions").WithArgs("jrn_pin_1_succeeded", "pay_1", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").WithArgs("jrn_pin_1_succeeded:debit", "jrn_pin_1_succeeded", "cash:operating", "debit", int64(9900), "USDC").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").WithArgs("jrn_pin_1_succeeded:credit", "jrn_pin_1_succeeded", "settlement:payable", "credit", int64(9900), "USDC").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payins SET status='succeeded'").WithArgs(now, "pin_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payments SET payment_status='succeeded'").WithArgs(now, "pay_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_1", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO webhook_deliveries").WithArgs("evt_pin_1_succeeded", "pay_1", "payin.succeeded", sqlmock.AnyArg(), now, "tenant_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_pin_1_succeeded", EventsTopic, "payin.succeeded", 1, "pay_1", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
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
