package blindpay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type fakeManagedWalletPayoutClient struct {
	request PayoutRequest
	payout  Payout
	err     error
}

func (f *fakeManagedWalletPayoutClient) CreateEVMPayout(_ context.Context, request PayoutRequest) (Payout, error) {
	f.request = request
	return f.payout, f.err
}

func TestPayoutServiceCommitsAttemptBeforeManagedWalletSubmission(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 12, 14, 0, 0, 0, time.UTC)
	client := &fakeManagedWalletPayoutClient{payout: Payout{ID: "po_test", Status: "processing", RawPayload: []byte(`{"id":"po_test","status":"processing"}`)}}
	service, err := NewPayoutService(db, client)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return now }

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_wallet_id,w.address FROM blindpay_quotes").
		WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_wallet_id", "address"}).AddRow("qu_test", "bl_test", "0xabc"))
	mock.ExpectExec("INSERT INTO blindpay_payouts").
		WithArgs("pay_test", "qu_test", "idem_test", "bl_test", "0xabc", now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE blindpay_payouts SET provider_payout_id").
		WithArgs("po_test", "processing", []byte(`{"id":"po_test","status":"processing"}`), now, "pay_test").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payments SET funds_status").WithArgs("reserved", now, "pay_test").WillReturnResult(sqlmock.NewResult(0, 1))

	payout, err := service.SubmitPayment(context.Background(), "pay_test", "idem_test")
	if err != nil {
		t.Fatal(err)
	}
	if payout.ProviderPayoutID != "po_test" || client.request.QuoteID != "qu_test" || client.request.SenderWalletAddress != "0xabc" || client.request.IdempotencyKey != "idem_test" {
		t.Fatalf("unexpected payout=%+v request=%+v", payout, client.request)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPayoutServiceKeepsFundsReservedAfterAmbiguousSubmission(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, time.August, 12, 14, 0, 0, 0, time.UTC)
	service, _ := NewPayoutService(db, &fakeManagedWalletPayoutClient{err: errors.New("connection reset")})
	service.now = func() time.Time { return now }
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_wallet_id,w.address FROM blindpay_quotes").WillReturnRows(sqlmock.NewRows([]string{"id", "provider_wallet_id", "address"}).AddRow("qu_test", "bl_test", "0xabc"))
	mock.ExpectExec("INSERT INTO blindpay_payouts").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE blindpay_payouts SET provider_status").WithArgs("unknown", "connection reset", now, "pay_test").WillReturnResult(sqlmock.NewResult(0, 1))

	if _, err := service.SubmitPayment(context.Background(), "pay_test", "idem_test"); !errors.Is(err, ErrPayoutSubmissionUnknown) {
		t.Fatalf("error=%v, want ErrPayoutSubmissionUnknown", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
