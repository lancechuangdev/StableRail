package blindpay

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/settlement"
)

func TestProviderMapsProcessingPayoutToPendingSettlement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewPayoutService(db, &fakeManagedWalletPayoutClient{payout: Payout{ID: "po_test", Status: "processing"}})
	provider, err := NewProvider(service)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_wallet_id,w.address FROM blindpay_quotes").WillReturnRows(sqlmock.NewRows([]string{"id", "provider_wallet_id", "address"}).AddRow("qu_test", "bl_test", "0xabc"))
	mock.ExpectExec("INSERT INTO blindpay_payouts").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE blindpay_payouts SET provider_payout_id").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE payments SET funds_status").WillReturnResult(sqlmock.NewResult(0, 1))

	result, err := provider.Submit(context.Background(), settlement.SettlementRequest{IdempotencyKey: "idem-1", PaymentID: "pay_test", AmountMinor: 1, Currency: "USDB"})
	if err != nil {
		t.Fatal(err)
	}
	if result.ProviderReference != "po_test" || result.Status != settlement.StatusPending {
		t.Fatalf("result=%+v", result)
	}
}

func TestProviderClassifiesPermanentAPIErrorAsSubmissionFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewPayoutService(db, &fakeManagedWalletPayoutClient{err: &APIError{StatusCode: 422, Code: "insufficient_balance", Message: "insufficient balance", Kind: ErrorUserAction}})
	provider, err := NewProvider(service)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT q.id,q.provider_wallet_id,w.address FROM blindpay_quotes").WillReturnRows(sqlmock.NewRows([]string{"id", "provider_wallet_id", "address"}).AddRow("qu_test", "bl_test", "0xabc"))
	mock.ExpectExec("INSERT INTO blindpay_payouts").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE blindpay_payouts SET provider_status").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = provider.Submit(context.Background(), settlement.SettlementRequest{IdempotencyKey: "idem-1", PaymentID: "pay_test", AmountMinor: 1, Currency: "USDB"})
	var providerErr *settlement.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Retryable || providerErr.Code != "submission_failed" {
		t.Fatalf("error=%v, want non-retryable submission_failed", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
