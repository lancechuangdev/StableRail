package blindpay

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/paymentcore"
	"stablerail/paymentcore/payout"
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

func TestProviderMapsProcessingPayoutToPendingSettlement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	client := &fakeManagedWalletPayoutClient{payout: Payout{ID: "po_test", Status: "processing"}}
	provider := &Provider{payoutClient: client, repo: &Repository{db: db}}
	mock.ExpectQuery("SELECT provider_reference,metadata FROM provider_resources").WithArgs("acct_test", "tenant_test", "account").WillReturnRows(sqlmock.NewRows([]string{"provider_reference", "metadata"}).AddRow("bl_test", []byte(`{"address":"0xabc"}`)))

	result, err := provider.ExecutePayout(context.Background(), payout.ExecuteRequest{IdempotencyKey: "idem-1", PaymentID: "pay_test", TenantID: "tenant_test", SourceAccountID: "acct_test", ProviderQuoteID: "qu_test", AmountMinor: 1, Currency: "USDB"})
	if err != nil {
		t.Fatal(err)
	}
	if result.ProviderReference != "po_test" || result.Status != paymentcore.ExecutionPending {
		t.Fatalf("result=%+v", result)
	}
}

func TestProviderClassifiesPermanentAPIErrorAsSubmissionFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	client := &fakeManagedWalletPayoutClient{err: &APIError{StatusCode: 422, Code: "insufficient_balance", Message: "insufficient balance", Kind: ErrorUserAction}}
	provider := &Provider{payoutClient: client, repo: &Repository{db: db}}
	mock.ExpectQuery("SELECT provider_reference,metadata FROM provider_resources").WillReturnRows(sqlmock.NewRows([]string{"provider_reference", "metadata"}).AddRow("bl_test", []byte(`{"address":"0xabc"}`)))

	_, err = provider.ExecutePayout(context.Background(), payout.ExecuteRequest{IdempotencyKey: "idem-1", PaymentID: "pay_test", TenantID: "tenant_test", SourceAccountID: "acct_test", ProviderQuoteID: "qu_test", AmountMinor: 1, Currency: "USDB"})
	var providerErr *payout.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Retryable || providerErr.Code != "submission_failed" {
		t.Fatalf("error=%v, want non-retryable submission_failed", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
