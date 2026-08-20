package blindpay

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/paymentcore"
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
	mock.ExpectQuery("SELECT provider_execution_context FROM payment_quotes").WithArgs("qu_test").WillReturnRows(sqlmock.NewRows([]string{"provider_execution_context"}).AddRow([]byte(`{"address":"0xabc"}`)))

	result, err := provider.ExecutePayout(context.Background(), paymentcore.ExecuteRequest{IdempotencyKey: "idem-1", ProviderQuoteID: "qu_test"})
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
	mock.ExpectQuery("SELECT provider_execution_context FROM payment_quotes").WithArgs("qu_test").WillReturnRows(sqlmock.NewRows([]string{"provider_execution_context"}).AddRow([]byte(`{"address":"0xabc"}`)))

	_, err = provider.ExecutePayout(context.Background(), paymentcore.ExecuteRequest{IdempotencyKey: "idem-1", ProviderQuoteID: "qu_test"})
	var providerErr *paymentcore.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Retryable || providerErr.Code != "submission_failed" {
		t.Fatalf("error=%v, want non-retryable submission_failed", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
