package blindpay

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type fakeQuoteClient struct {
	quote   Quote
	balance map[string]ManagedWalletAsset
}

func (f fakeQuoteClient) CreateQuote(context.Context, QuoteRequest) (Quote, error) {
	return f.quote, nil
}

func (f fakeQuoteClient) GetManagedWalletBalance(context.Context, string, string) (map[string]ManagedWalletAsset, error) {
	return f.balance, nil
}

func TestQuoteServiceRejectsInsufficientManagedWalletBalance(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	service, err := NewQuoteService(fakeQuoteClient{
		quote:   Quote{ID: "qu_test", ExpiresAt: now.Add(time.Minute).UnixMilli(), SenderAmount: 2500, ReceiverAmount: 2400},
		balance: map[string]ManagedWalletAsset{"USDB": {Symbol: "USDB", Amount: 2499}},
	}, repo, "sepolia", "USDB")
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return now }
	mock.ExpectQuery("SELECT c.tenant_id").
		WithArgs("tenant-1", "ba_test", "bl_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "provider_customer_id", "kyc_status", "provider_bank_account_id", "rail", "display_name", "account_last_four", "status", "provider_wallet_id", "network", "address", "display_name", "status"}).
			AddRow("tenant-1", "re_test", "approved", "ba_test", "pix", "Bank", "1234", "approved", "bl_test", "sepolia", "0xabc", "Wallet", "active"))

	_, err = service.Create(context.Background(), PayoutQuoteRequest{IdempotencyKey: "idem-1", TenantID: "tenant-1", BankAccountID: "ba_test", ManagedWalletID: "bl_test", DestinationCurrency: "BRL", CurrencyType: "sender", RequestAmountMinor: 2500})
	if err == nil || err.Error() != "managed wallet has insufficient USDB balance for payout" {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
