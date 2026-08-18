package payin_test

import (
	"context"
	"stablerail/paymentcore/payin"
	"stablerail/paymentcore/payout"
	"stablerail/settlement"
	"testing"
)

func TestMockProviderCreatesQuoteAndCompletesPayin(t *testing.T) {
	p := settlement.NewMockProvider(payout.Result{})
	q, err := p.CreatePayinQuote(context.Background(), payin.QuoteRequest{IdempotencyKey: "quote-1", TenantID: "tenant-1", FundingMethod: "ach", CurrencyType: "sender", DestinationAccountID: "acct_1", DestinationCurrency: "USDB", SourceCurrency: "USD", AmountMinor: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if q.ProviderQuoteID == "" || q.SenderAmountMinor != 1000 || q.ExpiresAt.IsZero() {
		t.Fatalf("quote=%+v", q)
	}
	r, err := p.ExecutePayin(context.Background(), payin.ExecuteRequest{IdempotencyKey: "payin-1", QuoteID: q.ProviderQuoteID})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != payin.StatusSucceeded || r.ProviderPayinID == "" || len(r.Instructions) == 0 {
		t.Fatalf("payin=%+v", r)
	}
}

func TestQuoteRequiresDestinationAccount(t *testing.T) {
	r := payin.QuoteRequest{IdempotencyKey: "q", TenantID: "t", FundingMethod: "ach", CurrencyType: "sender", DestinationCurrency: "USDC", SourceCurrency: "USD", AmountMinor: 1}
	if err := r.Validate(); err == nil {
		t.Fatal("expected destination validation error")
	}
}
