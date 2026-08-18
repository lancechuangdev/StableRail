package payin

import (
	"context"
	"stablerail/settlement"
	"testing"
)

func TestMockProviderCreatesQuoteAndCompletesPayin(t *testing.T) {
	p := settlement.NewMockProvider(settlement.PayoutResult{})
	q, err := p.CreatePayinQuote(context.Background(), QuoteRequest{IdempotencyKey: "quote-1", TenantID: "tenant-1", FundingMethod: "ach", CurrencyType: "sender", DestinationAccountID: "acct_1", DestinationCurrency: "USDB", SourceCurrency: "USD", AmountMinor: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if q.ProviderQuoteID == "" || q.SenderAmountMinor != 1000 || q.ExpiresAt.IsZero() {
		t.Fatalf("quote=%+v", q)
	}
	r, err := p.ExecutePayin(context.Background(), ExecuteRequest{IdempotencyKey: "payin-1", QuoteID: q.ProviderQuoteID})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != StatusSucceeded || r.ProviderPayinID == "" || len(r.Instructions) == 0 {
		t.Fatalf("payin=%+v", r)
	}
}

func TestQuoteRequiresDestinationAccount(t *testing.T) {
	r := QuoteRequest{IdempotencyKey: "q", TenantID: "t", FundingMethod: "ach", CurrencyType: "sender", DestinationCurrency: "USDC", SourceCurrency: "USD", AmountMinor: 1}
	if err := r.Validate(); err == nil {
		t.Fatal("expected destination validation error")
	}
}
