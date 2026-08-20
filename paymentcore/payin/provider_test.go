package payin_test

import (
	"context"
	"testing"

	"stablerail/paymentcore"
	"stablerail/paymentcore/payin"
	"stablerail/paymentcore/payout"
	"stablerail/settlement"
)

func TestMockProviderCreatesQuoteAndCompletesPayin(t *testing.T) {
	p := settlement.NewMockProvider(payout.ExecutionResult{})
	q, err := p.CreatePayinQuote(context.Background(), payin.QuoteRequest{QuoteRequest: paymentcore.QuoteRequest{IdempotencyKey: "quote-1", TenantID: "tenant-1", CurrencyType: "sender", DestinationCurrency: "USDB", SourceCurrency: "USD", AmountMinor: 1000}, FundingMethod: "ach", DestinationAccountID: "acct_1"})
	if err != nil {
		t.Fatal(err)
	}
	if q.ProviderQuoteID == "" || q.SenderAmountMinor != 1000 || q.ExpiresAt.IsZero() {
		t.Fatalf("quote=%+v", q)
	}
	r, err := p.ExecutePayin(context.Background(), payin.ExecuteRequest{IdempotencyKey: "payin-1", ProviderQuoteID: q.ProviderQuoteID})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != paymentcore.ExecutionSucceeded || r.ProviderReference == "" || len(r.Instructions) == 0 {
		t.Fatalf("payin=%+v", r)
	}
}

func TestQuoteRequiresDestinationAccount(t *testing.T) {
	r := payin.QuoteRequest{QuoteRequest: paymentcore.QuoteRequest{IdempotencyKey: "q", TenantID: "t", CurrencyType: "sender", DestinationCurrency: "USDC", SourceCurrency: "USD", AmountMinor: 1}, FundingMethod: "ach"}
	if err := r.Validate(); err == nil {
		t.Fatal("expected destination validation error")
	}
}
