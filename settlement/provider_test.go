package settlement

import (
	"context"
	"testing"
)

func TestMockProviderIsIdempotent(t *testing.T) {
	p := NewMockProvider(SettlementResult{})
	r := SettlementRequest{IdempotencyKey: "command-1", PaymentID: "payment-1", AmountMinor: 1250, Currency: "USD"}
	first, err := p.Submit(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	p.Result = SettlementResult{Status: StatusFailed, FailureCode: "declined"}
	second, err := p.Submit(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.Status != StatusSucceeded {
		t.Fatalf("results = %#v, %#v", first, second)
	}
}

func TestFailedResultRequiresCode(t *testing.T) {
	p := NewMockProvider(SettlementResult{Status: StatusFailed})
	_, err := p.Submit(context.Background(), SettlementRequest{IdempotencyKey: "x", PaymentID: "p", AmountMinor: 1, Currency: "USD"})
	if err == nil {
		t.Fatal("expected invalid result")
	}
}
