package settlement

import (
	"context"
	"testing"
)

func TestMockProviderIsIdempotent(t *testing.T) {
	p := NewMockProvider(PayoutResult{})
	r := PayoutRequest{IdempotencyKey: "command-1", PaymentID: "payment-1", AmountMinor: 1250, Currency: "USD"}
	first, err := p.ExecutePayout(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	p.Result = PayoutResult{Status: StatusFailed, FailureCode: "declined"}
	second, err := p.ExecutePayout(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.Status != StatusSucceeded {
		t.Fatalf("results = %#v, %#v", first, second)
	}
}

func TestFailedResultRequiresCode(t *testing.T) {
	p := NewMockProvider(PayoutResult{Status: StatusFailed})
	_, err := p.ExecutePayout(context.Background(), PayoutRequest{IdempotencyKey: "x", PaymentID: "p", AmountMinor: 1, Currency: "USD"})
	if err == nil {
		t.Fatal("expected invalid result")
	}
}
