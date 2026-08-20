package settlement

import (
	"context"
	"reflect"
	"testing"

	"stablerail/paymentcore"
	"stablerail/paymentcore/payout"
)

func TestMockProviderIsIdempotent(t *testing.T) {
	p := NewMockProvider(payout.ExecutionResult{})
	r := payout.ExecuteRequest{IdempotencyKey: "command-1", PaymentID: "payment-1", AmountMinor: 1250, Currency: "USD"}
	first, err := p.ExecutePayout(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	p.Result = payout.ExecutionResult{Status: paymentcore.ExecutionFailed, FailureCode: "declined"}
	second, err := p.ExecutePayout(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || first.Status != paymentcore.ExecutionSucceeded {
		t.Fatalf("results = %#v, %#v", first, second)
	}
}

func TestFailedResultRequiresCode(t *testing.T) {
	p := NewMockProvider(payout.ExecutionResult{Status: paymentcore.ExecutionFailed})
	_, err := p.ExecutePayout(context.Background(), payout.ExecuteRequest{IdempotencyKey: "x", PaymentID: "p", AmountMinor: 1, Currency: "USD"})
	if err == nil {
		t.Fatal("expected invalid result")
	}
}
