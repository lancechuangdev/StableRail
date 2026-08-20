package settlement

import (
	"context"
	"reflect"
	"testing"

	"stablerail/paymentcore"
)

func TestMockProviderIsIdempotent(t *testing.T) {
	p := NewMockProvider(paymentcore.ExecutionResult{})
	r := paymentcore.ExecuteRequest{IdempotencyKey: "command-1", ProviderQuoteID: "quote-1"}
	first, err := p.ExecutePayout(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	p.Result = paymentcore.ExecutionResult{Status: paymentcore.ExecutionFailed, FailureCode: "declined"}
	second, err := p.ExecutePayout(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || first.Status != paymentcore.ExecutionSucceeded {
		t.Fatalf("results = %#v, %#v", first, second)
	}
}

func TestFailedResultRequiresCode(t *testing.T) {
	p := NewMockProvider(paymentcore.ExecutionResult{Status: paymentcore.ExecutionFailed})
	_, err := p.ExecutePayout(context.Background(), paymentcore.ExecuteRequest{IdempotencyKey: "x", ProviderQuoteID: "quote-1"})
	if err == nil {
		t.Fatal("expected invalid result")
	}
}
