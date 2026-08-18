package settlement

import (
	"context"
	"reflect"
	"stablerail/paymentcore/payout"
	"testing"
)

func TestMockProviderIsIdempotent(t *testing.T) {
	p := NewMockProvider(payout.Result{})
	r := payout.Request{IdempotencyKey: "command-1", PaymentID: "payment-1", AmountMinor: 1250, Currency: "USD"}
	first, err := p.ExecutePayout(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	p.Result = payout.Result{Status: payout.StatusFailed, FailureCode: "declined"}
	second, err := p.ExecutePayout(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || first.Status != payout.StatusSucceeded {
		t.Fatalf("results = %#v, %#v", first, second)
	}
}

func TestFailedResultRequiresCode(t *testing.T) {
	p := NewMockProvider(payout.Result{Status: payout.StatusFailed})
	_, err := p.ExecutePayout(context.Background(), payout.Request{IdempotencyKey: "x", PaymentID: "p", AmountMinor: 1, Currency: "USD"})
	if err == nil {
		t.Fatal("expected invalid result")
	}
}
