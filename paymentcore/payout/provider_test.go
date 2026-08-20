package payout

import (
	"testing"

	"stablerail/paymentcore"
)

func TestRequestValidation(t *testing.T) {
	if err := (paymentcore.ExecuteRequest{IdempotencyKey: "command-1", ProviderQuoteID: "quote-1"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (paymentcore.ExecuteRequest{IdempotencyKey: "command-1"}).Validate(); err == nil {
		t.Fatal("expected provider quote validation error")
	}
}

func TestFailedResultRequiresFailureCode(t *testing.T) {
	if err := (paymentcore.ExecutionResult{ProviderReference: "po_1", Status: paymentcore.ExecutionFailed}).Validate(); err == nil {
		t.Fatal("expected missing failure code error")
	}
	if err := (paymentcore.ExecutionResult{ProviderReference: "po_1", Status: paymentcore.ExecutionFailed, FailureCode: "declined"}).Validate(); err != nil {
		t.Fatal(err)
	}
}
