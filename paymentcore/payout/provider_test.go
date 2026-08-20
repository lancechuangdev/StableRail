package payout

import (
	"testing"

	"stablerail/paymentcore"
)

func TestRequestValidation(t *testing.T) {
	if err := (ExecuteRequest{IdempotencyKey: "command-1", PaymentID: "payment-1", AmountMinor: 1250, Currency: "USD"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (ExecuteRequest{IdempotencyKey: "command-1", PaymentID: "payment-1", Currency: "USD"}).Validate(); err == nil {
		t.Fatal("expected positive amount validation error")
	}
}

func TestFailedResultRequiresFailureCode(t *testing.T) {
	if err := (ExecutionResult{ProviderReference: "po_1", Status: paymentcore.ExecutionFailed}).Validate(); err == nil {
		t.Fatal("expected missing failure code error")
	}
	if err := (ExecutionResult{ProviderReference: "po_1", Status: paymentcore.ExecutionFailed, FailureCode: "declined"}).Validate(); err != nil {
		t.Fatal(err)
	}
}
