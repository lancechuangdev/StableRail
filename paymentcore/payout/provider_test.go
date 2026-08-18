package payout

import "testing"

func TestRequestValidation(t *testing.T) {
	if err := (Request{IdempotencyKey: "command-1", PaymentID: "payment-1", AmountMinor: 1250, Currency: "USD"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Request{IdempotencyKey: "command-1", PaymentID: "payment-1", Currency: "USD"}).Validate(); err == nil {
		t.Fatal("expected positive amount validation error")
	}
}

func TestFailedResultRequiresFailureCode(t *testing.T) {
	if err := (Result{ProviderReference: "po_1", Status: StatusFailed}).Validate(); err == nil {
		t.Fatal("expected missing failure code error")
	}
	if err := (Result{ProviderReference: "po_1", Status: StatusFailed, FailureCode: "declined"}).Validate(); err != nil {
		t.Fatal(err)
	}
}
