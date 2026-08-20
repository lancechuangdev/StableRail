package paymentcore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ExecutionStatus is the provider-neutral outcome of an asynchronous
// settlement attempt. Direction-specific lifecycle detail is persisted by the
// payin or payout operation.
type ExecutionStatus string

const (
	ExecutionPending   ExecutionStatus = "pending"
	ExecutionOnHold    ExecutionStatus = "on_hold"
	ExecutionSucceeded ExecutionStatus = "succeeded"
	ExecutionFailed    ExecutionStatus = "failed"
)

type ProviderQuote struct {
	ProviderQuoteID                        string
	SourceCurrency, DestinationCurrency    string
	SenderAmountMinor, ReceiverAmountMinor int64
	CommercialRate, ProviderRate           string
	FlatFeeMinor, PartnerFeeMinor          int64
	BillingFeeMinor                        *int64
	ExpiresAt                              time.Time
	Payload                                json.RawMessage
}

type QuoteRequest struct {
	IdempotencyKey      string
	TenantID            string
	SourceCurrency      string
	DestinationCurrency string
	CurrencyType        string
	AmountMinor         int64
	CoverFees           bool
}

type ExecutionResult struct {
	ProviderReference string
	Status            ExecutionStatus
	FailureCode       string
	FailureMessage    string
	Payload           json.RawMessage
}

func (r ExecutionResult) Validate() error {
	if r.ProviderReference == "" {
		return errors.New("provider reference is required")
	}
	switch r.Status {
	case ExecutionPending, ExecutionOnHold, ExecutionSucceeded:
		return nil
	case ExecutionFailed:
		if r.FailureCode == "" {
			return errors.New("failed execution requires a failure code")
		}
		return nil
	default:
		return fmt.Errorf("invalid execution status %q", r.Status)
	}
}
