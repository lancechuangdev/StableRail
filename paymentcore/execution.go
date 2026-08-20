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

// ProviderQuote is the provider-neutral result returned after StableRail asks a provider to create a quote.
type ProviderQuote struct {
	// Normalized quote data
	ProviderQuoteID                        string
	SourceCurrency, DestinationCurrency    string
	SenderAmountMinor, ReceiverAmountMinor int64
	CommercialRate, ProviderRate           string
	FlatFeeMinor, PartnerFeeMinor          int64
	BillingFeeMinor                        *int64
	ExpiresAt                              time.Time
	// The original provider response, retained for auditing and troubleshooting
	Payload json.RawMessage
	// provider-owned information required later to execute the quote
	ExecutionContext json.RawMessage
}

type ExecuteRequest struct {
	IdempotencyKey  string
	ProviderQuoteID string
}

func (r ExecuteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.ProviderQuoteID == "" {
		return errors.New("execution idempotency key and provider quote ID are required")
	}
	return nil
}

type QuoteRequest struct {
	IdempotencyKey      string
	TenantID            string
	FundingMethod       string
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

type ProviderError struct {
	Message, Code string
	Retryable     bool
}

func (e *ProviderError) Error() string { return e.Message }

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
