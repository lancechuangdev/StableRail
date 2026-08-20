// Package payout defines provider-neutral outbound payment operations.
package payout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"stablerail/paymentcore"
)

type ExecuteRequest struct {
	IdempotencyKey  string
	PaymentID       string
	TenantID        string
	QuoteID         string
	ProviderQuoteID string
	SourceAccountID string
	AmountMinor     int64
	Currency        string
}

type ExecutionResult struct {
	ProviderReference string
	Status            paymentcore.ExecutionStatus
	FailureCode       string
	FailureMessage    string
	Payload           json.RawMessage
}

type QuoteProvider interface {
	Name() string
	CreatePayoutQuote(context.Context, QuoteRequest) (ProviderQuote, error)
}

type ExecutionProvider interface {
	Name() string
	ExecutePayout(context.Context, ExecuteRequest) (ExecutionResult, error)
}

type ProviderError struct {
	Message   string
	Code      string
	Retryable bool
}

func (e *ProviderError) Error() string { return e.Message }

type QuoteRequest struct {
	IdempotencyKey, TenantID                          string
	SourceAccountID, DestinationInstrumentID          string
	SourceCurrency, DestinationCurrency, CurrencyType string
	CoverFees                                         bool
	AmountMinor                                       int64
}

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

func (r QuoteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.TenantID == "" || r.SourceAccountID == "" || r.DestinationInstrumentID == "" || r.SourceCurrency == "" || r.DestinationCurrency == "" || r.AmountMinor <= 0 || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") {
		return errors.New("payout quote identity, route, currencies, positive amount, and valid currency type are required")
	}
	return nil
}

func (r ExecuteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.PaymentID == "" || r.Currency == "" || r.AmountMinor <= 0 {
		return errors.New("payout identity, positive amount, and currency are required")
	}
	return nil
}

func (r ExecutionResult) Validate() error {
	if r.ProviderReference == "" {
		return errors.New("provider reference is required")
	}
	switch r.Status {
	case paymentcore.ExecutionPending, paymentcore.ExecutionOnHold, paymentcore.ExecutionSucceeded:
		return nil
	case paymentcore.ExecutionFailed:
		if r.FailureCode == "" {
			return errors.New("failed payout requires a failure code")
		}
		return nil
	default:
		return fmt.Errorf("invalid payout status %q", r.Status)
	}
}
