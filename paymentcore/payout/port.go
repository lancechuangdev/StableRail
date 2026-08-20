// Package payout defines provider-neutral outbound payment operations.
package payout

import (
	"context"
	"errors"

	"stablerail/paymentcore"
)

type QuoteRequest struct {
	paymentcore.QuoteRequest
	FundingMethod           string
	SourceAccountID         string
	DestinationInstrumentID string
}

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

type ExecutionResult = paymentcore.ExecutionResult

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

type ProviderQuote = paymentcore.ProviderQuote

func (e *ProviderError) Error() string { return e.Message }

func (r QuoteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.TenantID == "" || r.FundingMethod == "" || r.SourceAccountID == "" || r.DestinationInstrumentID == "" || r.SourceCurrency == "" || r.DestinationCurrency == "" || r.AmountMinor <= 0 || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") {
		return errors.New("payout quote identity, funding method, route, currencies, positive amount, and valid currency type are required")
	}
	return nil
}

func (r ExecuteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.PaymentID == "" || r.Currency == "" || r.AmountMinor <= 0 {
		return errors.New("payout identity, positive amount, and currency are required")
	}
	return nil
}
