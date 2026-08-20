// Package payin models inbound fiat payments within the payment domain.
package payin

import (
	"context"
	"encoding/json"
	"errors"

	"stablerail/paymentcore"
)

type QuoteRequest struct {
	paymentcore.QuoteRequest
	FundingMethod        string
	SourceInstrumentID   string
	DestinationAccountID string
}

type ExecuteRequest struct {
	IdempotencyKey  string
	ProviderQuoteID string
}

type ExecuteResult struct {
	paymentcore.ExecutionResult
	Instructions json.RawMessage
}

type ProviderQuote = paymentcore.ProviderQuote

type QuoteProvider interface {
	Name() string
	CreatePayinQuote(context.Context, QuoteRequest) (ProviderQuote, error)
}

type ExecutionProvider interface {
	Name() string
	ExecutePayin(context.Context, ExecuteRequest) (ExecuteResult, error)
}

type ProviderError struct {
	Message, Code string
	Retryable     bool
}

func (e *ProviderError) Error() string { return e.Message }

func (r QuoteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.TenantID == "" || r.FundingMethod == "" || r.SourceCurrency == "" || r.DestinationCurrency == "" || r.DestinationAccountID == "" || r.AmountMinor <= 0 || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") {
		return errors.New("payin quote identity, funding method, currencies, destination account, positive amount, and valid currency type are required")
	}
	return nil
}

func (r ExecuteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.ProviderQuoteID == "" {
		return errors.New("payin execution idempotency key and provider quote ID are required")
	}
	return nil
}
