// Package payout defines provider-neutral outbound payment operations.
package payout

import (
	"context"
	"errors"

	"stablerail/paymentcore"
)

type QuoteRequest struct {
	paymentcore.QuoteRequest
	SourceAccountID         string
	DestinationInstrumentID string
}

type QuoteProvider interface {
	Name() string
	CreatePayoutQuote(context.Context, QuoteRequest) (paymentcore.ProviderQuote, error)
}

type ExecutionProvider interface {
	Name() string
	ExecutePayout(context.Context, paymentcore.ExecuteRequest) (paymentcore.ExecutionResult, error)
}

func (r QuoteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.TenantID == "" || r.FundingMethod == "" || r.SourceAccountID == "" || r.DestinationInstrumentID == "" || r.SourceCurrency == "" || r.DestinationCurrency == "" || r.AmountMinor <= 0 || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") {
		return errors.New("payout quote identity, funding method, route, currencies, positive amount, and valid currency type are required")
	}
	return nil
}
