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
	SourceInstrumentID   string
	DestinationAccountID string
}

type ExecuteResult struct {
	paymentcore.ExecutionResult
	Instructions json.RawMessage
}

type QuoteProvider interface {
	Name() string
	CreatePayinQuote(context.Context, QuoteRequest) (paymentcore.ProviderQuote, error)
}

type ExecutionProvider interface {
	Name() string
	ExecutePayin(context.Context, paymentcore.ExecuteRequest) (ExecuteResult, error)
}

func (r QuoteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.TenantID == "" || r.FundingMethod == "" || r.SourceCurrency == "" || r.DestinationCurrency == "" || r.DestinationAccountID == "" || r.AmountMinor <= 0 || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") {
		return errors.New("payin quote identity, funding method, currencies, destination account, positive amount, and valid currency type are required")
	}
	return nil
}
