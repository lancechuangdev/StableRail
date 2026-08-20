// Package payin models inbound fiat payments within the payment domain.
package payin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"stablerail/paymentcore"
)

type QuoteRequest struct {
	IdempotencyKey, TenantID, FundingMethod, CurrencyType string
	SourceInstrumentID, DestinationAccountID              string
	SourceCurrency, DestinationCurrency                   string
	AmountMinor                                           int64
	CoverFees                                             bool
}
type ProviderQuote struct {
	ProviderQuoteID, SourceCurrency, DestinationCurrency string
	SenderAmountMinor, ReceiverAmountMinor               int64
	ExpiresAt                                            time.Time
	Payload                                              json.RawMessage
}
type ExecuteRequest struct {
	IdempotencyKey         string
	QuoteID                string
	TenantID               string
	FundingMethod          string
	SourceInstrumentID     string
	DestinationAccountID   string
	SourceAmountMinor      int64
	SourceCurrency         string
	DestinationAmountMinor int64
	DestinationCurrency    string
}
type ExecuteResult struct {
	ProviderReference     string
	Status                paymentcore.ExecutionStatus
	Instructions, Payload json.RawMessage
	FailureCode           string
	FailureMessage        string
}
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
	if r.IdempotencyKey == "" || r.TenantID == "" || r.FundingMethod == "" || r.DestinationAccountID == "" || r.SourceAmountMinor <= 0 || r.SourceCurrency == "" || r.DestinationAmountMinor <= 0 || r.DestinationCurrency == "" {
		return errors.New("payin execution identity, route, currencies, and positive amounts are required")
	}
	return nil
}
func (r ExecuteResult) Validate() error {
	if r.ProviderReference == "" {
		return errors.New("provider reference is required")
	}
	switch r.Status {
	case paymentcore.ExecutionPending, paymentcore.ExecutionOnHold, paymentcore.ExecutionSucceeded, paymentcore.ExecutionFailed:
		return nil
	default:
		return fmt.Errorf("invalid payin status %q", r.Status)
	}
}
