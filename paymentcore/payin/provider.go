// Package payin models inbound fiat payments within the payment domain.
package payin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type PayinStatus string

const (
	StatusCreated    PayinStatus = "created"
	StatusSubmitting PayinStatus = "submission_pending"
	StatusUnknown    PayinStatus = "unknown"
	StatusProcessing PayinStatus = "processing"
	StatusOnHold     PayinStatus = "on_hold"
	StatusReceived   PayinStatus = "received"
	StatusSucceeded  PayinStatus = "succeeded"
	StatusFailed     PayinStatus = "failed"
	StatusRefunded   PayinStatus = "refunded"
)

type QuoteRequest struct {
	IdempotencyKey, TenantID, FundingMethod, CurrencyType string
	SourceInstrumentID, DestinationAccountID              string
	SourceCurrency, DestinationCurrency                   string
	AmountMinor                                           int64
	CoverFees                                             bool
}
type QuoteResult struct {
	ProviderQuoteID, SourceCurrency, DestinationCurrency string
	SenderAmountMinor, ReceiverAmountMinor               int64
	ExpiresAt                                            time.Time
	Payload                                              json.RawMessage
}
type ExecuteRequest struct{ IdempotencyKey, QuoteID string }
type ExecuteResult struct {
	ProviderPayinID       string
	Status                PayinStatus
	Instructions, Payload json.RawMessage
	FailureReason         string
}
type ExecutionProvider interface {
	Name() string
	ExecutePayin(context.Context, ExecuteRequest) (ExecuteResult, error)
}
type Provider interface {
	ExecutionProvider
	CreatePayinQuote(context.Context, QuoteRequest) (QuoteResult, error)
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
func (r ExecuteResult) Validate() error {
	if r.ProviderPayinID == "" {
		return errors.New("provider payin ID is required")
	}
	switch r.Status {
	case StatusProcessing, StatusOnHold, StatusSucceeded, StatusFailed, StatusRefunded:
		return nil
	default:
		return fmt.Errorf("invalid payin status %q", r.Status)
	}
}

type Quote struct {
	ID                   string    `json:"id"`
	Provider             string    `json:"provider"`
	TenantID             string    `json:"tenant_id"`
	FundingMethod        string    `json:"funding_method"`
	SourceInstrumentID   string    `json:"source_instrument_id,omitempty"`
	DestinationAccountID string    `json:"destination_account_id"`
	SourceCurrency       string    `json:"source_currency"`
	DestinationCurrency  string    `json:"destination_currency"`
	SenderAmountMinor    int64     `json:"sender_amount_minor"`
	ReceiverAmountMinor  int64     `json:"receiver_amount_minor"`
	Status               string    `json:"status"`
	ExpiresAt            time.Time `json:"expires_at"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type Payin struct {
	ID                     string          `json:"id"`
	QuoteID                string          `json:"quote_id,omitempty"`
	Provider               string          `json:"provider"`
	ProviderPayinID        string          `json:"provider_payin_id,omitempty"`
	FundingMethod          string          `json:"funding_method"`
	SourceInstrumentID     string          `json:"source_instrument_id,omitempty"`
	DestinationAccountID   string          `json:"destination_account_id"`
	SourceAmountMinor      int64           `json:"source_amount_minor"`
	SourceCurrency         string          `json:"source_currency"`
	DestinationAmountMinor int64           `json:"destination_amount_minor"`
	DestinationCurrency    string          `json:"destination_currency"`
	Status                 PayinStatus     `json:"status"`
	Instructions           json.RawMessage `json:"instructions"`
	CreatedAt              time.Time       `json:"created_at"`
	UpdatedAt              time.Time       `json:"updated_at"`
}
