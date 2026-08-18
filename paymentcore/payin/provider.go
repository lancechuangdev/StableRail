// Package payin models inbound fiat payments within the payment domain.
package payin

import (
	"encoding/json"
	"stablerail/settlement"
	"time"
)

type Status = settlement.PayinStatus

const (
	StatusProcessing = settlement.PayinStatusProcessing
	StatusOnHold     = settlement.PayinStatusOnHold
	StatusSucceeded  = settlement.PayinStatusSucceeded
	StatusFailed     = settlement.PayinStatusFailed
	StatusRefunded   = settlement.PayinStatusRefunded
)

type QuoteRequest = settlement.PayinQuoteRequest
type QuoteResult = settlement.PayinQuoteResult
type ExecuteRequest = settlement.PayinRequest
type ExecuteResult = settlement.PayinResult

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
	ProviderPayinID        string          `json:"provider_payin_id"`
	FundingMethod          string          `json:"funding_method"`
	SourceInstrumentID     string          `json:"source_instrument_id,omitempty"`
	DestinationAccountID   string          `json:"destination_account_id"`
	SourceAmountMinor      int64           `json:"source_amount_minor"`
	SourceCurrency         string          `json:"source_currency"`
	DestinationAmountMinor int64           `json:"destination_amount_minor"`
	DestinationCurrency    string          `json:"destination_currency"`
	Status                 Status          `json:"status"`
	Instructions           json.RawMessage `json:"instructions"`
	CreatedAt              time.Time       `json:"created_at"`
	UpdatedAt              time.Time       `json:"updated_at"`
}
