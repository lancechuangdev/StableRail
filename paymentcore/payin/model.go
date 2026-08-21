package payin

import (
	"encoding/json"
	"time"
)

// PayinStatus is the complete persisted payin-operation lifecycle.
type PayinStatus string

const (
	StatusCreated    PayinStatus = "created"
	StatusSubmitting PayinStatus = "submission_pending"
	StatusUnknown    PayinStatus = "unknown"
	StatusProcessing PayinStatus = "processing"
	StatusOnHold     PayinStatus = "on_hold"
	StatusReceived   PayinStatus = "received"
	StatusFailed     PayinStatus = "failed"
	StatusRefunded   PayinStatus = "refunded"
)

type Quote struct {
	Direction            string    `json:"direction"`
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
	PaymentID              string          `json:"payment_id"`
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
	Status                 PayinStatus     `json:"settlement_status"`
	Instructions           json.RawMessage `json:"instructions"`
	CreatedAt              time.Time       `json:"created_at"`
	UpdatedAt              time.Time       `json:"updated_at"`
}
