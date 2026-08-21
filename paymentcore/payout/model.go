package payout

import (
	"encoding/json"
	"time"
)

// PayoutStatus is the complete persisted payout-operation lifecycle.
type PayoutStatus string

const (
	PayoutStatusSubmissionPending PayoutStatus = "submission_pending"
	PayoutStatusUnknown           PayoutStatus = "unknown"
	PayoutStatusSubmissionFailed  PayoutStatus = "submission_failed"
	PayoutStatusProcessing        PayoutStatus = "processing"
	PayoutStatusOnHold            PayoutStatus = "on_hold"
	PayoutStatusCompleted         PayoutStatus = "completed"
	PayoutStatusFailed            PayoutStatus = "failed"
	PayoutStatusRefunded          PayoutStatus = "refunded"
)

// Quote is the persisted, provider-neutral payout quote exposed by the core.
type Quote struct {
	Direction           string          `json:"direction"`
	ID                  string          `json:"id"`
	Provider            string          `json:"provider"`
	ProviderQuoteID     string          `json:"provider_quote_id"`
	Status              string          `json:"status"`
	SourceCurrency      string          `json:"source_currency"`
	DestinationCurrency string          `json:"destination_currency"`
	SenderAmountMinor   int64           `json:"sender_amount_minor"`
	ReceiverAmountMinor int64           `json:"receiver_amount_minor"`
	CommercialRate      string          `json:"commercial_rate"`
	ProviderRate        string          `json:"provider_rate"`
	FlatFeeMinor        int64           `json:"flat_fee_minor"`
	PartnerFeeMinor     int64           `json:"partner_fee_minor"`
	BillingFeeMinor     *int64          `json:"billing_fee_minor,omitempty"`
	ExpiresAt           time.Time       `json:"expires_at"`
	Payload             json.RawMessage `json:"provider_payload,omitempty"`
}

// Operation is the provider-neutral persisted payout snapshot.
type Operation struct {
	PaymentID               string
	QuoteID                 string
	TenantID                string
	SourceAccountID         string
	DestinationInstrumentID string
	Method                  string
	SourceAmountMinor       int64
	SourceCurrency          string
	DestinationAmountMinor  int64
	DestinationCurrency     string
	Provider                string
	ProviderReference       string
	Status                  PayoutStatus
	CreatedAt               time.Time
	UpdatedAt               time.Time
}
