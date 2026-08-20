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
	Direction                              string `json:"direction"`
	ID, Provider, ProviderQuoteID, Status  string
	SourceCurrency, DestinationCurrency    string
	SenderAmountMinor, ReceiverAmountMinor int64
	CommercialRate, ProviderRate           string
	FlatFeeMinor, PartnerFeeMinor          int64
	BillingFeeMinor                        *int64
	ExpiresAt                              time.Time
	Payload                                json.RawMessage
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
