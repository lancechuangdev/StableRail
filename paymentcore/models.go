package paymentcore

import (
	"errors"
	"time"
)

var (
	ErrPaymentNotFound      = errors.New("payment not found")
	ErrIdempotencyConflict  = errors.New("idempotency key is bound to a different request")
	ErrPaymentNotRefundable = errors.New("payment is not refundable")
	ErrRefundAmountExceeded = errors.New("refund amount exceeds the remaining refundable amount")
)

// PaymentStatus represents the delivery lifecycle of a payment.
type PaymentStatus string

const (
	PaymentStatusCreated    PaymentStatus = "created"
	PaymentStatusProcessing PaymentStatus = "processing"
	PaymentStatusSucceeded  PaymentStatus = "succeeded"
	PaymentStatusFailed     PaymentStatus = "failed"
)

type PaymentDirection string

const (
	PaymentDirectionPayin  PaymentDirection = "payin"
	PaymentDirectionPayout PaymentDirection = "payout"
)

// Payment represents the merchant-facing business outcome of a payment.
type Payment struct {
	ID                string               `json:"id"`
	Direction         PaymentDirection     `json:"direction"`
	ExternalReference string               `json:"external_reference"`
	Currency          string               `json:"currency"`
	AmountMinor       int64                `json:"amount_minor"` // The payment amount expressed in the currency’s smallest unit
	TenantID          string               `json:"tenant_id"`
	PaymentStatus     PaymentStatus        `json:"payment_status"`
	LedgerEntries     []LedgerEntry        `json:"ledger_entries,omitempty"`
	AuditLog          []AuditEvent         `json:"audit_log,omitempty"`
	Timeline          []TimelineEntry      `json:"timeline,omitempty"`
	CreatedAt         time.Time            `json:"created_at"`
	UpdatedAt         time.Time            `json:"updated_at"`
	IdempotencyKey    string               `json:"-"`
	QuoteID           string               `json:"quote_id,omitempty"`
	Settlement        *SettlementOperation `json:"settlement,omitempty"`
}

// Refund is a merchant-issued payout linked to an original payment.
type Refund struct {
	ID              string    `json:"id"`
	PaymentID       string    `json:"payment_id"`
	RefundPaymentID string    `json:"refund_payment_id"`
	Reason          string    `json:"reason"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	IdempotencyKey  string    `json:"-"`
}

// SettlementOperation exposes provider execution state without making payins
// and payouts separate public resources.
type SettlementOperation struct {
	Provider             string `json:"provider"`
	ProviderReference    string `json:"provider_reference,omitempty"`
	Status               string `json:"settlement_status"`
	ReconciliationStatus string `json:"reconciliation_status"`
	Instructions         any    `json:"instructions,omitempty"`
}

type AccountType string

const (
	AccountAsset     AccountType = "asset"
	AccountLiability AccountType = "liability"
)

type EntrySide string

const (
	EntryDebit  EntrySide = "debit"
	EntryCredit EntrySide = "credit"
)

// LedgerEntry is one debit or credit line in a balanced journal transaction.
type LedgerEntry struct {
	ID            string
	TransactionID string
	AccountCode   string
	Side          EntrySide
	AmountMinor   int64
	Currency      string
	PaymentID     string
	At            time.Time
}

const (
	CashOperatingAccount = "cash:operating"
	SettlementAccount    = "settlement:payable"
)

// AuditEvent records a state transition or domain event.
type AuditEvent struct {
	Event   string
	Message string
	At      time.Time
}

// TimelineEntry is a public-facing entry for the timeline API.
type TimelineEntry struct {
	PaymentStatus PaymentStatus `json:"payment_status"`
	At            time.Time     `json:"at"`
	Note          string        `json:"note"`
}
