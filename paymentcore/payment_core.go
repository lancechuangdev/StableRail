package paymentcore

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrPaymentNotFound     = errors.New("payment not found")
	ErrIdempotencyConflict = errors.New("idempotency key is bound to a different request")
)

// PaymentState represents the lifecycle state of a payment.
type PaymentState string

const (
	StateCreated    PaymentState = "created"
	StateProcessing PaymentState = "processing"
	StateSettled    PaymentState = "settled"
	StateFailed     PaymentState = "failed"
	StateRefunded   PaymentState = "refunded"
)

// Payment represents a payment intent and its ledger state.
type Payment struct {
	ID                string          `json:"id"`
	ExternalReference string          `json:"external_reference"`
	Currency          string          `json:"currency"`
	AmountMinor       int64           `json:"amount_minor"` // The payment amount expressed in the currency’s smallest unit
	TenantID          string          `json:"tenant_id"`
	State             PaymentState    `json:"state"`
	LedgerEntries     []LedgerEntry   `json:"ledger_entries,omitempty"`
	AuditLog          []AuditEvent    `json:"audit_log,omitempty"`
	Timeline          []TimelineEntry `json:"timeline,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	IdempotencyKey    string          `json:"-"`
	PayoutQuoteID     string          `json:"payout_quote_id,omitempty"`
	Destination       *Destination    `json:"destination,omitempty"`
}

// Destination is immutable settlement routing selected when a payment is created.
type Destination struct {
	Type    string `json:"type"`
	Chain   string `json:"chain,omitempty"`
	Address string `json:"address,omitempty"`
}

func (d Destination) Validate() error {
	switch d.Type {
	case "blockchain_address":
		if d.Chain == "" || d.Address == "" {
			return errors.New("blockchain_address destination requires chain and address")
		}
	default:
		return errors.New("unsupported payment destination")
	}
	return nil
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
	State PaymentState `json:"state"`
	At    time.Time    `json:"at"`
	Note  string       `json:"note"`
}

// Service provides payment lifecycle operations.
type Service struct {
	mu            sync.RWMutex
	payments      map[string]*Payment
	byIdempotency map[string]string
	nextID        uint64
}

// NewService creates a payment service instance.
func NewService() *Service {
	return &Service{
		payments:      make(map[string]*Payment),
		byIdempotency: make(map[string]string),
	}
}

// CreatePayment creates a new payment or returns an existing one for the idempotency key.
func (s *Service) CreatePayment(externalRef, currency string, amountMinor int64, tenantID, idempotencyKey string) (*Payment, error) {
	if externalRef == "" || currency == "" || amountMinor <= 0 || tenantID == "" || idempotencyKey == "" {
		return nil, errors.New("invalid payment payload")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, ok := s.byIdempotency[idempotencyKey]; ok {
		return clonePayment(s.payments[existingID]), nil
	}

	now := time.Now().UTC()
	s.nextID++
	payment := &Payment{
		ID:                fmt.Sprintf("pay_%d", s.nextID),
		ExternalReference: externalRef,
		Currency:          currency,
		AmountMinor:       amountMinor,
		TenantID:          tenantID,
		State:             StateCreated,
		CreatedAt:         now,
		UpdatedAt:         now,
		IdempotencyKey:    idempotencyKey,
	}

	payment.AuditLog = []AuditEvent{{Event: "created", Message: "payment intent created", At: now}}
	payment.Timeline = []TimelineEntry{{State: StateCreated, At: now, Note: "payment created"}}

	s.payments[payment.ID] = payment
	s.byIdempotency[idempotencyKey] = payment.ID
	return clonePayment(payment), nil
}

// Process transitions the payment from created to processing.
func (s *Service) Process(paymentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payment, ok := s.payments[paymentID]
	if !ok {
		return fmt.Errorf("payment %s not found", paymentID)
	}
	if payment.State != StateCreated {
		return fmt.Errorf("payment %s cannot transition from %s", paymentID, payment.State)
	}

	now := time.Now().UTC()
	payment.State = StateProcessing
	payment.UpdatedAt = now
	payment.LedgerEntries = append(payment.LedgerEntries,
		newLedgerLine(payment, "processing", CashOperatingAccount, EntryDebit, now),
		newLedgerLine(payment, "processing", SettlementAccount, EntryCredit, now),
	)
	payment.AuditLog = append(payment.AuditLog, AuditEvent{Event: "processing", Message: "payment processing started", At: now})
	payment.Timeline = append(payment.Timeline, TimelineEntry{State: StateProcessing, At: now, Note: "payment processing"})
	return nil
}

// Settle settles a payment and records the final ledger update.
func (s *Service) Settle(paymentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payment, ok := s.payments[paymentID]
	if !ok {
		return fmt.Errorf("payment %s not found", paymentID)
	}
	if payment.State != StateProcessing {
		return fmt.Errorf("payment %s cannot settle from %s", paymentID, payment.State)
	}

	now := time.Now().UTC()
	payment.State = StateSettled
	payment.UpdatedAt = now
	payment.LedgerEntries = append(payment.LedgerEntries,
		newLedgerLine(payment, "settled", SettlementAccount, EntryDebit, now),
		newLedgerLine(payment, "settled", CashOperatingAccount, EntryCredit, now),
	)
	payment.AuditLog = append(payment.AuditLog, AuditEvent{Event: "settled", Message: "payment settled successfully", At: now})
	payment.Timeline = append(payment.Timeline, TimelineEntry{State: StateSettled, At: now, Note: "payment settled"})
	return nil
}

func newLedgerLine(payment *Payment, transition, account string, side EntrySide, at time.Time) LedgerEntry {
	transactionID := fmt.Sprintf("jrn_%s_%s", payment.ID, transition)
	return LedgerEntry{
		ID: fmt.Sprintf("led_%s_%s", transactionID, side), TransactionID: transactionID,
		AccountCode: account, Side: side, AmountMinor: payment.AmountMinor,
		Currency: payment.Currency, PaymentID: payment.ID, At: at,
	}
}

// Timeline returns the timeline entries for a payment.
func (s *Service) Timeline(paymentID string) []TimelineEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	payment, ok := s.payments[paymentID]
	if !ok {
		return nil
	}
	return append([]TimelineEntry(nil), payment.Timeline...)
}

// GetPayment returns a snapshot of a payment. Mutating it does not alter service state.
func (s *Service) GetPayment(paymentID string) (*Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	payment, ok := s.payments[paymentID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPaymentNotFound, paymentID)
	}
	return clonePayment(payment), nil
}

func clonePayment(payment *Payment) *Payment {
	clone := *payment
	clone.LedgerEntries = append([]LedgerEntry(nil), payment.LedgerEntries...)
	clone.AuditLog = append([]AuditEvent(nil), payment.AuditLog...)
	clone.Timeline = append([]TimelineEntry(nil), payment.Timeline...)
	return &clone
}
