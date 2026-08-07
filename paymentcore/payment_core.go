package paymentcore

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// PaymentState represents the lifecycle state of a payment.
type PaymentState string

const (
	StateCreated    PaymentState = "created"
	StateProcessing PaymentState = "processing"
	StateSettled    PaymentState = "settled"
	StateFailed     PaymentState = "failed"
)

// Payment represents a payment intent and its ledger state.
type Payment struct {
	ID                string
	ExternalReference string
	Currency          string
	AmountMinor       int64 // The payment amount expressed in the currency’s smallest unit
	CustomerID        string
	State             PaymentState
	LedgerBalance     int64
	AuditLog          []AuditEvent
	Timeline          []TimelineEntry
	CreatedAt         time.Time
	UpdatedAt         time.Time
	IdempotencyKey    string
}

// AuditEvent records a state transition or domain event.
type AuditEvent struct {
	Event   string
	Message string
	At      time.Time
}

// TimelineEntry is a public-facing entry for the timeline API.
type TimelineEntry struct {
	State PaymentState
	At    time.Time
	Note  string
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
func (s *Service) CreatePayment(externalRef, currency string, amountMinor int64, customerID, idempotencyKey string) (*Payment, error) {
	if externalRef == "" || currency == "" || amountMinor <= 0 || customerID == "" || idempotencyKey == "" {
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
		CustomerID:        customerID,
		State:             StateCreated,
		LedgerBalance:     0,
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
	payment.LedgerBalance = payment.AmountMinor
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
	payment.LedgerBalance = payment.AmountMinor
	payment.AuditLog = append(payment.AuditLog, AuditEvent{Event: "settled", Message: "payment settled successfully", At: now})
	payment.Timeline = append(payment.Timeline, TimelineEntry{State: StateSettled, At: now, Note: "payment settled"})
	return nil
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
		return nil, fmt.Errorf("payment %s not found", paymentID)
	}
	return clonePayment(payment), nil
}

func clonePayment(payment *Payment) *Payment {
	clone := *payment
	clone.AuditLog = append([]AuditEvent(nil), payment.AuditLog...)
	clone.Timeline = append([]TimelineEntry(nil), payment.Timeline...)
	return &clone
}
