// Package payout defines provider-neutral outbound payment operations.
package payout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type PayoutStatus string

const (
	StatusPending   PayoutStatus = "pending"
	StatusOnHold    PayoutStatus = "on_hold"
	StatusSucceeded PayoutStatus = "succeeded"
	StatusFailed    PayoutStatus = "failed"
)

type Destination struct{ Type, Chain, Address string }

type Request struct {
	IdempotencyKey  string
	PaymentID       string
	TenantID        string
	QuoteID         string
	ProviderQuoteID string
	SourceAccountID string
	AmountMinor     int64
	Currency        string
	Destination     *Destination
}

type Result struct {
	ProviderReference string
	Status            PayoutStatus
	FailureCode       string
	FailureMessage    string
	Payload           json.RawMessage
}

type ExecutionProvider interface {
	Name() string
	ExecutePayout(context.Context, Request) (Result, error)
}

type Provider interface {
	ExecutionProvider
	CreatePayoutQuote(context.Context, QuoteRequest) (QuoteResult, error)
}

type ProviderError struct {
	Message   string
	Code      string
	Retryable bool
}

func (e *ProviderError) Error() string { return e.Message }

type QuoteRequest struct {
	IdempotencyKey, TenantID                          string
	SourceAccountID, DestinationInstrumentID          string
	SourceCurrency, DestinationCurrency, CurrencyType string
	CoverFees                                         bool
	RequestAmountMinor                                int64
}

type QuoteResult struct {
	ID, Provider, ProviderQuoteID, Status  string
	SourceCurrency, DestinationCurrency    string
	SenderAmountMinor, ReceiverAmountMinor int64
	CommercialRate, ProviderRate           string
	FlatFeeMinor, PartnerFeeMinor          int64
	BillingFeeMinor                        *int64
	ExpiresAt                              time.Time
	Payload                                json.RawMessage
}

func (r QuoteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.TenantID == "" || r.SourceAccountID == "" || r.DestinationInstrumentID == "" || r.SourceCurrency == "" || r.DestinationCurrency == "" || r.RequestAmountMinor <= 0 || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") {
		return errors.New("payout quote identity, route, currencies, positive amount, and valid currency type are required")
	}
	return nil
}

func (r Request) Validate() error {
	if r.IdempotencyKey == "" || r.PaymentID == "" || r.Currency == "" || r.AmountMinor <= 0 {
		return errors.New("payout identity, positive amount, and currency are required")
	}
	return nil
}

func (r Result) Validate() error {
	if r.ProviderReference == "" {
		return errors.New("provider reference is required")
	}
	switch r.Status {
	case StatusPending, StatusOnHold, StatusSucceeded:
		return nil
	case StatusFailed:
		if r.FailureCode == "" {
			return errors.New("failed payout requires a failure code")
		}
		return nil
	default:
		return fmt.Errorf("invalid payout status %q", r.Status)
	}
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
	Status                  string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}
