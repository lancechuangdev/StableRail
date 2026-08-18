// Package settlement defines the boundary between StableRail and settlement providers.
package settlement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type ProviderError struct {
	Message   string
	Code      string
	Retryable bool
}

func (e *ProviderError) Error() string { return e.Message }

type Status string

const (
	StatusPending   Status = "pending"
	StatusOnHold    Status = "on_hold"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type PayoutRequest struct {
	IdempotencyKey string
	PaymentID      string
	AmountMinor    int64
	Currency       string
	Destination    *Destination
}

type Destination struct{ Type, Chain, Address string }

type PayoutResult struct {
	ProviderReference string
	Status            Status
	FailureCode       string
	FailureMessage    string
}

func (r PayoutRequest) Validate() error {
	if r.IdempotencyKey == "" || r.PaymentID == "" || r.Currency == "" || r.AmountMinor <= 0 {
		return errors.New("settlement request identity, positive amount, and currency are required")
	}
	return nil
}

func (r PayoutResult) Validate() error {
	if r.ProviderReference == "" {
		return errors.New("provider reference is required")
	}
	switch r.Status {
	case StatusPending, StatusOnHold, StatusSucceeded:
		return nil
	case StatusFailed:
		if r.FailureCode == "" {
			return errors.New("failed settlement requires a failure code")
		}
		return nil
	default:
		return fmt.Errorf("invalid settlement status %q", r.Status)
	}
}

type PayoutQuoteRequest struct {
	IdempotencyKey, TenantID, BankAccountID, ManagedWalletID string
	DestinationCurrency, CurrencyType, PartnerFeeID          string
	CoverFees                                                bool
	RequestAmountMinor                                       int64
}
type PayoutQuoteResult struct {
	ID, Provider, Status, SourceCurrency, DestinationCurrency string
	SenderAmountMinor, ReceiverAmountMinor                    int64
	ExpiresAt                                                 time.Time
}
type PayinQuoteRequest struct {
	IdempotencyKey, TenantID, PaymentMethod, CurrencyType string
	ManagedWalletID, BlockchainWalletID                   string
	Token, SourceCurrency                                 string
	AmountMinor                                           int64
	CoverFees                                             bool
}
type PayinQuoteResult struct {
	ProviderQuoteID, SourceCurrency, DestinationCurrency string
	SenderAmountMinor, ReceiverAmountMinor               int64
	ExpiresAt                                            time.Time
	Payload                                              json.RawMessage
}
type PayinRequest struct{ IdempotencyKey, QuoteID string }
type PayinStatus string

const (
	PayinStatusProcessing PayinStatus = "processing"
	PayinStatusOnHold     PayinStatus = "on_hold"
	PayinStatusSucceeded  PayinStatus = "succeeded"
	PayinStatusFailed     PayinStatus = "failed"
	PayinStatusRefunded   PayinStatus = "refunded"
)

type PayinResult struct {
	ProviderPayinID       string
	Status                PayinStatus
	Instructions, Payload json.RawMessage
}

func (r PayinQuoteRequest) Validate() error {
	if r.IdempotencyKey == "" || r.TenantID == "" || r.PaymentMethod == "" || r.SourceCurrency == "" || r.AmountMinor <= 0 || r.Token == "" || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") {
		return errors.New("payin quote identity, payment method, token, positive amount, and valid currency type are required")
	}
	if (r.ManagedWalletID == "") == (r.BlockchainWalletID == "") {
		return errors.New("exactly one payin destination wallet is required")
	}
	return nil
}

func (r PayinResult) Validate() error {
	if r.ProviderPayinID == "" {
		return errors.New("provider payin ID is required")
	}
	switch r.Status {
	case PayinStatusProcessing, PayinStatusOnHold, PayinStatusSucceeded, PayinStatusFailed, PayinStatusRefunded:
		return nil
	default:
		return fmt.Errorf("invalid payin status %q", r.Status)
	}
}

type PayoutProvider interface {
	CreatePayoutQuote(context.Context, PayoutQuoteRequest) (PayoutQuoteResult, error)
	ExecutePayout(context.Context, PayoutRequest) (PayoutResult, error)
}
type PayinProvider interface {
	CreatePayinQuote(context.Context, PayinQuoteRequest) (PayinQuoteResult, error)
	ExecutePayin(context.Context, PayinRequest) (PayinResult, error)
}
type SettlementProvider interface {
	Name() string
	PayoutProvider
	PayinProvider
}

// MockProvider is deterministic and idempotent. It is safe for concurrent use.
// By default every new request succeeds; Result can be set to exercise other outcomes.
type MockProvider struct {
	mu              sync.Mutex
	Result          PayoutResult
	ResultsByAmount map[int64]PayoutResult
	seen            map[string]PayoutResult
	now             func() time.Time
}

func NewMockProvider(result PayoutResult) *MockProvider {
	if result.Status == "" {
		result = PayoutResult{Status: StatusSucceeded}
	}
	return &MockProvider{Result: result, seen: make(map[string]PayoutResult), now: time.Now}
}

func (*MockProvider) Name() string { return "mock" }

func (p *MockProvider) ExecutePayout(_ context.Context, request PayoutRequest) (PayoutResult, error) {
	if err := request.Validate(); err != nil {
		return PayoutResult{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if result, ok := p.seen[request.IdempotencyKey]; ok {
		return result, nil
	}
	result := p.Result
	if configured, ok := p.ResultsByAmount[request.AmountMinor]; ok {
		result = configured
	}
	if result.ProviderReference == "" {
		result.ProviderReference = "mock_" + request.IdempotencyKey
	}
	if err := result.Validate(); err != nil {
		return PayoutResult{}, err
	}
	p.seen[request.IdempotencyKey] = result
	return result, nil
}

func (p *MockProvider) CreatePayoutQuote(_ context.Context, r PayoutQuoteRequest) (PayoutQuoteResult, error) {
	if r.IdempotencyKey == "" || r.RequestAmountMinor <= 0 {
		return PayoutQuoteResult{}, errors.New("invalid payout quote request")
	}
	return PayoutQuoteResult{ID: "qu_" + r.IdempotencyKey, Provider: p.Name(), Status: "open", SourceCurrency: "USD", DestinationCurrency: r.DestinationCurrency, SenderAmountMinor: r.RequestAmountMinor, ReceiverAmountMinor: r.RequestAmountMinor, ExpiresAt: p.now().Add(5 * time.Minute)}, nil
}

func (p *MockProvider) CreatePayinQuote(_ context.Context, r PayinQuoteRequest) (PayinQuoteResult, error) {
	if err := r.Validate(); err != nil {
		return PayinQuoteResult{}, err
	}
	payload, _ := json.Marshal(map[string]any{"mock": true})
	return PayinQuoteResult{ProviderQuoteID: "pq_" + r.IdempotencyKey, SourceCurrency: r.SourceCurrency, DestinationCurrency: r.Token, SenderAmountMinor: r.AmountMinor, ReceiverAmountMinor: r.AmountMinor, ExpiresAt: p.now().Add(5 * time.Minute), Payload: payload}, nil
}

func (p *MockProvider) ExecutePayin(_ context.Context, r PayinRequest) (PayinResult, error) {
	if r.IdempotencyKey == "" || r.QuoteID == "" {
		return PayinResult{}, errors.New("payin idempotency key and quote ID are required")
	}
	instructions, _ := json.Marshal(map[string]string{"reference": "MOCK-" + r.QuoteID})
	return PayinResult{ProviderPayinID: "pi_" + r.IdempotencyKey, Status: PayinStatusSucceeded, Instructions: instructions, Payload: instructions}, nil
}
