// Package settlement defines the boundary between StableRail and settlement providers.
package settlement

import (
	"context"
	"encoding/json"
	"errors"
	"stablerail/paymentcore/payin"
	"stablerail/paymentcore/payout"
	"sync"
	"time"
)

type SettlementProvider interface {
	payout.Provider
	payin.Provider
}

// MockProvider is deterministic and idempotent. It is safe for concurrent use.
// By default every new request succeeds; Result can be set to exercise other outcomes.
type MockProvider struct {
	mu              sync.Mutex
	Result          payout.Result
	ResultsByAmount map[int64]payout.Result
	seen            map[string]payout.Result
	now             func() time.Time
}

func NewMockProvider(result payout.Result) *MockProvider {
	if result.Status == "" {
		result = payout.Result{Status: payout.StatusSucceeded}
	}
	return &MockProvider{Result: result, seen: make(map[string]payout.Result), now: time.Now}
}

func (*MockProvider) Name() string { return "mock" }

func (p *MockProvider) ExecutePayout(_ context.Context, request payout.Request) (payout.Result, error) {
	if err := request.Validate(); err != nil {
		return payout.Result{}, err
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
		return payout.Result{}, err
	}
	p.seen[request.IdempotencyKey] = result
	return result, nil
}

func (p *MockProvider) CreatePayoutQuote(_ context.Context, r payout.QuoteRequest) (payout.QuoteResult, error) {
	if r.IdempotencyKey == "" || r.RequestAmountMinor <= 0 {
		return payout.QuoteResult{}, errors.New("invalid payout quote request")
	}
	return payout.QuoteResult{ProviderQuoteID: "qu_" + r.IdempotencyKey, SourceCurrency: r.SourceCurrency, DestinationCurrency: r.DestinationCurrency, SenderAmountMinor: r.RequestAmountMinor, ReceiverAmountMinor: r.RequestAmountMinor, CommercialRate: "1", ProviderRate: "1", ExpiresAt: p.now().Add(5 * time.Minute)}, nil
}

func (p *MockProvider) CreatePayinQuote(_ context.Context, r payin.QuoteRequest) (payin.QuoteResult, error) {
	if err := r.Validate(); err != nil {
		return payin.QuoteResult{}, err
	}
	payload, _ := json.Marshal(map[string]any{"mock": true})
	return payin.QuoteResult{ProviderQuoteID: "pq_" + r.IdempotencyKey, SourceCurrency: r.SourceCurrency, DestinationCurrency: r.DestinationCurrency, SenderAmountMinor: r.AmountMinor, ReceiverAmountMinor: r.AmountMinor, ExpiresAt: p.now().Add(5 * time.Minute), Payload: payload}, nil
}

func (p *MockProvider) ExecutePayin(_ context.Context, r payin.ExecuteRequest) (payin.ExecuteResult, error) {
	if r.IdempotencyKey == "" || r.QuoteID == "" {
		return payin.ExecuteResult{}, errors.New("payin idempotency key and quote ID are required")
	}
	instructions, _ := json.Marshal(map[string]string{"reference": "MOCK-" + r.QuoteID})
	return payin.ExecuteResult{ProviderPayinID: "pi_" + r.IdempotencyKey, Status: payin.StatusSucceeded, Instructions: instructions, Payload: instructions}, nil
}
