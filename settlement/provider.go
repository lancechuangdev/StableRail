// Package settlement defines the boundary between StableRail and settlement providers.
package settlement

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"stablerail/paymentcore"
	"stablerail/paymentcore/payin"
	"stablerail/paymentcore/payout"
)

type SettlementProvider interface {
	payout.QuoteProvider
	payout.ExecutionProvider
	payin.QuoteProvider
	payin.ExecutionProvider
}

// MockProvider is deterministic and idempotent. It is safe for concurrent use.
// By default every new request succeeds; Result can be set to exercise other outcomes.
type MockProvider struct {
	mu              sync.Mutex
	Result          paymentcore.ExecutionResult
	ResultsByAmount map[int64]paymentcore.ExecutionResult
	amountsByQuote  map[string]int64
	seen            map[string]paymentcore.ExecutionResult
	now             func() time.Time
}

func NewMockProvider(result paymentcore.ExecutionResult) *MockProvider {
	if result.Status == "" {
		result = paymentcore.ExecutionResult{Status: paymentcore.ExecutionSucceeded}
	}
	return &MockProvider{Result: result, seen: make(map[string]paymentcore.ExecutionResult), amountsByQuote: make(map[string]int64), now: time.Now}
}

func (*MockProvider) Name() string { return "mock" }

func (p *MockProvider) ExecutePayout(_ context.Context, request paymentcore.ExecuteRequest) (paymentcore.ExecutionResult, error) {
	if err := request.Validate(); err != nil {
		return paymentcore.ExecutionResult{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if result, ok := p.seen[request.IdempotencyKey]; ok {
		return result, nil
	}
	result := p.Result
	if configured, ok := p.ResultsByAmount[p.amountsByQuote[request.ProviderQuoteID]]; ok {
		result = configured
	}
	if result.ProviderReference == "" {
		result.ProviderReference = "mock_" + request.IdempotencyKey
	}
	if err := result.Validate(); err != nil {
		return paymentcore.ExecutionResult{}, err
	}
	p.seen[request.IdempotencyKey] = result
	return result, nil
}

func (p *MockProvider) CreatePayoutQuote(_ context.Context, r payout.QuoteRequest) (paymentcore.ProviderQuote, error) {
	if r.IdempotencyKey == "" || r.AmountMinor <= 0 {
		return paymentcore.ProviderQuote{}, errors.New("invalid payout quote request")
	}
	providerQuoteID := "qu_" + r.IdempotencyKey
	p.mu.Lock()
	p.amountsByQuote[providerQuoteID] = r.AmountMinor
	p.mu.Unlock()
	return paymentcore.ProviderQuote{ProviderQuoteID: providerQuoteID, SourceCurrency: r.SourceCurrency, DestinationCurrency: r.DestinationCurrency, SenderAmountMinor: r.AmountMinor, ReceiverAmountMinor: r.AmountMinor, CommercialRate: "1", ProviderRate: "1", ExpiresAt: p.now().Add(5 * time.Minute)}, nil
}

func (p *MockProvider) CreatePayinQuote(_ context.Context, r payin.QuoteRequest) (paymentcore.ProviderQuote, error) {
	if err := r.Validate(); err != nil {
		return paymentcore.ProviderQuote{}, err
	}
	payload, _ := json.Marshal(map[string]any{"mock": true})
	return paymentcore.ProviderQuote{ProviderQuoteID: "pq_" + r.IdempotencyKey, SourceCurrency: r.SourceCurrency, DestinationCurrency: r.DestinationCurrency, SenderAmountMinor: r.AmountMinor, ReceiverAmountMinor: r.AmountMinor, ExpiresAt: p.now().Add(5 * time.Minute), Payload: payload}, nil
}

func (p *MockProvider) ExecutePayin(_ context.Context, r paymentcore.ExecuteRequest) (payin.ExecuteResult, error) {
	if err := r.Validate(); err != nil {
		return payin.ExecuteResult{}, err
	}
	instructions, _ := json.Marshal(map[string]string{"reference": "MOCK-" + r.ProviderQuoteID})
	return payin.ExecuteResult{ExecutionResult: paymentcore.ExecutionResult{ProviderReference: "pi_" + r.IdempotencyKey, Status: paymentcore.ExecutionSucceeded, Payload: instructions}, Instructions: instructions}, nil
}
