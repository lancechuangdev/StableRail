// Package settlement defines the boundary between StableRail and settlement providers.
package settlement

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type ProviderError struct {
	Message   string
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

type SettlementRequest struct {
	IdempotencyKey string
	PaymentID      string
	AmountMinor    int64
	Currency       string
	Destination    *Destination
}

type Destination struct{ Type, Chain, Address string }

type SettlementResult struct {
	ProviderReference string
	Status            Status
	FailureCode       string
	FailureMessage    string
}

func (r SettlementRequest) Validate() error {
	if r.IdempotencyKey == "" || r.PaymentID == "" || r.Currency == "" || r.AmountMinor <= 0 {
		return errors.New("settlement request identity, positive amount, and currency are required")
	}
	return nil
}

func (r SettlementResult) Validate() error {
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

type SettlementProvider interface {
	Name() string
	Submit(context.Context, SettlementRequest) (SettlementResult, error)
}

// MockProvider is deterministic and idempotent. It is safe for concurrent use.
// By default every new request succeeds; Result can be set to exercise other outcomes.
type MockProvider struct {
	mu              sync.Mutex
	Result          SettlementResult
	ResultsByAmount map[int64]SettlementResult
	seen            map[string]SettlementResult
}

func NewMockProvider(result SettlementResult) *MockProvider {
	if result.Status == "" {
		result = SettlementResult{Status: StatusSucceeded}
	}
	return &MockProvider{Result: result, seen: make(map[string]SettlementResult)}
}

func (*MockProvider) Name() string { return "mock" }

func (p *MockProvider) Submit(_ context.Context, request SettlementRequest) (SettlementResult, error) {
	if err := request.Validate(); err != nil {
		return SettlementResult{}, err
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
		return SettlementResult{}, err
	}
	p.seen[request.IdempotencyKey] = result
	return result, nil
}
