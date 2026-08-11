// Package settlement defines the boundary between StableRail and settlement providers.
package settlement

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type Request struct {
	IdempotencyKey string
	PaymentID      string
	AmountMinor    int64
	Currency       string
}

type Result struct {
	ProviderReference string
	Status            Status
	FailureCode       string
	FailureMessage    string
}

func (r Request) Validate() error {
	if r.IdempotencyKey == "" || r.PaymentID == "" || r.Currency == "" || r.AmountMinor <= 0 {
		return errors.New("settlement request identity, positive amount, and currency are required")
	}
	return nil
}

func (r Result) Validate() error {
	if r.ProviderReference == "" {
		return errors.New("provider reference is required")
	}
	switch r.Status {
	case StatusPending, StatusSucceeded:
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

type Provider interface {
	Name() string
	Submit(context.Context, Request) (Result, error)
}

// MockProvider is deterministic and idempotent. It is safe for concurrent use.
// By default every new request succeeds; Result can be set to exercise other outcomes.
type MockProvider struct {
	mu     sync.Mutex
	Result Result
	seen   map[string]Result
}

func NewMockProvider(result Result) *MockProvider {
	if result.Status == "" {
		result = Result{Status: StatusSucceeded}
	}
	return &MockProvider{Result: result, seen: make(map[string]Result)}
}

func (*MockProvider) Name() string { return "mock" }

func (p *MockProvider) Submit(_ context.Context, request Request) (Result, error) {
	if err := request.Validate(); err != nil {
		return Result{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if result, ok := p.seen[request.IdempotencyKey]; ok {
		return result, nil
	}
	result := p.Result
	if result.ProviderReference == "" {
		result.ProviderReference = "mock_" + request.IdempotencyKey
	}
	if err := result.Validate(); err != nil {
		return Result{}, err
	}
	p.seen[request.IdempotencyKey] = result
	return result, nil
}
