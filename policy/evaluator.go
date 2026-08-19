// Package policy defines payment policy evaluation contracts.
package policy

import "context"

type PolicyRequest struct {
	OperationID string
	Direction   string
	AmountMinor int64
	Currency    string
}

type PolicyDecision struct {
	Approved bool
	Reason   string
}

type PolicyEvaluator interface {
	Evaluate(context.Context, PolicyRequest) (PolicyDecision, error)
}

// DeterministicEvaluator approves every valid payment and is the local default.
type DeterministicEvaluator struct {
	// RejectAmountMinor is a local-test seam. Zero preserves the normal
	// approve-all behaviour.
	RejectAmountMinor int64
}

func (e DeterministicEvaluator) Evaluate(_ context.Context, request PolicyRequest) (PolicyDecision, error) {
	if request.OperationID == "" || (request.Direction != "payin" && request.Direction != "payout") || request.AmountMinor <= 0 || request.Currency == "" {
		return PolicyDecision{Approved: false, Reason: "invalid payment"}, nil
	}
	if e.RejectAmountMinor > 0 && request.AmountMinor == e.RejectAmountMinor {
		return PolicyDecision{Approved: false, Reason: "rejected by local policy"}, nil
	}
	return PolicyDecision{Approved: true}, nil
}
