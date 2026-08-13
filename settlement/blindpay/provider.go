package blindpay

import (
	"context"
	"errors"
	"fmt"

	"stablerail/settlement"
)

// Provider adapts managed-wallet payout submission to the settlement boundary.
type Provider struct{ payouts *PayoutService }

func NewProvider(payouts *PayoutService) (*Provider, error) {
	if payouts == nil {
		return nil, errors.New("BlindPay payout service is required")
	}
	return &Provider{payouts: payouts}, nil
}

func (*Provider) Name() string { return "blindpay" }

func (p *Provider) Submit(ctx context.Context, request settlement.SettlementRequest) (settlement.SettlementResult, error) {
	if err := request.Validate(); err != nil {
		return settlement.SettlementResult{}, err
	}
	payout, err := p.payouts.SubmitPayment(ctx, request.PaymentID, request.IdempotencyKey)
	if err != nil {
		var apiErr *APIError
		switch {
		case errors.Is(err, ErrPayoutSubmissionUnknown):
			return settlement.SettlementResult{}, &settlement.ProviderError{Message: err.Error(), Retryable: true}
		case errors.As(err, &apiErr):
			return settlement.SettlementResult{}, &settlement.ProviderError{Message: err.Error(), Retryable: apiErr.Kind == ErrorRetryable}
		default:
			return settlement.SettlementResult{}, err
		}
	}
	result := settlement.SettlementResult{ProviderReference: payout.ProviderPayoutID}
	switch payout.ProviderStatus {
	case "completed":
		result.Status = settlement.StatusSucceeded
	case "processing", "submission_pending", "unknown":
		result.Status = settlement.StatusPending
	case "on_hold":
		result.Status = settlement.StatusOnHold
	case "failed", "refunded", "submission_failed":
		result.Status, result.FailureCode = settlement.StatusFailed, payout.ProviderStatus
	default:
		return settlement.SettlementResult{}, fmt.Errorf("unsupported BlindPay payout status %q", payout.ProviderStatus)
	}
	if err := result.Validate(); err != nil {
		return settlement.SettlementResult{}, err
	}
	return result, nil
}
