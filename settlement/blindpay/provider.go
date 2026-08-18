package blindpay

import (
	"context"
	"errors"
	"fmt"

	"stablerail/settlement"
)

// Provider adapts BlindPay payout and payin operations to the settlement boundary.
type Provider struct {
	payouts *PayoutService
	quotes  *QuoteService
	client  *Client
}

func NewProvider(payouts *PayoutService, quotes *QuoteService, client *Client) (*Provider, error) {
	if payouts == nil || quotes == nil || client == nil {
		return nil, errors.New("BlindPay payout, quote, and client dependencies are required")
	}
	return &Provider{payouts: payouts, quotes: quotes, client: client}, nil
}

func (*Provider) Name() string { return "blindpay" }

func (p *Provider) CreatePayoutQuote(ctx context.Context, request settlement.PayoutQuoteRequest) (settlement.PayoutQuoteResult, error) {
	quote, err := p.quotes.Create(ctx, PayoutQuoteRequest{
		IdempotencyKey: request.IdempotencyKey, TenantID: request.TenantID,
		BankAccountID: request.BankAccountID, ManagedWalletID: request.ManagedWalletID,
		DestinationCurrency: request.DestinationCurrency, CurrencyType: request.CurrencyType,
		CoverFees: request.CoverFees, RequestAmountMinor: request.RequestAmountMinor,
		PartnerFeeID: request.PartnerFeeID,
	})
	if err != nil {
		return settlement.PayoutQuoteResult{}, err
	}
	return settlement.PayoutQuoteResult{
		ID: quote.ID, Provider: quote.Provider, Status: quote.Status,
		SourceCurrency: quote.SourceCurrency, DestinationCurrency: quote.DestinationCurrency,
		SenderAmountMinor: quote.SenderAmountMinor, ReceiverAmountMinor: quote.ReceiverAmountMinor,
		ExpiresAt: quote.ExpiresAt,
	}, nil
}

func (p *Provider) ExecutePayout(ctx context.Context, request settlement.PayoutRequest) (settlement.PayoutResult, error) {
	if err := request.Validate(); err != nil {
		return settlement.PayoutResult{}, err
	}
	payout, err := p.payouts.SubmitPayment(ctx, request.PaymentID, request.IdempotencyKey)
	if err != nil {
		var apiErr *APIError
		switch {
		case errors.Is(err, ErrPayoutSubmissionUnknown):
			return settlement.PayoutResult{}, &settlement.ProviderError{Message: err.Error(), Retryable: true}
		case errors.As(err, &apiErr):
			retryable := apiErr.Kind == ErrorRetryable
			code := ""
			if !retryable {
				code = "submission_failed"
			}
			return settlement.PayoutResult{}, &settlement.ProviderError{Message: err.Error(), Code: code, Retryable: retryable}
		default:
			return settlement.PayoutResult{}, err
		}
	}
	result := settlement.PayoutResult{ProviderReference: payout.ProviderPayoutID}
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
		return settlement.PayoutResult{}, fmt.Errorf("unsupported BlindPay payout status %q", payout.ProviderStatus)
	}
	if err := result.Validate(); err != nil {
		return settlement.PayoutResult{}, err
	}
	return result, nil
}
