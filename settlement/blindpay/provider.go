package blindpay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"stablerail/paymentcore"
	"stablerail/paymentcore/payout"
)

type managedWalletPayoutClient interface {
	CreateEVMPayout(context.Context, PayoutRequest) (Payout, error)
}

// Provider adapts BlindPay payout and payin operations to the settlement boundary.
type Provider struct {
	quotes       *QuoteService
	client       *Client
	payoutClient managedWalletPayoutClient
	repo         *Repository
}

func NewProvider(quotes *QuoteService, client *Client, repo *Repository) (*Provider, error) {
	if quotes == nil || client == nil || repo == nil {
		return nil, errors.New("BlindPay quote, client, and repository dependencies are required")
	}
	return &Provider{quotes: quotes, client: client, payoutClient: client, repo: repo}, nil
}

func (*Provider) Name() string { return "blindpay" }

func (p *Provider) CreatePayoutQuote(ctx context.Context, request payout.QuoteRequest) (paymentcore.ProviderQuote, error) {
	source, err := p.quotes.repo.ResolveProviderResource(ctx, request.TenantID, request.SourceAccountID, "account")
	if err != nil {
		return paymentcore.ProviderQuote{}, err
	}
	var executionContext payoutExecutionContext
	if err := json.Unmarshal(source.Metadata, &executionContext); err != nil || executionContext.SenderWalletAddress == "" {
		return paymentcore.ProviderQuote{}, errors.New("BlindPay payout source account has no wallet address")
	}
	destination, err := p.quotes.repo.ResolveProviderResource(ctx, request.TenantID, request.DestinationInstrumentID, "payment_instrument")
	if err != nil {
		return paymentcore.ProviderQuote{}, err
	}
	quote, err := p.quotes.Create(ctx, PayoutQuoteRequest{
		IdempotencyKey: request.IdempotencyKey, TenantID: request.TenantID,
		BankAccountID: destination.ProviderReference, ManagedWalletID: source.ProviderReference,
		SourceAccountID: request.SourceAccountID, DestinationInstrumentID: request.DestinationInstrumentID,
		FundingMethod:       request.FundingMethod,
		DestinationCurrency: request.DestinationCurrency, CurrencyType: request.CurrencyType,
		CoverFees: request.CoverFees, AmountMinor: request.AmountMinor,
	})
	if err != nil {
		return paymentcore.ProviderQuote{}, err
	}
	contextPayload, err := json.Marshal(executionContext)
	if err != nil {
		return paymentcore.ProviderQuote{}, fmt.Errorf("encode BlindPay payout execution context: %w", err)
	}
	return paymentcore.ProviderQuote{
		ProviderQuoteID: quote.ProviderQuoteID,
		SourceCurrency:  quote.SourceCurrency, DestinationCurrency: quote.DestinationCurrency,
		SenderAmountMinor: quote.SenderAmountMinor, ReceiverAmountMinor: quote.ReceiverAmountMinor,
		CommercialRate: quote.CommercialRate, ProviderRate: quote.ProviderRate,
		FlatFeeMinor: quote.FlatFeeMinor, PartnerFeeMinor: quote.PartnerFeeMinor,
		BillingFeeMinor: quote.BillingFeeMinor, ExpiresAt: quote.ExpiresAt, Payload: quote.RawPayload,
		ExecutionContext: contextPayload,
	}, nil
}

func (p *Provider) ExecutePayout(ctx context.Context, request paymentcore.ExecuteRequest) (paymentcore.ExecutionResult, error) {
	if err := request.Validate(); err != nil {
		return paymentcore.ExecutionResult{}, err
	}
	contextPayload, err := p.repo.LoadPayoutExecutionContext(ctx, request.ProviderQuoteID)
	if err != nil {
		return paymentcore.ExecutionResult{}, err
	}
	var executionContext payoutExecutionContext
	if err := json.Unmarshal(contextPayload, &executionContext); err != nil || executionContext.SenderWalletAddress == "" {
		return paymentcore.ExecutionResult{}, errors.New("BlindPay payout quote has no execution context")
	}
	providerPayout, err := p.payoutClient.CreateEVMPayout(ctx, PayoutRequest{IdempotencyKey: request.IdempotencyKey, QuoteID: request.ProviderQuoteID, SenderWalletAddress: executionContext.SenderWalletAddress})
	if err != nil {
		var apiErr *APIError
		switch {
		case errors.As(err, &apiErr):
			retryable := apiErr.Kind == ErrorRetryable
			code := ""
			if !retryable {
				code = "submission_failed"
			}
			return paymentcore.ExecutionResult{}, &paymentcore.ProviderError{Message: err.Error(), Code: code, Retryable: retryable}
		default:
			return paymentcore.ExecutionResult{}, err
		}
	}
	result := paymentcore.ExecutionResult{ProviderReference: providerPayout.ID, Payload: providerPayout.RawPayload}
	switch providerPayout.Status {
	case "completed":
		result.Status = paymentcore.ExecutionSucceeded
	case "processing", "submission_pending", "unknown":
		result.Status = paymentcore.ExecutionPending
	case "on_hold":
		result.Status = paymentcore.ExecutionOnHold
	case "failed", "refunded", "submission_failed":
		result.Status, result.FailureCode = paymentcore.ExecutionFailed, providerPayout.Status
	default:
		return paymentcore.ExecutionResult{}, fmt.Errorf("unsupported BlindPay payout status %q", providerPayout.Status)
	}
	if err := result.Validate(); err != nil {
		return paymentcore.ExecutionResult{}, err
	}
	return result, nil
}
