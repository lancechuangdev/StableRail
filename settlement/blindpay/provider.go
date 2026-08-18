package blindpay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

func (p *Provider) CreatePayoutQuote(ctx context.Context, request payout.QuoteRequest) (payout.QuoteResult, error) {
	source, err := p.quotes.repo.ResolveProviderResource(ctx, request.TenantID, request.SourceAccountID, "account")
	if err != nil {
		return payout.QuoteResult{}, err
	}
	destination, err := p.quotes.repo.ResolveProviderResource(ctx, request.TenantID, request.DestinationInstrumentID, "payment_instrument")
	if err != nil {
		return payout.QuoteResult{}, err
	}
	quote, err := p.quotes.Create(ctx, PayoutQuoteRequest{
		IdempotencyKey: request.IdempotencyKey, TenantID: request.TenantID,
		BankAccountID: destination.ProviderReference, ManagedWalletID: source.ProviderReference,
		SourceAccountID: request.SourceAccountID, DestinationInstrumentID: request.DestinationInstrumentID,
		DestinationCurrency: request.DestinationCurrency, CurrencyType: request.CurrencyType,
		CoverFees: request.CoverFees, RequestAmountMinor: request.RequestAmountMinor,
	})
	if err != nil {
		return payout.QuoteResult{}, err
	}
	return payout.QuoteResult{
		ProviderQuoteID: quote.ProviderQuoteID,
		SourceCurrency:  quote.SourceCurrency, DestinationCurrency: quote.DestinationCurrency,
		SenderAmountMinor: quote.SenderAmountMinor, ReceiverAmountMinor: quote.ReceiverAmountMinor,
		CommercialRate: quote.CommercialRate, ProviderRate: quote.ProviderRate,
		FlatFeeMinor: quote.FlatFeeMinor, PartnerFeeMinor: quote.PartnerFeeMinor,
		BillingFeeMinor: quote.BillingFeeMinor, ExpiresAt: quote.ExpiresAt, Payload: quote.RawPayload,
	}, nil
}

func (p *Provider) ExecutePayout(ctx context.Context, request payout.Request) (payout.Result, error) {
	if err := request.Validate(); err != nil {
		return payout.Result{}, err
	}
	resource, err := p.repo.ResolveProviderResource(ctx, request.TenantID, request.SourceAccountID, "account")
	if err != nil {
		return payout.Result{}, err
	}
	var metadata struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(resource.Metadata, &metadata); err != nil || metadata.Address == "" {
		return payout.Result{}, errors.New("BlindPay payout source account has no wallet address")
	}
	providerPayout, err := p.payoutClient.CreateEVMPayout(ctx, PayoutRequest{IdempotencyKey: request.IdempotencyKey, QuoteID: request.ProviderQuoteID, SenderWalletAddress: metadata.Address})
	if err != nil {
		var apiErr *APIError
		switch {
		case errors.As(err, &apiErr):
			retryable := apiErr.Kind == ErrorRetryable
			code := ""
			if !retryable {
				code = "submission_failed"
			}
			return payout.Result{}, &payout.ProviderError{Message: err.Error(), Code: code, Retryable: retryable}
		default:
			return payout.Result{}, err
		}
	}
	result := payout.Result{ProviderReference: providerPayout.ID, Payload: providerPayout.RawPayload}
	switch providerPayout.Status {
	case "completed":
		result.Status = payout.StatusSucceeded
	case "processing", "submission_pending", "unknown":
		result.Status = payout.StatusPending
	case "on_hold":
		result.Status = payout.StatusOnHold
	case "failed", "refunded", "submission_failed":
		result.Status, result.FailureCode = payout.StatusFailed, providerPayout.Status
	default:
		return payout.Result{}, fmt.Errorf("unsupported BlindPay payout status %q", providerPayout.Status)
	}
	if err := result.Validate(); err != nil {
		return payout.Result{}, err
	}
	return result, nil
}
