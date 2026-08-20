package blindpay

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"stablerail/paymentcore"
	corepayin "stablerail/paymentcore/payin"
)

func (p *Provider) CreatePayinQuote(ctx context.Context, r corepayin.QuoteRequest) (corepayin.ProviderQuote, error) {
	if err := r.Validate(); err != nil {
		return corepayin.ProviderQuote{}, err
	}
	destination, err := p.quotes.repo.ResolveProviderResource(ctx, r.TenantID, r.DestinationAccountID, "account")
	if err != nil {
		return corepayin.ProviderQuote{}, err
	}
	var metadata struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(destination.Metadata, &metadata); err != nil {
		return corepayin.ProviderQuote{}, err
	}
	walletID, blockchainWalletID := "", ""
	if metadata.Kind == "managed_wallet" {
		walletID = destination.ProviderReference
	} else {
		blockchainWalletID = destination.ProviderReference
	}
	q, err := p.client.CreatePayinQuote(ctx, PayinQuoteRequest{IdempotencyKey: r.IdempotencyKey, WalletID: walletID, BlockchainWalletID: blockchainWalletID, CurrencyType: r.CurrencyType, CoverFees: r.CoverFees, RequestAmount: r.AmountMinor, PaymentMethod: r.FundingMethod, Token: r.DestinationCurrency})
	if err != nil {
		return corepayin.ProviderQuote{}, err
	}
	source := strings.ToUpper(r.SourceCurrency)
	if source == "" {
		source = payinCurrency(r.FundingMethod)
	}
	return corepayin.ProviderQuote{ProviderQuoteID: q.ID, SourceCurrency: source, DestinationCurrency: strings.ToUpper(r.DestinationCurrency), SenderAmountMinor: q.SenderAmount, ReceiverAmountMinor: q.ReceiverAmount, ExpiresAt: time.UnixMilli(q.ExpiresAt).UTC(), Payload: q.RawPayload}, nil
}
func (p *Provider) ExecutePayin(ctx context.Context, r corepayin.ExecuteRequest) (corepayin.ExecuteResult, error) {
	providerQuoteID := r.QuoteID
	if providerQuoteID == "" {
		q, err := p.CreatePayinQuote(ctx, corepayin.QuoteRequest{IdempotencyKey: r.IdempotencyKey + ":quote", TenantID: r.TenantID, FundingMethod: r.FundingMethod, CurrencyType: "sender", SourceInstrumentID: r.SourceInstrumentID, DestinationAccountID: r.DestinationAccountID, SourceCurrency: r.SourceCurrency, DestinationCurrency: r.DestinationCurrency, AmountMinor: r.SourceAmountMinor})
		if err != nil {
			return corepayin.ExecuteResult{}, err
		}
		providerQuoteID = q.ProviderQuoteID
	}
	v, err := p.client.CreatePayin(ctx, PayinRequest{IdempotencyKey: r.IdempotencyKey, PayinQuoteID: providerQuoteID})
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			return corepayin.ExecuteResult{}, &corepayin.ProviderError{Message: err.Error(), Code: apiErr.Code, Retryable: apiErr.Kind == ErrorRetryable}
		}
		return corepayin.ExecuteResult{}, &corepayin.ProviderError{Message: err.Error(), Retryable: true}
	}
	status := mapPayinStatus(v.Status)
	result := corepayin.ExecuteResult{ProviderReference: v.ID, Status: status, Instructions: v.Instructions, Payload: v.RawPayload}
	if v.Status == "refunded" {
		result.FailureCode = "refunded"
	}
	return result, nil
}
func mapPayinStatus(v string) paymentcore.ExecutionStatus {
	switch v {
	case "completed":
		return paymentcore.ExecutionSucceeded
	case "failed":
		return paymentcore.ExecutionFailed
	case "refunded":
		return paymentcore.ExecutionFailed
	case "on_hold":
		return paymentcore.ExecutionOnHold
	default:
		return paymentcore.ExecutionPending
	}
}
func payinCurrency(method string) string {
	switch method {
	case "pix":
		return "BRL"
	case "spei":
		return "MXN"
	case "transfers":
		return "ARS"
	case "pse":
		return "COP"
	default:
		return "USD"
	}
}
