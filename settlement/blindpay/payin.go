package blindpay

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	corepayin "stablerail/paymentcore/payin"
)

func (p *Provider) CreatePayinQuote(ctx context.Context, r corepayin.QuoteRequest) (corepayin.QuoteResult, error) {
	if err := r.Validate(); err != nil {
		return corepayin.QuoteResult{}, err
	}
	destination, err := p.quotes.repo.ResolveProviderResource(ctx, r.TenantID, r.DestinationAccountID, "account")
	if err != nil {
		return corepayin.QuoteResult{}, err
	}
	var metadata struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(destination.Metadata, &metadata); err != nil {
		return corepayin.QuoteResult{}, err
	}
	walletID, blockchainWalletID := "", ""
	if metadata.Kind == "managed_wallet" {
		walletID = destination.ProviderReference
	} else {
		blockchainWalletID = destination.ProviderReference
	}
	q, err := p.client.CreatePayinQuote(ctx, PayinQuoteRequest{IdempotencyKey: r.IdempotencyKey, WalletID: walletID, BlockchainWalletID: blockchainWalletID, CurrencyType: r.CurrencyType, CoverFees: r.CoverFees, RequestAmount: r.AmountMinor, PaymentMethod: r.FundingMethod, Token: r.DestinationCurrency})
	if err != nil {
		return corepayin.QuoteResult{}, err
	}
	source := strings.ToUpper(r.SourceCurrency)
	if source == "" {
		source = payinCurrency(r.FundingMethod)
	}
	return corepayin.QuoteResult{ProviderQuoteID: q.ID, SourceCurrency: source, DestinationCurrency: strings.ToUpper(r.DestinationCurrency), SenderAmountMinor: q.SenderAmount, ReceiverAmountMinor: q.ReceiverAmount, ExpiresAt: time.UnixMilli(q.ExpiresAt).UTC(), Payload: q.RawPayload}, nil
}
func (p *Provider) ExecutePayin(ctx context.Context, r corepayin.ExecuteRequest) (corepayin.ExecuteResult, error) {
	v, err := p.client.CreatePayin(ctx, PayinRequest{IdempotencyKey: r.IdempotencyKey, PayinQuoteID: r.QuoteID})
	if err != nil {
		return corepayin.ExecuteResult{}, err
	}
	return corepayin.ExecuteResult{ProviderPayinID: v.ID, Status: mapPayinStatus(v.Status), Instructions: v.Instructions, Payload: v.RawPayload}, nil
}
func mapPayinStatus(v string) corepayin.PayinStatus {
	switch v {
	case "completed":
		return corepayin.StatusSucceeded
	case "failed":
		return corepayin.StatusFailed
	case "refunded":
		return corepayin.StatusRefunded
	case "on_hold":
		return corepayin.StatusOnHold
	default:
		return corepayin.StatusProcessing
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
