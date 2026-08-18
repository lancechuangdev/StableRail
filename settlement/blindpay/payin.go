package blindpay

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"stablerail/settlement"
)

func (p *Provider) CreatePayinQuote(ctx context.Context, r settlement.PayinQuoteRequest) (settlement.PayinQuoteResult, error) {
	if err := r.Validate(); err != nil {
		return settlement.PayinQuoteResult{}, err
	}
	destination, err := p.quotes.repo.ResolveProviderResource(ctx, r.TenantID, r.DestinationAccountID, "account")
	if err != nil {
		return settlement.PayinQuoteResult{}, err
	}
	var metadata struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(destination.Metadata, &metadata); err != nil {
		return settlement.PayinQuoteResult{}, err
	}
	walletID, blockchainWalletID := "", ""
	if metadata.Kind == "managed_wallet" {
		walletID = destination.ProviderReference
	} else {
		blockchainWalletID = destination.ProviderReference
	}
	q, err := p.client.CreatePayinQuote(ctx, PayinQuoteRequest{IdempotencyKey: r.IdempotencyKey, WalletID: walletID, BlockchainWalletID: blockchainWalletID, CurrencyType: r.CurrencyType, CoverFees: r.CoverFees, RequestAmount: r.AmountMinor, PaymentMethod: r.FundingMethod, Token: r.DestinationCurrency})
	if err != nil {
		return settlement.PayinQuoteResult{}, err
	}
	source := strings.ToUpper(r.SourceCurrency)
	if source == "" {
		source = payinCurrency(r.FundingMethod)
	}
	return settlement.PayinQuoteResult{ProviderQuoteID: q.ID, SourceCurrency: source, DestinationCurrency: strings.ToUpper(r.DestinationCurrency), SenderAmountMinor: q.SenderAmount, ReceiverAmountMinor: q.ReceiverAmount, ExpiresAt: time.UnixMilli(q.ExpiresAt).UTC(), Payload: q.RawPayload}, nil
}
func (p *Provider) ExecutePayin(ctx context.Context, r settlement.PayinRequest) (settlement.PayinResult, error) {
	v, err := p.client.CreatePayin(ctx, PayinRequest{IdempotencyKey: r.IdempotencyKey, PayinQuoteID: r.QuoteID})
	if err != nil {
		return settlement.PayinResult{}, err
	}
	return settlement.PayinResult{ProviderPayinID: v.ID, Status: mapPayinStatus(v.Status), Instructions: v.Instructions, Payload: v.RawPayload}, nil
}
func mapPayinStatus(v string) settlement.PayinStatus {
	switch v {
	case "completed":
		return settlement.PayinStatusSucceeded
	case "failed":
		return settlement.PayinStatusFailed
	case "refunded":
		return settlement.PayinStatusRefunded
	case "on_hold":
		return settlement.PayinStatusOnHold
	default:
		return settlement.PayinStatusProcessing
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
