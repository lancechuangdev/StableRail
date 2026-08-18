package blindpay

import (
	"context"
	"strings"
	"time"

	"stablerail/settlement"
)

func (p *Provider) CreatePayinQuote(ctx context.Context, r settlement.PayinQuoteRequest) (settlement.PayinQuoteResult, error) {
	if err := r.Validate(); err != nil {
		return settlement.PayinQuoteResult{}, err
	}
	q, err := p.client.CreatePayinQuote(ctx, PayinQuoteRequest{IdempotencyKey: r.IdempotencyKey, WalletID: r.ManagedWalletID, BlockchainWalletID: r.BlockchainWalletID, CurrencyType: r.CurrencyType, CoverFees: r.CoverFees, RequestAmount: r.AmountMinor, PaymentMethod: r.PaymentMethod, Token: r.Token})
	if err != nil {
		return settlement.PayinQuoteResult{}, err
	}
	source := strings.ToUpper(r.SourceCurrency)
	if source == "" {
		source = payinCurrency(r.PaymentMethod)
	}
	return settlement.PayinQuoteResult{ProviderQuoteID: q.ID, SourceCurrency: source, DestinationCurrency: strings.ToUpper(r.Token), SenderAmountMinor: q.SenderAmount, ReceiverAmountMinor: q.ReceiverAmount, ExpiresAt: time.UnixMilli(q.ExpiresAt).UTC(), Payload: q.RawPayload}, nil
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
