package payout

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrQuoteIdempotencyConflict = errors.New("idempotency key is bound to another payout quote")

func (s *Service) CreateQuote(ctx context.Context, request QuoteRequest) (QuoteResult, error) {
	if err := request.Validate(); err != nil {
		return QuoteResult{}, err
	}
	var existing QuoteResult
	var sourceAccountID, destinationInstrumentID, currencyType string
	var coverFees bool
	var requestAmount int64
	err := s.db.QueryRowContext(ctx, `SELECT id,provider,provider_quote_id,status,source_currency,destination_currency,sender_amount_minor,receiver_amount_minor,commercial_rate,provider_rate,flat_fee_minor,partner_fee_minor,billing_fee_minor,expires_at,provider_payload,source_resource_id,destination_resource_id,currency_type,cover_fees,request_amount_minor FROM payment_quotes WHERE direction='payout' AND tenant_id=$1 AND idempotency_key=$2`, request.TenantID, request.IdempotencyKey).Scan(&existing.ID, &existing.Provider, &existing.ProviderQuoteID, &existing.Status, &existing.SourceCurrency, &existing.DestinationCurrency, &existing.SenderAmountMinor, &existing.ReceiverAmountMinor, &existing.CommercialRate, &existing.ProviderRate, &existing.FlatFeeMinor, &existing.PartnerFeeMinor, &existing.BillingFeeMinor, &existing.ExpiresAt, &existing.Payload, &sourceAccountID, &destinationInstrumentID, &currencyType, &coverFees, &requestAmount)
	if err == nil {
		existing.Direction = "payout"
		if sourceAccountID != request.SourceAccountID || destinationInstrumentID != request.DestinationInstrumentID || existing.SourceCurrency != request.SourceCurrency || existing.DestinationCurrency != request.DestinationCurrency || currencyType != request.CurrencyType || coverFees != request.CoverFees || requestAmount != request.RequestAmountMinor {
			return QuoteResult{}, ErrQuoteIdempotencyConflict
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return QuoteResult{}, fmt.Errorf("lookup payout quote: %w", err)
	}
	result, err := s.provider.CreatePayoutQuote(ctx, request)
	if err != nil {
		return QuoteResult{}, err
	}
	if result.ProviderQuoteID == "" || result.SourceCurrency == "" || result.DestinationCurrency == "" || result.SenderAmountMinor <= 0 || result.ReceiverAmountMinor <= 0 || !result.ExpiresAt.After(s.now()) {
		return QuoteResult{}, errors.New("provider returned an invalid payout quote")
	}
	id, err := s.newID("pqi_")
	if err != nil {
		return QuoteResult{}, err
	}
	payload := result.Payload
	if len(payload) == 0 {
		payload, _ = json.Marshal(result)
	}
	now := s.now()
	_, err = s.db.ExecContext(ctx, `INSERT INTO payment_quotes(id,direction,provider,provider_quote_id,tenant_id,idempotency_key,source_resource_id,destination_resource_id,payment_method,source_currency,destination_currency,currency_type,cover_fees,request_amount_minor,sender_amount_minor,receiver_amount_minor,commercial_rate,provider_rate,flat_fee_minor,partner_fee_minor,billing_fee_minor,status,expires_at,provider_payload,created_at,updated_at) VALUES($1,'payout',$2,$3,$4,$5,$6,$7,'provider_quote',$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,'open',$20,$21,$22,$22)`, id, s.provider.Name(), result.ProviderQuoteID, request.TenantID, request.IdempotencyKey, request.SourceAccountID, request.DestinationInstrumentID, result.SourceCurrency, result.DestinationCurrency, request.CurrencyType, request.CoverFees, request.RequestAmountMinor, result.SenderAmountMinor, result.ReceiverAmountMinor, result.CommercialRate, result.ProviderRate, result.FlatFeeMinor, result.PartnerFeeMinor, result.BillingFeeMinor, result.ExpiresAt, payload, now)
	if err != nil {
		return QuoteResult{}, fmt.Errorf("store payout quote: %w", err)
	}
	result.Direction, result.ID, result.Provider, result.Status, result.Payload = "payout", id, s.provider.Name(), "open", payload
	return result, nil
}
