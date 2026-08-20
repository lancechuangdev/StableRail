package payout

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrQuoteIdempotencyConflict = errors.New("idempotency key is bound to another payout quote")

func (s *Service) CreateQuote(ctx context.Context, request QuoteRequest) (Quote, error) {
	if err := request.Validate(); err != nil {
		return Quote{}, err
	}
	var existing Quote
	var sourceAccountID, destinationInstrumentID, fundingMethod, currencyType string
	var coverFees bool
	var requestAmount int64
	err := s.db.QueryRowContext(ctx, `SELECT id,provider,provider_quote_id,status,source_currency,destination_currency,sender_amount_minor,receiver_amount_minor,commercial_rate,provider_rate,flat_fee_minor,partner_fee_minor,billing_fee_minor,expires_at,provider_payload,source_resource_id,destination_resource_id,payment_method,currency_type,cover_fees,request_amount_minor FROM payment_quotes WHERE direction='payout' AND tenant_id=$1 AND idempotency_key=$2`, request.TenantID, request.IdempotencyKey).Scan(&existing.ID, &existing.Provider, &existing.ProviderQuoteID, &existing.Status, &existing.SourceCurrency, &existing.DestinationCurrency, &existing.SenderAmountMinor, &existing.ReceiverAmountMinor, &existing.CommercialRate, &existing.ProviderRate, &existing.FlatFeeMinor, &existing.PartnerFeeMinor, &existing.BillingFeeMinor, &existing.ExpiresAt, &existing.Payload, &sourceAccountID, &destinationInstrumentID, &fundingMethod, &currencyType, &coverFees, &requestAmount)
	if err == nil {
		existing.Direction = "payout"
		if sourceAccountID != request.SourceAccountID || destinationInstrumentID != request.DestinationInstrumentID || fundingMethod != request.FundingMethod || existing.SourceCurrency != request.SourceCurrency || existing.DestinationCurrency != request.DestinationCurrency || currencyType != request.CurrencyType || coverFees != request.CoverFees || requestAmount != request.AmountMinor {
			return Quote{}, ErrQuoteIdempotencyConflict
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Quote{}, fmt.Errorf("lookup payout quote: %w", err)
	}
	result, err := s.quoteProvider.CreatePayoutQuote(ctx, request)
	if err != nil {
		return Quote{}, err
	}
	if result.ProviderQuoteID == "" || result.SourceCurrency == "" || result.DestinationCurrency == "" || result.SenderAmountMinor <= 0 || result.ReceiverAmountMinor <= 0 || !result.ExpiresAt.After(s.now()) {
		return Quote{}, errors.New("provider returned an invalid payout quote")
	}
	id, err := s.newID("pqi_")
	if err != nil {
		return Quote{}, err
	}
	payload := result.Payload
	if len(payload) == 0 {
		payload, _ = json.Marshal(result)
	}
	executionContext := result.ExecutionContext
	if len(executionContext) == 0 {
		executionContext = json.RawMessage(`{}`)
	}
	now := s.now()
	_, err = s.db.ExecContext(ctx, `INSERT INTO payment_quotes(id,direction,provider,provider_quote_id,tenant_id,idempotency_key,source_resource_id,destination_resource_id,payment_method,source_currency,destination_currency,currency_type,cover_fees,request_amount_minor,sender_amount_minor,receiver_amount_minor,commercial_rate,provider_rate,flat_fee_minor,partner_fee_minor,billing_fee_minor,status,expires_at,provider_payload,provider_execution_context,created_at,updated_at) VALUES($1,'payout',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,'open',$21,$22,$23,$24,$24)`, id, s.quoteProvider.Name(), result.ProviderQuoteID, request.TenantID, request.IdempotencyKey, request.SourceAccountID, request.DestinationInstrumentID, request.FundingMethod, result.SourceCurrency, result.DestinationCurrency, request.CurrencyType, request.CoverFees, request.AmountMinor, result.SenderAmountMinor, result.ReceiverAmountMinor, result.CommercialRate, result.ProviderRate, result.FlatFeeMinor, result.PartnerFeeMinor, result.BillingFeeMinor, result.ExpiresAt, payload, executionContext, now)
	if err != nil {
		return Quote{}, fmt.Errorf("store payout quote: %w", err)
	}
	return Quote{Direction: "payout", ID: id, Provider: s.quoteProvider.Name(), ProviderQuoteID: result.ProviderQuoteID, Status: "open", SourceCurrency: result.SourceCurrency, DestinationCurrency: result.DestinationCurrency, SenderAmountMinor: result.SenderAmountMinor, ReceiverAmountMinor: result.ReceiverAmountMinor, CommercialRate: result.CommercialRate, ProviderRate: result.ProviderRate, FlatFeeMinor: result.FlatFeeMinor, PartnerFeeMinor: result.PartnerFeeMinor, BillingFeeMinor: result.BillingFeeMinor, ExpiresAt: result.ExpiresAt, Payload: payload}, nil
}
