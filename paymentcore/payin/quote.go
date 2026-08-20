package payin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

func (s *Service) CreateQuote(ctx context.Context, request QuoteRequest) (Quote, error) {
	if err := request.Validate(); err != nil {
		return Quote{}, err
	}
	var existing Quote
	var existingCurrencyType string
	var existingCoverFees bool
	var existingRequestAmount int64
	err := s.db.QueryRowContext(ctx, `SELECT id,provider,tenant_id,payment_method,COALESCE(source_resource_id,''),destination_resource_id,source_currency,destination_currency,sender_amount_minor,receiver_amount_minor,status,expires_at,created_at,updated_at,currency_type,cover_fees,request_amount_minor FROM payment_quotes WHERE direction='payin' AND tenant_id=$1 AND idempotency_key=$2`, request.TenantID, request.IdempotencyKey).Scan(&existing.ID, &existing.Provider, &existing.TenantID, &existing.FundingMethod, &existing.SourceInstrumentID, &existing.DestinationAccountID, &existing.SourceCurrency, &existing.DestinationCurrency, &existing.SenderAmountMinor, &existing.ReceiverAmountMinor, &existing.Status, &existing.ExpiresAt, &existing.CreatedAt, &existing.UpdatedAt, &existingCurrencyType, &existingCoverFees, &existingRequestAmount)
	if err == nil {
		existing.Direction = "payin"
		if existing.FundingMethod != request.FundingMethod || existing.SourceInstrumentID != request.SourceInstrumentID || existing.DestinationAccountID != request.DestinationAccountID || existingCurrencyType != request.CurrencyType || existingCoverFees != request.CoverFees || existingRequestAmount != request.AmountMinor || existing.SourceCurrency != request.SourceCurrency || existing.DestinationCurrency != request.DestinationCurrency {
			return Quote{}, ErrIdempotencyConflict
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Quote{}, fmt.Errorf("lookup payin quote: %w", err)
	}
	result, err := s.quoteProvider.CreatePayinQuote(ctx, request)
	if err != nil {
		return Quote{}, err
	}
	id, err := s.newID("pqi_")
	if err != nil {
		return Quote{}, err
	}
	now := s.now()
	payload := result.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	executionContext := result.ExecutionContext
	if len(executionContext) == 0 {
		executionContext = json.RawMessage(`{}`)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO payment_quotes(id,direction,provider,provider_quote_id,tenant_id,idempotency_key,payment_method,currency_type,cover_fees,request_amount_minor,source_resource_id,destination_resource_id,source_currency,destination_currency,sender_amount_minor,receiver_amount_minor,status,expires_at,provider_payload,provider_execution_context,created_at,updated_at) VALUES($1,'payin',$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,''),$11,$12,$13,$14,$15,'open',$16,$17,$18,$19,$19)`, id, s.quoteProvider.Name(), result.ProviderQuoteID, request.TenantID, request.IdempotencyKey, request.FundingMethod, request.CurrencyType, request.CoverFees, request.AmountMinor, request.SourceInstrumentID, request.DestinationAccountID, result.SourceCurrency, result.DestinationCurrency, result.SenderAmountMinor, result.ReceiverAmountMinor, result.ExpiresAt, payload, executionContext, now)
	if err != nil {
		return Quote{}, fmt.Errorf("store payin quote: %w", err)
	}
	return Quote{Direction: "payin", ID: id, Provider: s.quoteProvider.Name(), TenantID: request.TenantID, FundingMethod: request.FundingMethod, SourceInstrumentID: request.SourceInstrumentID, DestinationAccountID: request.DestinationAccountID, SourceCurrency: result.SourceCurrency, DestinationCurrency: result.DestinationCurrency, SenderAmountMinor: result.SenderAmountMinor, ReceiverAmountMinor: result.ReceiverAmountMinor, Status: "open", ExpiresAt: result.ExpiresAt, CreatedAt: now, UpdatedAt: now}, nil
}
