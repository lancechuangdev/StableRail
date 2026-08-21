package payin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"stablerail/paymentcore"
)

// ExecutePayin is called by the Kafka command worker. The durable pay-in row
// exists before the provider call, and retries reuse the original idempotency key.
func (s *Service) ExecutePayin(ctx context.Context, payinID string) (ExecuteResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ExecuteResult{}, err
	}
	defer tx.Rollback()
	var providerQuote, key, status, tenantID, fundingMethod, sourceInstrumentID, destinationAccountID, sourceCurrency, destinationCurrency string
	var sourceAmountMinor, destinationAmountMinor int64
	var providerID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(q.provider_quote_id,''),p.idempotency_key,p.settlement_status,p.provider_payin_id,p.tenant_id,p.funding_method,COALESCE(p.source_instrument_id,''),p.destination_account_id,p.source_amount_minor,p.source_currency,p.destination_amount_minor,p.destination_currency FROM payins p LEFT JOIN payment_quotes q ON q.id=p.quote_id WHERE p.id=$1 AND p.provider=$2 FOR UPDATE`, payinID, s.executionProvider.Name()).Scan(&providerQuote, &key, &status, &providerID, &tenantID, &fundingMethod, &sourceInstrumentID, &destinationAccountID, &sourceAmountMinor, &sourceCurrency, &destinationAmountMinor, &destinationCurrency)
	if errors.Is(err, sql.ErrNoRows) {
		return ExecuteResult{}, ErrNotFound
	}
	if err != nil {
		return ExecuteResult{}, err
	}
	if providerID.Valid && providerID.String != "" {
		var instructions, payload json.RawMessage
		if err := tx.QueryRowContext(ctx, `SELECT instructions,provider_payload FROM payins WHERE id=$1`, payinID).Scan(&instructions, &payload); err != nil {
			return ExecuteResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return ExecuteResult{}, err
		}
		return ExecuteResult{ExecutionResult: paymentcore.ExecutionResult{ProviderReference: providerID.String, Status: executionStatusFromPayin(PayinStatus(status)), Payload: payload}, Instructions: instructions}, nil
	}
	if status != "created" && status != "submission_pending" && status != "unknown" {
		return ExecuteResult{}, fmt.Errorf("payin cannot execute from status %s", status)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payins SET settlement_status='submission_pending',updated_at=$1 WHERE id=$2`, s.now(), payinID); err != nil {
		return ExecuteResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ExecuteResult{}, err
	}
	request := paymentcore.ExecuteRequest{IdempotencyKey: key, ProviderQuoteID: providerQuote}
	if err := request.Validate(); err != nil {
		return ExecuteResult{}, err
	}
	result, err := s.executionProvider.ExecutePayin(ctx, request)
	if err != nil {
		state := "unknown"
		var providerErr *paymentcore.ProviderError
		if errors.As(err, &providerErr) && !providerErr.Retryable {
			state = "failed"
		}
		if updateErr := s.recordError(ctx, payinID, state, err); updateErr != nil {
			return ExecuteResult{}, fmt.Errorf("execute payin: %w (record outcome: %v)", err, updateErr)
		}
		return ExecuteResult{}, err
	}
	if err := result.Validate(); err != nil {
		return ExecuteResult{}, err
	}
	return result, nil
}
