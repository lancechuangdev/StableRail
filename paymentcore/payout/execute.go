package payout

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"stablerail/paymentcore"
)

var ErrSubmissionUnknown = errors.New("payout submission outcome is unknown")

type attempt struct {
	request           paymentcore.ExecuteRequest
	providerReference string
	providerStatus    string
	new               bool
}

type executionRequest struct {
	paymentID      string
	idempotencyKey string
}

// ExecutePayout loads the persisted payment route and durably submits it to the provider.
func (s *Service) ExecutePayout(ctx context.Context, paymentID string) (paymentcore.ExecutionResult, error) {
	request, err := s.loadExecutionRequest(ctx, paymentID)
	if err != nil {
		return paymentcore.ExecutionResult{}, err
	}
	return s.executePayout(ctx, request)
}

func (s *Service) loadExecutionRequest(ctx context.Context, paymentID string) (executionRequest, error) {
	request := executionRequest{paymentID: paymentID}
	err := s.db.QueryRowContext(ctx, `SELECT idempotency_key FROM payments WHERE id=$1 AND direction='payout'`, paymentID).Scan(&request.idempotencyKey)
	if errors.Is(err, sql.ErrNoRows) {
		return executionRequest{}, fmt.Errorf("payout payment %s not found", paymentID)
	}
	if err != nil {
		return executionRequest{}, fmt.Errorf("load payout execution request: %w", err)
	}
	return request, nil
}

func (s *Service) executePayout(ctx context.Context, request executionRequest) (paymentcore.ExecutionResult, error) {
	if request.paymentID == "" || request.idempotencyKey == "" {
		return paymentcore.ExecutionResult{}, errors.New("payout payment ID and idempotency key are required")
	}
	a, err := s.prepare(ctx, request)
	if err != nil {
		return paymentcore.ExecutionResult{}, err
	}
	if !a.new {
		if a.providerReference != "" {
			if err := s.setPaymentFundsStatus(ctx, request.paymentID, "reserved"); err != nil {
				return paymentcore.ExecutionResult{}, err
			}
			return mapPersistedResult(a.providerReference, a.providerStatus)
		}
		if a.providerStatus != "unknown" {
			return paymentcore.ExecutionResult{}, ErrSubmissionUnknown
		}
	}

	result, err := s.executionProvider.ExecutePayout(ctx, a.request)
	if err != nil {
		status := "unknown"
		var providerErr *paymentcore.ProviderError
		if errors.As(err, &providerErr) && !providerErr.Retryable {
			status = "submission_failed"
		}
		if updateErr := s.recordError(ctx, request.paymentID, status, err); updateErr != nil {
			return paymentcore.ExecutionResult{}, fmt.Errorf("execute payout: %w (also failed to record outcome: %v)", err, updateErr)
		}
		if status == "unknown" {
			return paymentcore.ExecutionResult{}, fmt.Errorf("%w: %v", ErrSubmissionUnknown, err)
		}
		return paymentcore.ExecutionResult{}, err
	}
	if err := result.Validate(); err != nil {
		return paymentcore.ExecutionResult{}, err
	}
	payload := result.Payload
	if len(payload) == 0 {
		payload, _ = json.Marshal(result)
	}
	providerStatus := persistedStatus(result)
	now := s.now()
	if _, err := s.db.ExecContext(ctx, `UPDATE payouts SET provider_payout_id=$1,provider_status=$2,provider_payload=$3,last_error=NULL,updated_at=$4,submitted_at=$4 WHERE payment_id=$5 AND provider_status IN ('submission_pending','unknown')`, result.ProviderReference, providerStatus, payload, now, request.paymentID); err != nil {
		return paymentcore.ExecutionResult{}, fmt.Errorf("record payout response: %w", err)
	}
	if err := s.setPaymentFundsStatus(ctx, request.paymentID, "reserved"); err != nil {
		return paymentcore.ExecutionResult{}, err
	}
	return result, nil
}

func (s *Service) prepare(ctx context.Context, request executionRequest) (attempt, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return attempt{}, fmt.Errorf("begin payout submission: %w", err)
	}
	defer tx.Rollback()
	var quoteID, providerQuoteID, tenantID, sourceAccountID, destinationInstrumentID, method, sourceCurrency, destinationCurrency string
	var sourceAmount, destinationAmount int64
	err = tx.QueryRowContext(ctx, `SELECT q.id,q.provider_quote_id,q.tenant_id,q.source_resource_id,q.destination_resource_id,q.payment_method,q.sender_amount_minor,q.source_currency,q.receiver_amount_minor,q.destination_currency FROM payment_quotes q WHERE q.direction='payout' AND q.payment_id=$1 AND q.provider=$2 AND q.status='accepted' FOR UPDATE`, request.paymentID, s.executionProvider.Name()).Scan(&quoteID, &providerQuoteID, &tenantID, &sourceAccountID, &destinationInstrumentID, &method, &sourceAmount, &sourceCurrency, &destinationAmount, &destinationCurrency)
	if errors.Is(err, sql.ErrNoRows) {
		return attempt{}, fmt.Errorf("payout payment %s has no accepted provider-resource quote", request.paymentID)
	}
	if err != nil {
		return attempt{}, fmt.Errorf("load payout route: %w", err)
	}
	now := s.now()
	res, err := tx.ExecContext(ctx, `INSERT INTO payouts(payment_id,quote_id,tenant_id,source_account_id,destination_instrument_id,payout_method,source_amount_minor,source_currency,destination_amount_minor,destination_currency,provider,provider_status,idempotency_key,created_at,updated_at) VALUES($1,NULLIF($2,''),$3,NULLIF($4,''),NULLIF($5,''),$6,$7,$8,$9,$10,$11,'submission_pending',$12,$13,$13) ON CONFLICT(payment_id) DO NOTHING`, request.paymentID, quoteID, tenantID, sourceAccountID, destinationInstrumentID, method, sourceAmount, sourceCurrency, destinationAmount, destinationCurrency, s.executionProvider.Name(), request.idempotencyKey, now)
	if err != nil {
		return attempt{}, fmt.Errorf("create payout submission: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return attempt{}, err
	}
	a := attempt{new: rows == 1}
	if !a.new {
		var storedKey string
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(provider_payout_id,''),provider_status,idempotency_key FROM payouts WHERE payment_id=$1 FOR UPDATE`, request.paymentID).Scan(&a.providerReference, &a.providerStatus, &storedKey)
		if err != nil {
			return attempt{}, fmt.Errorf("get existing payout submission: %w", err)
		}
		if storedKey != request.idempotencyKey {
			return attempt{}, errors.New("payment is already bound to a different payout idempotency key")
		}
	}
	a.request = paymentcore.ExecuteRequest{IdempotencyKey: request.idempotencyKey, ProviderQuoteID: providerQuoteID}
	if err := tx.Commit(); err != nil {
		return attempt{}, fmt.Errorf("commit payout submission: %w", err)
	}
	return a, nil
}

func (s *Service) setPaymentFundsStatus(ctx context.Context, paymentID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE payments SET funds_status=$1,updated_at=$2 WHERE id=$3 AND payment_status='processing'`, status, s.now(), paymentID)
	if err != nil {
		return fmt.Errorf("update payment funds status: %w", err)
	}
	return nil
}
