package payout

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"stablerail/paymentcore"
)

// ApplyResult persists a normalized provider outcome, applies its immediate
// payment effects, and publishes the next saga event in the inbox transaction.
func (s *Service) ApplyResult(ctx context.Context, tx *sql.Tx, paymentID, commandEventID, correlationID string, result ExecutionResult, now time.Time) error {
	if tx == nil {
		return errors.New("payout result transaction is required")
	}
	if paymentID == "" || commandEventID == "" || correlationID == "" {
		return errors.New("payout payment, command event, and correlation IDs are required")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO settlement_submissions
		(payment_id,command_event_id,provider,provider_reference,status,failure_code,failure_message,created_at,updated_at)
		VALUES($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,''),NULLIF($7,''),$8,$8)
		ON CONFLICT(command_event_id) DO UPDATE SET status=EXCLUDED.status,failure_code=EXCLUDED.failure_code,failure_message=EXCLUDED.failure_message,updated_at=EXCLUDED.updated_at`, paymentID, commandEventID, s.executionProvider.Name(), result.ProviderReference, result.Status, result.FailureCode, result.FailureMessage, now)
	if err != nil {
		return fmt.Errorf("persist payout submission: %w", err)
	}
	if result.Status == paymentcore.ExecutionPending {
		return nil
	}
	eventType, reason := "", result.FailureMessage
	switch result.Status {
	case paymentcore.ExecutionOnHold:
		eventType = "payout.on_hold"
	case paymentcore.ExecutionFailed:
		if reason == "" {
			reason = result.FailureCode
		}
		if result.FailureCode == "refunded" {
			eventType = "payout.provider_returned"
		} else {
			eventType = "payout.provider_failed"
		}
	case paymentcore.ExecutionSucceeded:
		if err := s.completePayment(ctx, tx, paymentID, now); err != nil {
			return err
		}
		eventType = "payout.provider_completed"
	default:
		return fmt.Errorf("unsupported payout result status %q", result.Status)
	}
	return enqueueProviderResult(ctx, tx, paymentID, commandEventID, correlationID, eventType, reason, now)
}

func (s *Service) completePayment(ctx context.Context, tx *sql.Tx, paymentID string, now time.Time) error {
	var status paymentcore.PaymentStatus
	var amount int64
	var currency string
	if err := tx.QueryRowContext(ctx, `SELECT payment_status,amount_minor,currency FROM payments WHERE id=$1 FOR UPDATE`, paymentID).Scan(&status, &amount, &currency); err != nil {
		return fmt.Errorf("lock completed payout payment: %w", err)
	}
	if status == paymentcore.PaymentStatusSucceeded {
		return nil
	}
	if status != paymentcore.PaymentStatusProcessing {
		return fmt.Errorf("payment %s cannot complete from %s", paymentID, status)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status='succeeded',funds_status='consumed',updated_at=$1 WHERE id=$2`, now, paymentID); err != nil {
		return fmt.Errorf("complete payout payment: %w", err)
	}
	if err := s.insertJournal(ctx, tx, paymentID, "payment.succeeded", paymentcore.SettlementAccount, paymentcore.CashOperatingAccount, amount, currency, now); err != nil {
		return err
	}
	return insertHistory(ctx, tx, paymentID, "succeeded", "payment succeeded", paymentcore.PaymentStatusSucceeded, "payment succeeded", now)
}

func (s *Service) recordError(ctx context.Context, paymentID, status string, cause error) error {
	_, err := s.db.ExecContext(ctx, `UPDATE payouts SET provider_status=$1,last_error=$2,updated_at=$3 WHERE payment_id=$4 AND provider_status IN ('submission_pending','unknown')`, status, cause.Error(), s.now(), paymentID)
	return err
}

func persistedStatus(result ExecutionResult) string {
	switch result.Status {
	case paymentcore.ExecutionSucceeded:
		return "completed"
	case paymentcore.ExecutionOnHold:
		return "on_hold"
	case paymentcore.ExecutionFailed:
		if strings.TrimSpace(result.FailureCode) != "" {
			return result.FailureCode
		}
		return "failed"
	default:
		return "processing"
	}
}

func mapPersistedResult(reference, status string) (ExecutionResult, error) {
	result := ExecutionResult{ProviderReference: reference}
	switch status {
	case "completed":
		result.Status = paymentcore.ExecutionSucceeded
	case "processing", "submission_pending", "unknown":
		result.Status = paymentcore.ExecutionPending
	case "on_hold":
		result.Status = paymentcore.ExecutionOnHold
	case "failed", "refunded", "submission_failed":
		result.Status, result.FailureCode = paymentcore.ExecutionFailed, status
	default:
		return ExecutionResult{}, fmt.Errorf("unsupported payout status %q", status)
	}
	return result, result.Validate()
}
