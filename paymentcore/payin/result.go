package payin

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"stablerail/eventbus"
	"stablerail/paymentcore"
)

func (s *Service) recordError(ctx context.Context, payinID, status string, cause error) error {
	_, err := s.db.ExecContext(ctx, `UPDATE payins SET status=$1,failure_reason=$2,updated_at=$3 WHERE id=$4 AND status='submission_pending'`, status, cause.Error(), s.now(), payinID)
	return err
}

func normalizeResultStatus(status paymentcore.ExecutionStatus) PayinStatus {
	switch status {
	case paymentcore.ExecutionPending:
		return StatusProcessing
	case paymentcore.ExecutionOnHold:
		return StatusOnHold
	case paymentcore.ExecutionSucceeded:
		return StatusReceived
	case paymentcore.ExecutionFailed:
		return StatusFailed
	default:
		return StatusUnknown
	}
}

func executionStatusFromPayin(status PayinStatus) paymentcore.ExecutionStatus {
	switch status {
	case StatusOnHold:
		return paymentcore.ExecutionOnHold
	case StatusReceived, StatusSucceeded:
		return paymentcore.ExecutionSucceeded
	case StatusFailed:
		return paymentcore.ExecutionFailed
	case StatusRefunded:
		return paymentcore.ExecutionFailed
	default:
		return paymentcore.ExecutionPending
	}
}

func paymentStateForResult(status PayinStatus, currentFunds paymentcore.FundsStatus) (paymentcore.PaymentStatus, paymentcore.FundsStatus) {
	switch status {
	case StatusReceived:
		return paymentcore.PaymentStatusProcessing, paymentcore.FundsStatusReceived
	case StatusFailed:
		return paymentcore.PaymentStatusFailed, currentFunds
	case StatusRefunded:
		return paymentcore.PaymentStatusFailed, paymentcore.FundsStatusReturned
	default:
		return paymentcore.PaymentStatusProcessing, currentFunds
	}
}

// ApplyResult records a provider result and its accounting effects in
// the inbox transaction that consumed the execution command.
func (s *Service) ApplyResult(ctx context.Context, tx *sql.Tx, payinID, correlationID string, result ExecuteResult, now time.Time) error {
	payload, instructions := result.Payload, result.Instructions
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if len(instructions) == 0 {
		instructions = json.RawMessage(`{}`)
	}
	var paymentID string
	var currentFunds paymentcore.FundsStatus
	if err := tx.QueryRowContext(ctx, `SELECT p.payment_id,pm.funds_status FROM payins p JOIN payments pm ON pm.id=p.payment_id WHERE p.id=$1 FOR UPDATE OF p,pm`, payinID).Scan(&paymentID, &currentFunds); err != nil {
		return err
	}
	lifecycleStatus := normalizeResultStatus(result.Status)
	failureReason := result.FailureMessage
	if failureReason == "" {
		failureReason = result.FailureCode
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payins SET provider_payin_id=NULLIF($1,''),status=$2,instructions=$3,provider_payload=$4,failure_reason=NULLIF($5,''),updated_at=$6 WHERE id=$7`, result.ProviderReference, lifecycleStatus, instructions, payload, failureReason, now, payinID); err != nil {
		return err
	}
	paymentStatus, fundsStatus := paymentStateForResult(lifecycleStatus, currentFunds)
	eventType, eventID := "payin."+string(lifecycleStatus), "evt_"+payinID+"_"+string(lifecycleStatus)
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status=$1,funds_status=$2,updated_at=$3 WHERE id=$4`, paymentStatus, fundsStatus, now, paymentID); err != nil {
		return err
	}
	if err := paymentcore.NewHistoryService().RecordTimeline(ctx, tx, paymentcore.TimelineRecord{PaymentID: paymentID, PaymentStatus: paymentStatus, Note: eventType, At: now}); err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]any{"id": eventID, "type": eventType, "payin_id": payinID, "correlation_id": correlationID, "reason": failureReason, "occurred_at": now, "data": map[string]any{"status": result.Status}})
	version := map[PayinStatus]int{StatusProcessing: eventbus.PayinProcessingVersion, StatusOnHold: eventbus.PayinOnHoldVersion, StatusReceived: eventbus.PayinReceivedVersion, StatusFailed: eventbus.PayinFailedVersion, StatusRefunded: eventbus.PayinRefundedVersion}[lifecycleStatus]
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payin',$6,$7) ON CONFLICT(id) DO NOTHING`, eventID, eventbus.PayinEventsTopic, eventType, version, paymentID, body, now); err != nil {
		return err
	}
	publicType := map[PayinStatus]string{StatusProcessing: "payment.processing", StatusOnHold: "payment.processing", StatusReceived: "payment.funds_status_changed", StatusFailed: "payment.failed", StatusRefunded: "payment.failed"}[lifecycleStatus]
	publicVersion := map[string]int{"payment.processing": eventbus.PaymentProcessingVersion, "payment.failed": eventbus.PaymentFailedVersion, "payment.funds_status_changed": eventbus.PaymentFundsStatusChangedVersion}[publicType]
	if err := enqueuePublicPaymentEvent(ctx, tx, eventID+"_payment", paymentID, publicType, publicVersion, body, now); err != nil {
		return err
	}
	if lifecycleStatus == StatusRefunded {
		return enqueuePublicPaymentEvent(ctx, tx, eventID+"_funds", paymentID, "payment.funds_status_changed", eventbus.PaymentFundsStatusChangedVersion, body, now)
	}
	return nil
}
