// Package workers implements the in-process payment workflow workers.
package workers

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"stablerail/consumer"
	"stablerail/eventbus"
	"stablerail/ledger"
	"stablerail/paymentcore"
	"stablerail/paymentcore/payin"
	"stablerail/paymentcore/payout"
	"stablerail/policy"
)

type CommandHandler struct {
	now             func() time.Time
	newID           func() (string, error)
	policyEvaluator policy.PolicyEvaluator
	ledgerService   ledger.LedgerService
	payoutService   payoutCommandService
	payinService    payinCommandService
}

type payoutCommandService interface {
	ExecutePayout(context.Context, string) (paymentcore.ExecutionResult, error)
	ApplyResult(context.Context, *sql.Tx, string, string, paymentcore.ExecutionResult, time.Time) error
}

type payinCommandService interface {
	ExecutePayin(context.Context, string) (payin.ExecuteResult, error)
	ApplyResult(context.Context, *sql.Tx, string, payin.ExecuteResult, time.Time) error
}

func NewCommandHandler(evaluator policy.PolicyEvaluator, ledgerService ledger.LedgerService, payouts payoutCommandService, payins payinCommandService) *CommandHandler {
	h := &CommandHandler{policyEvaluator: evaluator, ledgerService: ledgerService, payoutService: payouts, payinService: payins, now: func() time.Time { return time.Now().UTC() }, newID: func() (string, error) {
		b := make([]byte, 16)
		_, err := rand.Read(b)
		return "evt_" + hex.EncodeToString(b), err
	}}
	return h
}

type commandPayload struct {
	PaymentID string `json:"payment_id"`
	PayinID   string `json:"payin_id"`
	Reason    string `json:"reason"`
}

func (h *CommandHandler) Handle(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
	if h.policyEvaluator == nil || h.ledgerService == nil || h.payoutService == nil || h.payinService == nil {
		return errors.New("command handler dependencies are required")
	}
	var payload commandPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return consumer.Permanent(errors.New("invalid command payload"))
	}
	if payload.PaymentID == "" {
		payload.PaymentID = event.AggregateID
	}
	var reply string
	switch event.Type {
	case "payin.policy.evaluate":
		if payload.PayinID == "" {
			payload.PayinID = event.AggregateID
		}
		var amount int64
		var currency string
		if err := tx.QueryRowContext(ctx, `SELECT source_amount_minor,source_currency FROM payins WHERE payment_id=$1`, payload.PayinID).Scan(&amount, &currency); err != nil {
			return fmt.Errorf("load payin for policy: %w", err)
		}
		decision, err := h.policyEvaluator.Evaluate(ctx, policy.PolicyRequest{OperationID: payload.PayinID, Direction: "payin", AmountMinor: amount, Currency: currency})
		if err != nil {
			return fmt.Errorf("evaluate payin policy: %w", err)
		}
		if !decision.Approved {
			return h.payinService.ApplyResult(ctx, tx, payload.PayinID, payin.ExecuteResult{ExecutionResult: paymentcore.ExecutionResult{Status: paymentcore.ExecutionFailed, FailureMessage: decision.Reason}}, h.now())
		}
		return h.enqueuePayinReply(ctx, tx, event, payload.PayinID, "payin.policy.approved", "")
	case "payin.execute":
		if payload.PayinID == "" {
			payload.PayinID = event.AggregateID
		}
		result, err := h.payinService.ExecutePayin(ctx, payload.PayinID)
		if err != nil {
			var providerErr *paymentcore.ProviderError
			if errors.As(err, &providerErr) && !providerErr.Retryable {
				return h.payinService.ApplyResult(ctx, tx, payload.PayinID, payin.ExecuteResult{ExecutionResult: paymentcore.ExecutionResult{Status: paymentcore.ExecutionFailed, FailureCode: providerErr.Code, FailureMessage: providerErr.Message}}, h.now())
			}
			return fmt.Errorf("execute payin: %w", err)
		}
		return h.payinService.ApplyResult(ctx, tx, payload.PayinID, result, h.now())
	case "payin.ledger.record":
		if payload.PayinID == "" {
			payload.PayinID = event.AggregateID
		}
		return h.ledgerService.RecordPayin(ctx, tx, ledger.PayinReceiptRequest{PayinID: payload.PayinID, At: h.now()})
	case "payin.fail":
		if payload.PayinID == "" {
			return consumer.Permanent(errors.New("payin ID is required"))
		}
		return h.payinService.ApplyResult(ctx, tx, payload.PayinID, payin.ExecuteResult{ExecutionResult: paymentcore.ExecutionResult{Status: paymentcore.ExecutionFailed, FailureMessage: payload.Reason}}, h.now())
	case "policy.evaluate":
		amount, currency, err := loadPaymentAmount(ctx, tx, payload.PaymentID)
		if err != nil {
			return err
		}
		decision, err := h.policyEvaluator.Evaluate(ctx, policy.PolicyRequest{OperationID: payload.PaymentID, Direction: "payout", AmountMinor: amount, Currency: currency})
		if err != nil {
			return fmt.Errorf("evaluate payment policy: %w", err)
		}
		if !decision.Approved {
			payload.Reason, reply = decision.Reason, "payout.policy.rejected"
		} else {
			reply = "payout.policy.approved"
		}
	case "ledger.reserve":
		if err := h.ledgerService.Reserve(ctx, tx, ledger.ReservationRequest{PaymentID: payload.PaymentID, At: h.now()}); err != nil {
			if errors.Is(err, ledger.ErrInvalidPaymentStatus) {
				return consumer.Permanent(err)
			}
			return fmt.Errorf("reserve ledger funds: %w", err)
		}
		reply = "payout.funds_reserved"
	case "settlement.execute":
		result, err := h.payoutService.ExecutePayout(ctx, payload.PaymentID)
		if err != nil {
			var providerErr *paymentcore.ProviderError
			if errors.As(err, &providerErr) && !providerErr.Retryable {
				if providerErr.Code == "submission_failed" {
					return h.payoutService.ApplyResult(ctx, tx, payload.PaymentID, event.ID, paymentcore.ExecutionResult{Status: paymentcore.ExecutionFailed, FailureCode: providerErr.Code, FailureMessage: providerErr.Message}, h.now())
				}
				return consumer.Permanent(err)
			}
			return fmt.Errorf("submit settlement: %w", err)
		}
		return h.payoutService.ApplyResult(ctx, tx, payload.PaymentID, event.ID, result, h.now())
	case "ledger.release":
		if err := h.ledgerService.Release(ctx, tx, ledger.ReleaseRequest{PaymentID: payload.PaymentID, At: h.now()}); err != nil {
			if errors.Is(err, ledger.ErrInvalidPaymentStatus) {
				return consumer.Permanent(err)
			}
			return fmt.Errorf("release ledger funds: %w", err)
		}
		reply = "payout.funds_released"
	case "payment.settle":
		if err := h.ledgerService.Settle(ctx, tx, ledger.SettlementRequest{PaymentID: payload.PaymentID, At: h.now()}); err != nil {
			if errors.Is(err, ledger.ErrInvalidPaymentStatus) {
				return consumer.Permanent(err)
			}
			return err
		}
		return h.enqueueReply(ctx, tx, event, payload, "payout.completed")
	case "payment.fail", "payment.fail_reserved", "payment.return":
		status, eventName, message := paymentcore.PaymentStatusFailed, "failed", payload.Reason
		if event.Type == "payment.return" {
			eventName = "returned"
			if message == "" {
				message = "provider returned payout funds"
			}
		}
		now := h.now()
		predicate := "payment_status NOT IN ('succeeded','failed')"
		if event.Type == "payment.return" {
			predicate = "payment_status='failed' AND EXISTS (SELECT 1 FROM ledger_journals WHERE payment_id=payments.id AND event_type='payment.released' AND ledger_status='posted')"
		}
		result, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status=$1, updated_at=$2 WHERE id=$3 AND `+predicate, status, now, payload.PaymentID)
		if err != nil {
			return fmt.Errorf("record payment outcome: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect payment outcome: %w", err)
		}
		if rows != 1 {
			return consumer.Permanent(fmt.Errorf("payment %s is not eligible for %s", payload.PaymentID, event.Type))
		}
		if err := paymentcore.NewHistoryService().Record(ctx, tx,
			paymentcore.AuditRecord{PaymentID: payload.PaymentID, Event: eventName, Message: message, At: now},
			paymentcore.TimelineRecord{PaymentID: payload.PaymentID, PaymentStatus: status, Note: message, At: now}); err != nil {
			return err
		}
		reply := "payout.failed"
		if event.Type == "payment.return" {
			reply = "payout.funds_returned"
		}
		return h.enqueueReply(ctx, tx, event, payload, reply)
	default:
		return consumer.Permanent(fmt.Errorf("unsupported command %q", event.Type))
	}
	return h.enqueueReply(ctx, tx, event, payload, reply)
}

func (h *CommandHandler) enqueuePayinReply(ctx context.Context, tx *sql.Tx, caused eventbus.Event, payinID, eventType, reason string) error {
	id, err := h.newID()
	if err != nil {
		return err
	}
	now := h.now()
	body, _ := json.Marshal(map[string]any{"payment_id": caused.AggregateID, "payin_id": payinID, "reason": reason, "caused_by_event_id": caused.ID})
	version := map[string]int{"payin.policy.approved": eventbus.PayinPolicyApprovedVersion}[eventType]
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payin',$6,$7)`, id, eventbus.PayinEventsTopic, eventType, version, caused.AggregateID, body, now)
	return err
}

func loadPaymentAmount(ctx context.Context, tx *sql.Tx, paymentID string) (int64, string, error) {
	var amount int64
	var currency string
	if err := tx.QueryRowContext(ctx, `SELECT amount_minor, currency FROM payments WHERE id=$1`, paymentID).Scan(&amount, &currency); err != nil {
		return 0, "", fmt.Errorf("load payment: %w", err)
	}
	return amount, currency, nil
}

func (h *CommandHandler) enqueueReply(ctx context.Context, tx *sql.Tx, caused eventbus.Event, p commandPayload, reply string) error {
	id, err := h.newID()
	if err != nil {
		return err
	}
	bodyFields := map[string]string{"caused_by_event_id": caused.ID, "reason": p.Reason}
	switch reply {
	case "payout.completed":
		bodyFields["payment_status"] = "succeeded"
	case "payout.failed":
		bodyFields["payment_status"] = "failed"
	case "payout.funds_returned":
		bodyFields["payment_status"] = "failed"
	}
	body, _ := json.Marshal(bodyFields)
	version := map[string]int{"payout.policy.approved": eventbus.PayoutPolicyApprovedVersion, "payout.policy.rejected": eventbus.PayoutPolicyRejectedVersion, "payout.funds_reserved": eventbus.PayoutFundsReservedVersion, "payout.ledger_failed": eventbus.PayoutLedgerFailedVersion, "payout.funds_released": eventbus.PayoutFundsReleasedVersion, "payout.provider_completed": eventbus.PayoutProviderCompletedVersion, "payout.provider_failed": eventbus.PayoutProviderFailedVersion, "payout.provider_returned": eventbus.PayoutProviderReturnedVersion, "payout.on_hold": eventbus.PayoutOnHoldVersion, "payout.completed": eventbus.PayoutCompletedVersion, "payout.failed": eventbus.PayoutFailedVersion, "payout.funds_returned": eventbus.PayoutFundsReturnedVersion}[reply]
	now := h.now()
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payout',$6,$7)`, id, eventbus.PayoutEventsTopic, reply, version, p.PaymentID, body, now)
	if err != nil {
		return fmt.Errorf("enqueue %s: %w", reply, err)
	}
	publicType := map[string]string{
		"payout.funds_reserved": "payment.processing",
		"payout.completed":      "payment.succeeded",
		"payout.failed":         "payment.failed",
	}[reply]
	if publicType == "" {
		return nil
	}
	publicID, err := h.newID()
	if err != nil {
		return err
	}
	publicVersion := map[string]int{"payment.processing": eventbus.PaymentProcessingVersion, "payment.succeeded": eventbus.PaymentSucceededVersion, "payment.failed": eventbus.PaymentFailedVersion}[publicType]
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7)`, publicID, eventbus.PaymentEventsTopic, publicType, publicVersion, p.PaymentID, body, now); err != nil {
		return fmt.Errorf("enqueue %s: %w", publicType, err)
	}
	return nil
}

func PayoutSagaHandler(coordinator *payout.SagaCoordinator) func(context.Context, *sql.Tx, eventbus.Event) error {
	allowed := map[string]bool{"payout.created": true, "payout.completed": true, "payout.failed": true, "payout.policy.approved": true, "payout.policy.rejected": true, "payout.funds_reserved": true, "payout.ledger_failed": true, "payout.funds_released": true, "payout.provider_completed": true, "payout.provider_failed": true, "payout.provider_returned": true, "payout.on_hold": true}
	return func(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
		if !allowed[event.Type] {
			return nil
		}
		return coordinator.Handle(ctx, tx, event)
	}
}

func PayinSagaHandler(coordinator *payin.SagaCoordinator) func(context.Context, *sql.Tx, eventbus.Event) error {
	return func(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
		if !strings.HasPrefix(event.Type, "payin.") {
			return nil
		}
		return coordinator.Handle(ctx, tx, event)
	}
}
