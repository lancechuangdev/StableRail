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
	"time"

	"stablerail/consumer"
	"stablerail/eventbus"
	"stablerail/ledger"
	"stablerail/paymentcore"
	"stablerail/policy"
	"stablerail/saga"
	"stablerail/settlement"
)

type CommandHandler struct {
	now                func() time.Time
	newID              func() (string, error)
	policyEvaluator    policy.PolicyEvaluator
	ledgerService      ledger.LedgerService
	settlementProvider settlement.SettlementProvider
}

func NewCommandHandler(evaluator policy.PolicyEvaluator, ledgerService ledger.LedgerService, provider settlement.SettlementProvider) *CommandHandler {
	h := &CommandHandler{policyEvaluator: evaluator, ledgerService: ledgerService, settlementProvider: provider, now: func() time.Time { return time.Now().UTC() }, newID: func() (string, error) {
		b := make([]byte, 16)
		_, err := rand.Read(b)
		return "evt_" + hex.EncodeToString(b), err
	}}
	return h
}

type commandPayload struct {
	CorrelationID string `json:"correlation_id"`
	PaymentID     string `json:"payment_id"`
	Reason        string `json:"reason"`
}

func (h *CommandHandler) Handle(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
	if h.policyEvaluator == nil || h.ledgerService == nil || h.settlementProvider == nil {
		return errors.New("command handler dependencies are required")
	}
	var payload commandPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return consumer.Permanent(errors.New("invalid command payload"))
	}
	if payload.PaymentID == "" {
		payload.PaymentID = event.AggregateID
	}
	if payload.CorrelationID == "" {
		return consumer.Permanent(errors.New("invalid command payload"))
	}
	var reply string
	switch event.Type {
	case "policy.evaluate":
		amount, currency, err := loadPaymentAmount(ctx, tx, payload.PaymentID)
		if err != nil {
			return err
		}
		decision, err := h.policyEvaluator.Evaluate(ctx, policy.PolicyRequest{PaymentID: payload.PaymentID, AmountMinor: amount, Currency: currency})
		if err != nil {
			return fmt.Errorf("evaluate payment policy: %w", err)
		}
		if !decision.Approved {
			payload.Reason, reply = decision.Reason, "policy.rejected"
		} else {
			reply = "policy.approved"
		}
	case "ledger.reserve":
		if err := h.ledgerService.Reserve(ctx, tx, ledger.ReservationRequest{PaymentID: payload.PaymentID, At: h.now()}); err != nil {
			if errors.Is(err, ledger.ErrInvalidPaymentStatus) {
				return consumer.Permanent(err)
			}
			return fmt.Errorf("reserve ledger funds: %w", err)
		}
		reply = "ledger.reserved"
	case "settlement.execute":
		amount, currency, err := loadPaymentAmount(ctx, tx, payload.PaymentID)
		if err != nil {
			return err
		}
		destination, err := loadDestination(ctx, tx, payload.PaymentID)
		if err != nil {
			return err
		}
		result, err := h.settlementProvider.ExecutePayout(ctx, settlement.PayoutRequest{IdempotencyKey: event.ID, PaymentID: payload.PaymentID, AmountMinor: amount, Currency: currency, Destination: destination})
		if err != nil {
			var providerErr *settlement.ProviderError
			if errors.As(err, &providerErr) && !providerErr.Retryable {
				if providerErr.Code == "submission_failed" {
					payload.Reason = providerErr.Code
					return h.enqueueReply(ctx, tx, event, payload, "settlement.failed")
				}
				return consumer.Permanent(err)
			}
			return fmt.Errorf("submit settlement: %w", err)
		}
		now := h.now()
		_, err = tx.ExecContext(ctx, `INSERT INTO settlement_submissions
			(payment_id,command_event_id,provider,provider_reference,status,failure_code,failure_message,created_at,updated_at)
			VALUES($1,$2,$3,$4,$5,NULLIF($6,''),NULLIF($7,''),$8,$8)
			ON CONFLICT(command_event_id) DO UPDATE SET status=EXCLUDED.status,failure_code=EXCLUDED.failure_code,failure_message=EXCLUDED.failure_message,updated_at=EXCLUDED.updated_at`,
			payload.PaymentID, event.ID, h.settlementProvider.Name(), result.ProviderReference, result.Status, result.FailureCode, result.FailureMessage, now)
		if err != nil {
			return fmt.Errorf("persist settlement submission: %w", err)
		}
		if result.Status == settlement.StatusPending {
			return nil
		}
		if result.Status == settlement.StatusOnHold {
			return h.enqueueReply(ctx, tx, event, payload, "settlement.on_hold")
		}
		if result.Status == settlement.StatusFailed {
			payload.Reason = result.FailureMessage
			if payload.Reason == "" {
				payload.Reason = result.FailureCode
			}
			if result.FailureCode == "refunded" {
				return h.enqueueReply(ctx, tx, event, payload, "settlement.returned")
			}
			return h.enqueueReply(ctx, tx, event, payload, "settlement.failed")
		}
		if err := transitionPayment(ctx, tx, payload.PaymentID, paymentcore.PaymentStatusProcessing, paymentcore.PaymentStatusSucceeded, h.now()); err != nil {
			return err
		}
		reply = "settlement.completed"
	case "ledger.release":
		if err := h.ledgerService.Release(ctx, tx, ledger.ReleaseRequest{PaymentID: payload.PaymentID, At: h.now()}); err != nil {
			if errors.Is(err, ledger.ErrInvalidPaymentStatus) {
				return consumer.Permanent(err)
			}
			return fmt.Errorf("release ledger funds: %w", err)
		}
		reply = "ledger.released"
	case "payment.settle":
		if err := transitionPayment(ctx, tx, payload.PaymentID, paymentcore.PaymentStatusProcessing, paymentcore.PaymentStatusSucceeded, h.now()); err != nil {
			return err
		}
		return h.enqueueReply(ctx, tx, event, payload, "payment.succeeded")
	case "payment.fail", "payment.fail_reserved", "payment.return":
		status, fundsStatus, eventName, message := paymentcore.PaymentStatusFailed, paymentcore.FundsStatusAvailable, "failed", payload.Reason
		if event.Type == "payment.fail_reserved" {
			fundsStatus = paymentcore.FundsStatusReserved
		} else if event.Type == "payment.return" {
			fundsStatus, eventName = paymentcore.FundsStatusReturned, "returned"
			if message == "" {
				message = "provider returned payout funds"
			}
		}
		now := h.now()
		update := `UPDATE payments SET payment_status=$1, funds_status=$2, updated_at=$3 WHERE id=$4 AND payment_status NOT IN ('succeeded','failed')`
		if _, err := tx.ExecContext(ctx, update, status, fundsStatus, now, payload.PaymentID); err != nil {
			return fmt.Errorf("record payment outcome: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO payment_audit_events(payment_id,event,message,occurred_at) VALUES($1,$2,$3,$4)`, payload.PaymentID, eventName, message, now); err != nil {
			return fmt.Errorf("audit payment outcome: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,payment_status,note,occurred_at) VALUES($1,$2,$3,$4)`, payload.PaymentID, status, message, now); err != nil {
			return fmt.Errorf("timeline payment outcome: %w", err)
		}
		reply := "payment.failed"
		if event.Type == "payment.return" {
			reply = "payment.funds_returned"
		}
		return h.enqueueReply(ctx, tx, event, payload, reply)
	default:
		return consumer.Permanent(fmt.Errorf("unsupported command %q", event.Type))
	}
	return h.enqueueReply(ctx, tx, event, payload, reply)
}

func loadPaymentAmount(ctx context.Context, tx *sql.Tx, paymentID string) (int64, string, error) {
	var amount int64
	var currency string
	if err := tx.QueryRowContext(ctx, `SELECT amount_minor, currency FROM payments WHERE id=$1`, paymentID).Scan(&amount, &currency); err != nil {
		return 0, "", fmt.Errorf("load payment: %w", err)
	}
	return amount, currency, nil
}

func loadDestination(ctx context.Context, tx *sql.Tx, paymentID string) (*settlement.Destination, error) {
	var d settlement.Destination
	err := tx.QueryRowContext(ctx, `SELECT kind,COALESCE(chain,''),COALESCE(address,'') FROM payment_destinations WHERE payment_id=$1`, paymentID).Scan(&d.Type, &d.Chain, &d.Address)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load payment destination: %w", err)
	}
	return &d, nil
}

func transitionPayment(ctx context.Context, tx *sql.Tx, id string, from, to paymentcore.PaymentStatus, now time.Time) error {
	var amount int64
	var currency string
	var status paymentcore.PaymentStatus
	if err := tx.QueryRowContext(ctx, `SELECT payment_status, amount_minor, currency FROM payments WHERE id=$1 FOR UPDATE`, id).Scan(&status, &amount, &currency); err != nil {
		return fmt.Errorf("lock payment: %w", err)
	}
	if status == to {
		return nil
	}
	if status != from {
		return consumer.Permanent(fmt.Errorf("payment %s cannot transition from %s", id, status))
	}
	debit, credit := paymentcore.CashOperatingAccount, paymentcore.SettlementAccount
	if to == paymentcore.PaymentStatusSucceeded {
		debit, credit = credit, debit
	}
	journal := "jrn_" + eventSafeID(id, string(to))
	fundsStatus := paymentcore.FundsStatusReserved
	if to == paymentcore.PaymentStatusSucceeded {
		fundsStatus = paymentcore.FundsStatusConsumed
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status=$1, funds_status=$2, updated_at=$3 WHERE id=$4`, to, fundsStatus, now, id); err != nil {
		return fmt.Errorf("update payment: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions(id,payment_id,event_type,occurred_at) VALUES($1,$2,$3,$4) ON CONFLICT(payment_id,event_type) DO NOTHING`, journal, id, "payment."+string(to), now); err != nil {
		return fmt.Errorf("insert journal: %w", err)
	}
	for _, line := range []struct{ suffix, account, side string }{{"debit", debit, "debit"}, {"credit", credit, "credit"}} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_code,side,amount_minor,currency) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`, journal+":"+line.suffix, journal, line.account, line.side, amount, currency); err != nil {
			return fmt.Errorf("insert ledger entry: %w", err)
		}
	}
	message, note := "payment processing started", "payment processing"
	if to == paymentcore.PaymentStatusSucceeded {
		message, note = "payment succeeded", "payment succeeded"
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_audit_events(payment_id,event,message,occurred_at) VALUES($1,$2,$3,$4)`, id, string(to), message, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,payment_status,note,occurred_at) VALUES($1,$2,$3,$4)`, id, to, note, now); err != nil {
		return err
	}
	return nil
}
func eventSafeID(id, state string) string { return id + "_" + state }

func (h *CommandHandler) enqueueReply(ctx context.Context, tx *sql.Tx, caused eventbus.Event, p commandPayload, reply string) error {
	id, err := h.newID()
	if err != nil {
		return err
	}
	bodyFields := map[string]string{"correlation_id": p.CorrelationID, "caused_by_event_id": caused.ID, "reason": p.Reason}
	switch reply {
	case "payment.succeeded":
		bodyFields["payment_status"], bodyFields["funds_status"] = "succeeded", "consumed"
	case "payment.failed":
		bodyFields["payment_status"] = "failed"
		if caused.Type == "payment.fail_reserved" {
			bodyFields["funds_status"] = "reserved"
		} else {
			bodyFields["funds_status"] = "available"
		}
	case "payment.funds_returned":
		bodyFields["payment_status"], bodyFields["funds_status"] = "failed", "returned"
	}
	body, _ := json.Marshal(bodyFields)
	version := map[string]int{"policy.approved": eventbus.PolicyApprovedVersion, "policy.rejected": eventbus.PolicyRejectedVersion, "ledger.reserved": eventbus.LedgerReservedVersion, "ledger.released": eventbus.LedgerReleasedVersion, "settlement.completed": eventbus.SettlementCompletedVersion, "settlement.failed": eventbus.SettlementFailedVersion, "settlement.returned": eventbus.SettlementReturnedVersion, "settlement.on_hold": eventbus.SettlementOnHoldVersion, "payment.succeeded": eventbus.PaymentSucceededVersion, "payment.failed": eventbus.PaymentFailedVersion, "payment.funds_returned": eventbus.PaymentFundsReturnedVersion}[reply]
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7)`, id, paymentcore.PaymentEventsTopic, reply, version, p.PaymentID, body, h.now())
	if err != nil {
		return fmt.Errorf("enqueue %s: %w", reply, err)
	}
	return nil
}

func SagaHandler(coordinator *saga.Coordinator) func(context.Context, *sql.Tx, eventbus.Event) error {
	allowed := map[string]bool{"payment.created": true, "payment.succeeded": true, "policy.approved": true, "policy.rejected": true, "ledger.reserved": true, "ledger.failed": true, "ledger.released": true, "settlement.completed": true, "settlement.failed": true, "settlement.returned": true, "settlement.on_hold": true}
	return func(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
		if !allowed[event.Type] {
			return nil
		}
		return coordinator.Handle(ctx, tx, event)
	}
}
