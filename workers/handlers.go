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
	"stablerail/paymentcore"
	"stablerail/saga"
	"stablerail/settlement"
)

type CommandHandler struct {
	now                func() time.Time
	newID              func() (string, error)
	settlementProvider settlement.Provider
}

func NewCommandHandler(providers ...settlement.Provider) *CommandHandler {
	provider := settlement.Provider(settlement.NewMockProvider(settlement.Result{}))
	if len(providers) > 0 && providers[0] != nil {
		provider = providers[0]
	}
	return &CommandHandler{settlementProvider: provider, now: func() time.Time { return time.Now().UTC() }, newID: func() (string, error) {
		b := make([]byte, 16)
		_, err := rand.Read(b)
		return "evt_" + hex.EncodeToString(b), err
	}}
}

type commandPayload struct {
	CorrelationID string `json:"correlation_id"`
	PaymentID     string `json:"payment_id"`
	Reason        string `json:"reason"`
}

func (h *CommandHandler) Handle(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
	var payload commandPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.CorrelationID == "" {
		return consumer.Permanent(errors.New("invalid command payload"))
	}
	if payload.PaymentID == "" {
		payload.PaymentID = event.AggregateID
	}
	var reply string
	switch event.Type {
	case "policy.evaluate":
		reply = "policy.approved"
	case "ledger.reserve":
		if err := transitionPayment(ctx, tx, payload.PaymentID, paymentcore.StateCreated, paymentcore.StateProcessing, h.now()); err != nil {
			return err
		}
		reply = "ledger.reserved"
	case "settlement.execute":
		var amount int64
		var currency string
		if err := tx.QueryRowContext(ctx, `SELECT amount_minor, currency FROM payments WHERE id=$1`, payload.PaymentID).Scan(&amount, &currency); err != nil {
			return fmt.Errorf("load payment for settlement: %w", err)
		}
		result, err := h.settlementProvider.Submit(ctx, settlement.Request{IdempotencyKey: event.ID, PaymentID: payload.PaymentID, AmountMinor: amount, Currency: currency})
		if err != nil {
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
		if result.Status == settlement.StatusFailed {
			payload.Reason = result.FailureMessage
			if payload.Reason == "" {
				payload.Reason = result.FailureCode
			}
			return h.enqueueReply(ctx, tx, event, payload, "settlement.failed")
		}
		if err := transitionPayment(ctx, tx, payload.PaymentID, paymentcore.StateProcessing, paymentcore.StateSettled, h.now()); err != nil {
			return err
		}
		reply = "settlement.completed"
	case "ledger.release":
		reply = "ledger.released"
	case "payment.fail":
		now := h.now()
		if _, err := tx.ExecContext(ctx, `UPDATE payments SET state='failed', updated_at=$1 WHERE id=$2 AND state <> 'settled'`, now, payload.PaymentID); err != nil {
			return fmt.Errorf("fail payment: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO payment_audit_events(payment_id,event,message,occurred_at) VALUES($1,'failed',$2,$3)`, payload.PaymentID, payload.Reason, now); err != nil {
			return fmt.Errorf("audit failed payment: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,state,note,occurred_at) VALUES($1,'failed',$2,$3)`, payload.PaymentID, payload.Reason, now); err != nil {
			return fmt.Errorf("timeline failed payment: %w", err)
		}
		return nil
	default:
		return consumer.Permanent(fmt.Errorf("unsupported command %q", event.Type))
	}
	return h.enqueueReply(ctx, tx, event, payload, reply)
}

func transitionPayment(ctx context.Context, tx *sql.Tx, id string, from, to paymentcore.PaymentState, now time.Time) error {
	var amount int64
	var currency string
	var state paymentcore.PaymentState
	if err := tx.QueryRowContext(ctx, `SELECT state, amount_minor, currency FROM payments WHERE id=$1 FOR UPDATE`, id).Scan(&state, &amount, &currency); err != nil {
		return fmt.Errorf("lock payment: %w", err)
	}
	if state == to {
		return nil
	}
	if state != from {
		return consumer.Permanent(fmt.Errorf("payment %s cannot transition from %s", id, state))
	}
	debit, credit := paymentcore.CashOperatingAccount, paymentcore.SettlementAccount
	if to == paymentcore.StateSettled {
		debit, credit = credit, debit
	}
	journal := "jrn_" + eventSafeID(id, string(to))
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET state=$1, updated_at=$2 WHERE id=$3`, to, now, id); err != nil {
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
	if to == paymentcore.StateSettled {
		message, note = "payment settled successfully", "payment settled"
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_audit_events(payment_id,event,message,occurred_at) VALUES($1,$2,$3,$4)`, id, string(to), message, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,state,note,occurred_at) VALUES($1,$2,$3,$4)`, id, to, note, now); err != nil {
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
	body, _ := json.Marshal(map[string]string{"correlation_id": p.CorrelationID, "caused_by_event_id": caused.ID, "reason": p.Reason})
	version := map[string]int{"policy.approved": eventbus.PolicyApprovedVersion, "ledger.reserved": eventbus.LedgerReservedVersion, "ledger.released": eventbus.LedgerReleasedVersion, "settlement.completed": eventbus.SettlementCompletedVersion, "settlement.failed": eventbus.SettlementFailedVersion}[reply]
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7)`, id, paymentcore.PaymentEventsTopic, reply, version, p.PaymentID, body, h.now())
	if err != nil {
		return fmt.Errorf("enqueue %s: %w", reply, err)
	}
	return nil
}

func SagaHandler(coordinator *saga.Coordinator) func(context.Context, *sql.Tx, eventbus.Event) error {
	allowed := map[string]bool{"payment.created": true, "policy.approved": true, "policy.rejected": true, "ledger.reserved": true, "ledger.failed": true, "ledger.released": true, "settlement.completed": true, "settlement.failed": true}
	return func(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
		if !allowed[event.Type] {
			return nil
		}
		return coordinator.Handle(ctx, tx, event)
	}
}
