package payin

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"stablerail/eventbus"
	"stablerail/saga"
)

type SagaCoordinator struct {
	db    *sql.DB
	now   func() time.Time
	newID func(string) (string, error)
}

func NewSagaCoordinator(db *sql.DB) (*SagaCoordinator, error) {
	if db == nil {
		return nil, errors.New("payin saga database is required")
	}
	return &SagaCoordinator{db: db, now: func() time.Time { return time.Now().UTC() }, newID: func(prefix string) (string, error) {
		b := make([]byte, 16)
		_, err := rand.Read(b)
		return prefix + hex.EncodeToString(b), err
	}}, nil
}

func (c *SagaCoordinator) Handle(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
	if event.AggregateType != "payment" {
		return fmt.Errorf("unsupported payin saga aggregate %q", event.AggregateType)
	}
	if event.Type == "payin.created" {
		return c.start(ctx, tx, event)
	}
	var id, correlation, state string
	if err := tx.QueryRowContext(ctx, `SELECT id,correlation_id,state FROM settlement_sagas WHERE payment_id=$1 AND direction='payin' FOR UPDATE`, event.AggregateID).Scan(&id, &correlation, &state); err != nil {
		return err
	}
	var payload struct {
		CorrelationID string `json:"correlation_id"`
		Reason        string `json:"reason"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}
	if payload.CorrelationID != correlation {
		return fmt.Errorf("payin saga correlation mismatch")
	}
	next, command := "", ""
	switch {
	case state == "awaiting_policy" && event.Type == "payin.policy.approved":
		next, command = "awaiting_execution", "payin.execute"
	case event.Type == "payin.processing" && (state == "awaiting_execution" || state == "processing" || state == "on_hold"):
		next = "processing"
	case event.Type == "payin.on_hold" && (state == "awaiting_execution" || state == "processing"):
		next = "on_hold"
	case event.Type == "payin.received" && (state == "awaiting_execution" || state == "processing" || state == "on_hold"):
		next, command = "awaiting_ledger", "payin.ledger.record"
	case state == "awaiting_ledger" && event.Type == "payin.succeeded":
		next = "completed"
	case event.Type == "payin.failed" && state != "completed" && state != "refunded":
		next = "failed"
	case event.Type == "payin.refunded" && state != "failed" && state != "refunded":
		next = "refunded"
	case state == "completed" || state == "failed" || state == "refunded":
		return nil
	default:
		return fmt.Errorf("event %s is invalid while payin saga is %s", event.Type, state)
	}
	now := c.now()
	if _, err := tx.ExecContext(ctx, `UPDATE settlement_sagas SET state=$1,failure_reason=COALESCE(NULLIF($2,''),failure_reason),updated_at=$3 WHERE id=$4`, next, payload.Reason, now, id); err != nil {
		return err
	}
	if command == "" {
		return nil
	}
	return c.enqueueCommand(ctx, tx, id, correlation, event.AggregateID, event.ID, command, now)
}

func (c *SagaCoordinator) start(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
	id, err := c.newID("psaga_")
	if err != nil {
		return err
	}
	correlation, err := c.newID("corr_")
	if err != nil {
		return err
	}
	now := c.now()
	res, err := tx.ExecContext(ctx, `INSERT INTO settlement_sagas(id,payment_id,direction,correlation_id,state,created_at,updated_at) VALUES($1,$2,'payin',$3,'awaiting_policy',$4,$4) ON CONFLICT(payment_id) DO NOTHING`, id, event.AggregateID, correlation, now)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return nil
	}
	return c.enqueueCommand(ctx, tx, id, correlation, event.AggregateID, event.ID, "payin.policy.evaluate", now)
}

func (c *SagaCoordinator) enqueueCommand(ctx context.Context, tx *sql.Tx, sagaID, correlationID, payinID, causedBy, command string, now time.Time) error {
	commandID, err := c.newID("evt_")
	if err != nil {
		return err
	}
	var operationID string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM payins WHERE payment_id=$1`, payinID).Scan(&operationID); err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]string{"saga_id": sagaID, "correlation_id": correlationID, "payment_id": payinID, "payin_id": operationID, "caused_by_event_id": causedBy})
	version := map[string]int{"payin.policy.evaluate": eventbus.PayinPolicyEvaluateVersion, "payin.execute": eventbus.PayinExecuteVersion, "payin.ledger.record": eventbus.PayinLedgerRecordVersion}[command]
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7)`, commandID, saga.CommandTopic, command, version, payinID, body, now)
	return err
}
