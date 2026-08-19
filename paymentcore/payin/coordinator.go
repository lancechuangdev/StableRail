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
)

type SagaCoordinator struct {
	db                *sql.DB
	operationTimeout  time.Duration
	complianceTimeout time.Duration
	timeoutBatchSize  int
	now               func() time.Time
	newID             func(string) (string, error)
}

func NewSagaCoordinator(db *sql.DB) (*SagaCoordinator, error) {
	if db == nil {
		return nil, errors.New("payin saga database is required")
	}
	return &SagaCoordinator{db: db, operationTimeout: 10 * time.Minute, complianceTimeout: 24 * time.Hour, timeoutBatchSize: 100, now: func() time.Time { return time.Now().UTC() }, newID: func(prefix string) (string, error) {
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
	deadline := c.deadline(next, now)
	if _, err := tx.ExecContext(ctx, `UPDATE settlement_sagas SET state=$1,deadline_at=$2,failure_reason=COALESCE(NULLIF($3,''),failure_reason),updated_at=$4 WHERE id=$5`, next, deadline, payload.Reason, now, id); err != nil {
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
	res, err := tx.ExecContext(ctx, `INSERT INTO settlement_sagas(id,payment_id,direction,correlation_id,state,deadline_at,created_at,updated_at) VALUES($1,$2,'payin',$3,'awaiting_policy',$4,$5,$5) ON CONFLICT(payment_id) DO NOTHING`, id, event.AggregateID, correlation, now.Add(c.operationTimeout), now)
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
	reason := ""
	if command == "payin.fail" {
		reason = "payin workflow timed out"
	}
	body, _ := json.Marshal(map[string]string{"saga_id": sagaID, "correlation_id": correlationID, "payment_id": payinID, "payin_id": operationID, "caused_by_event_id": causedBy, "reason": reason})
	version := map[string]int{"payin.policy.evaluate": eventbus.PayinPolicyEvaluateVersion, "payin.execute": eventbus.PayinExecuteVersion, "payin.ledger.record": eventbus.PayinLedgerRecordVersion, "payin.fail": eventbus.PayinFailedVersion}[command]
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7)`, commandID, eventbus.SettlementCommandsTopic, command, version, payinID, body, now)
	return err
}

func (c *SagaCoordinator) deadline(state string, now time.Time) any {
	switch state {
	case "awaiting_policy", "awaiting_execution", "processing", "awaiting_ledger":
		return now.Add(c.operationTimeout)
	case "on_hold":
		return now.Add(c.complianceTimeout)
	default:
		return nil
	}
}

// ExpireOnce retries idempotent provider/ledger work and fails policy or
// compliance waits that cannot make progress. Only pay-in saga rows are claimed.
func (c *SagaCoordinator) ExpireOnce(ctx context.Context) (int, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := c.now()
	rows, err := tx.QueryContext(ctx, `SELECT s.id,s.payment_id,s.correlation_id,s.state,p.id FROM settlement_sagas s JOIN payins p ON p.payment_id=s.payment_id WHERE s.direction='payin' AND s.deadline_at <= $1 AND s.state IN ('awaiting_policy','awaiting_execution','processing','on_hold','awaiting_ledger') ORDER BY s.deadline_at FOR UPDATE OF s SKIP LOCKED LIMIT $2`, now, c.timeoutBatchSize)
	if err != nil {
		return 0, err
	}
	type expired struct{ sagaID, paymentID, correlation, state, payinID string }
	var items []expired
	for rows.Next() {
		var item expired
		if err := rows.Scan(&item.sagaID, &item.paymentID, &item.correlation, &item.state, &item.payinID); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, item)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	for _, item := range items {
		command := "payin.execute"
		if item.state == "awaiting_policy" || item.state == "on_hold" {
			command = "payin.fail"
		}
		if item.state == "awaiting_ledger" {
			command = "payin.ledger.record"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE settlement_sagas SET deadline_at=$1,failure_reason=COALESCE(failure_reason,$2),updated_at=$3 WHERE id=$4`, now.Add(c.operationTimeout), item.state+" timeout", now, item.sagaID); err != nil {
			return 0, err
		}
		if err := c.enqueueCommand(ctx, tx, item.sagaID, item.correlation, item.paymentID, "timeout", command, now); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(items), nil
}
