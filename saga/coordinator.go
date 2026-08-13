// Package saga coordinates the asynchronous payment workflow.
package saga

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

const CommandTopic eventbus.Topic = "payment-commands"

type State string

const (
	StateAwaitingPolicy     State = "awaiting_policy"
	StateAwaitingLedger     State = "awaiting_ledger"
	StateAwaitingSettlement State = "awaiting_settlement"
	StateReleasingLedger    State = "releasing_ledger"
	StateRefunding          State = "refunding"
	StateCompleted          State = "completed"
	StateLedgerReleased     State = "ledger_released"
	StateFailed             State = "failed"
	StateRefunded           State = "refunded"
)

type Config struct {
	PolicyTimeout     time.Duration
	LedgerTimeout     time.Duration
	SettlementTimeout time.Duration
	TimeoutBatchSize  int
}

type Coordinator struct {
	db                *sql.DB
	policyTimeout     time.Duration
	ledgerTimeout     time.Duration
	settlementTimeout time.Duration
	timeoutBatchSize  int
	now               func() time.Time
	newID             func(string) (string, error)
}

func NewCoordinator(db *sql.DB, config Config) (*Coordinator, error) {
	if db == nil {
		return nil, errors.New("saga database is required")
	}
	if config.PolicyTimeout < 0 || config.LedgerTimeout < 0 || config.SettlementTimeout < 0 || config.TimeoutBatchSize < 0 {
		return nil, errors.New("saga configuration cannot be negative")
	}
	if config.PolicyTimeout == 0 {
		config.PolicyTimeout = time.Minute
	}
	if config.LedgerTimeout == 0 {
		config.LedgerTimeout = time.Minute
	}
	if config.SettlementTimeout == 0 {
		config.SettlementTimeout = 10 * time.Minute
	}
	if config.TimeoutBatchSize == 0 {
		config.TimeoutBatchSize = 100
	}
	return &Coordinator{
		db: db, policyTimeout: config.PolicyTimeout, ledgerTimeout: config.LedgerTimeout,
		settlementTimeout: config.SettlementTimeout, timeoutBatchSize: config.TimeoutBatchSize,
		now: func() time.Time { return time.Now().UTC() },
		newID: func(prefix string) (string, error) {
			value := make([]byte, 16)
			if _, err := rand.Read(value); err != nil {
				return "", err
			}
			return prefix + hex.EncodeToString(value), nil
		},
	}, nil
}

// Handle applies a workflow event using the inbox transaction supplied by the
// caller. Saga state and resulting outbox command therefore commit atomically.
func (c *Coordinator) Handle(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
	if tx == nil {
		return errors.New("saga transaction is required")
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate saga event: %w", err)
	}
	if event.AggregateType != "payment" {
		return fmt.Errorf("unsupported saga aggregate type %q", event.AggregateType)
	}
	if event.Type == "payment.created" {
		return c.start(ctx, tx, event)
	}

	var sagaID, correlationID string
	var state State
	if err := tx.QueryRowContext(ctx, `
		SELECT id, correlation_id, state FROM payment_sagas
		WHERE payment_id = $1 FOR UPDATE`, event.AggregateID).Scan(&sagaID, &correlationID, &state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("payment saga for %s not found", event.AggregateID)
		}
		return fmt.Errorf("lock payment saga: %w", err)
	}
	var reply struct {
		CorrelationID string `json:"correlation_id"`
		Reason        string `json:"reason"`
	}
	if err := json.Unmarshal(event.Payload, &reply); err != nil {
		return fmt.Errorf("decode saga event %s: %w", event.ID, err)
	}
	if reply.CorrelationID == "" || reply.CorrelationID != correlationID {
		return fmt.Errorf("saga correlation mismatch for event %s", event.ID)
	}

	next, command, timeout, failure, err := c.transition(state, event.Type, reply.Reason)
	if err != nil {
		return err
	}
	return c.updateAndCommand(ctx, tx, sagaID, correlationID, event.AggregateID, event.ID, next, command, timeout, failure)
}

func (c *Coordinator) start(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
	sagaID, err := c.newID("saga_")
	if err != nil {
		return fmt.Errorf("generate saga ID: %w", err)
	}
	correlationID, err := c.newID("corr_")
	if err != nil {
		return fmt.Errorf("generate correlation ID: %w", err)
	}
	now := c.now()
	result, err := tx.ExecContext(ctx, `INSERT INTO payment_sagas
		(id, payment_id, correlation_id, state, deadline_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $6) ON CONFLICT (payment_id) DO NOTHING`,
		sagaID, event.AggregateID, correlationID, StateAwaitingPolicy, now.Add(c.policyTimeout), now)
	if err != nil {
		return fmt.Errorf("create payment saga: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect payment saga insert: %w", err)
	}
	if rows == 0 {
		return nil
	}
	if rows != 1 {
		return fmt.Errorf("create payment saga: inserted %d rows", rows)
	}
	return c.enqueue(ctx, tx, sagaID, correlationID, event.AggregateID, event.ID, "policy.evaluate", "", now)
}

func (c *Coordinator) transition(state State, eventType, reason string) (State, string, time.Duration, string, error) {
	switch {
	case state == StateAwaitingPolicy && eventType == "policy.approved":
		return StateAwaitingLedger, "ledger.reserve", c.ledgerTimeout, "", nil
	case state == StateAwaitingPolicy && eventType == "policy.rejected":
		return StateFailed, "payment.fail", 0, reasonOrDefault(reason, "policy rejected"), nil
	case state == StateAwaitingLedger && eventType == "ledger.reserved":
		return StateAwaitingSettlement, "settlement.execute", c.settlementTimeout, "", nil
	case state == StateAwaitingLedger && eventType == "ledger.failed":
		return StateFailed, "payment.fail", 0, reasonOrDefault(reason, "ledger reservation failed"), nil
	case state == StateAwaitingSettlement && eventType == "settlement.completed":
		return StateCompleted, "payment.settle", 0, "", nil
	case state == StateAwaitingSettlement && eventType == "settlement.failed":
		return StateReleasingLedger, "ledger.release", c.ledgerTimeout, reasonOrDefault(reason, "settlement failed"), nil
	case state == StateAwaitingSettlement && eventType == "settlement.refunded":
		return StateRefunding, "ledger.release", c.ledgerTimeout, reasonOrDefault(reason, "settlement refunded"), nil
	case state == StateCompleted && eventType == "settlement.refunded":
		return StateRefunding, "ledger.release", c.ledgerTimeout, reasonOrDefault(reason, "settlement refunded"), nil
	case state == StateReleasingLedger && eventType == "ledger.released":
		return StateLedgerReleased, "payment.fail", 0, reason, nil
	case state == StateRefunding && eventType == "ledger.released":
		return StateRefunded, "payment.refund", 0, reason, nil
	default:
		return "", "", 0, "", fmt.Errorf("event %s is invalid while saga is %s", eventType, state)
	}
}

func (c *Coordinator) updateAndCommand(ctx context.Context, tx *sql.Tx, sagaID, correlationID, paymentID, causedBy string, state State, command string, timeout time.Duration, failure string) error {
	now := c.now()
	var deadline any
	if timeout > 0 {
		deadline = now.Add(timeout)
	}
	_, err := tx.ExecContext(ctx, `UPDATE payment_sagas SET state = $1, deadline_at = $2,
		failure_reason = COALESCE(NULLIF($3, ''), failure_reason), updated_at = $4 WHERE id = $5`, state, deadline, failure, now, sagaID)
	if err != nil {
		return fmt.Errorf("update payment saga: %w", err)
	}
	if command == "" {
		return nil
	}
	return c.enqueue(ctx, tx, sagaID, correlationID, paymentID, causedBy, command, failure, now)
}

func (c *Coordinator) enqueue(ctx context.Context, tx *sql.Tx, sagaID, correlationID, paymentID, causedBy, command, reason string, now time.Time) error {
	eventID, err := c.newID("evt_")
	if err != nil {
		return fmt.Errorf("generate saga command ID: %w", err)
	}
	payload, err := json.Marshal(map[string]string{"saga_id": sagaID, "correlation_id": correlationID, "payment_id": paymentID, "caused_by_event_id": causedBy, "reason": reason})
	if err != nil {
		return fmt.Errorf("encode saga command: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events
		(id, topic, event_type, event_version, aggregate_id, aggregate_type, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5, 'payment', $6, $7)`, eventID, CommandTopic, command, sagaCommandVersion(command), paymentID, payload, now)
	if err != nil {
		return fmt.Errorf("enqueue saga command %s: %w", command, err)
	}
	return nil
}

func sagaCommandVersion(command string) int {
	switch command {
	case "policy.evaluate":
		return eventbus.PolicyEvaluateVersion
	case "ledger.reserve":
		return eventbus.LedgerReserveVersion
	case "settlement.execute":
		return eventbus.SettlementExecuteVersion
	case "payment.fail":
		return eventbus.PaymentFailVersion
	case "payment.settle":
		return eventbus.PaymentSettleVersion
	case "payment.refund":
		return eventbus.PaymentRefundVersion
	case "ledger.release":
		return eventbus.LedgerReleaseVersion
	default:
		panic("unknown saga command type: " + command)
	}
}

func reasonOrDefault(reason, fallback string) string {
	if reason != "" {
		return reason
	}
	return fallback
}

// ExpireOnce claims overdue active sagas and emits their failure or
// compensation command. Multiple timeout workers may run concurrently.
func (c *Coordinator) ExpireOnce(ctx context.Context) (int, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin saga timeout transaction: %w", err)
	}
	defer tx.Rollback()
	now := c.now()
	rows, err := tx.QueryContext(ctx, `SELECT id, payment_id, correlation_id, state
		FROM payment_sagas WHERE deadline_at <= $1
		  AND state IN ('awaiting_policy', 'awaiting_ledger', 'awaiting_settlement', 'releasing_ledger', 'refunding')
		ORDER BY deadline_at FOR UPDATE SKIP LOCKED LIMIT $2`, now, c.timeoutBatchSize)
	if err != nil {
		return 0, fmt.Errorf("claim timed out payment sagas: %w", err)
	}
	type expired struct {
		id, paymentID, correlationID string
		state                        State
	}
	var sagas []expired
	for rows.Next() {
		var s expired
		if err := rows.Scan(&s.id, &s.paymentID, &s.correlationID, &s.state); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan timed out saga: %w", err)
		}
		sagas = append(sagas, s)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close timed out sagas: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read timed out sagas: %w", err)
	}
	for _, s := range sagas {
		next, command, timeout := StateFailed, "payment.fail", time.Duration(0)
		if s.state == StateAwaitingSettlement {
			next, command, timeout = StateReleasingLedger, "ledger.release", c.ledgerTimeout
		}
		if s.state == StateReleasingLedger {
			next, command = StateFailed, "payment.fail"
		}
		if s.state == StateRefunding {
			next, command = StateFailed, "payment.refund"
		}
		if err := c.updateAndCommand(ctx, tx, s.id, s.correlationID, s.paymentID, "timeout", next, command, timeout, string(s.state)+" timeout"); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit saga timeouts: %w", err)
	}
	return len(sagas), nil
}
