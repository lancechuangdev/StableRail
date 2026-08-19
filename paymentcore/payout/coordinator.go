// Package payout coordinates the asynchronous outbound payment workflow.
package payout

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
	"stablerail/paymentcore/workflow"
)

var (
	ErrSagaNotFound      = errors.New("payment saga not found")
	ErrNotInManualReview = errors.New("payment saga is not in manual review")
)

type State string

const (
	StateAwaitingPolicy     State = "awaiting_policy"
	StateAwaitingLedger     State = "awaiting_ledger"
	StateAwaitingSettlement State = "awaiting_settlement"
	StateOnHold             State = "on_hold"
	StateManualReview       State = "manual_review"
	StateReleasingLedger    State = "releasing_ledger"
	StateReturning          State = "returning"
	StateSettlingPayment    State = "settling_payment"
	StateCompleted          State = "completed"
	StateLedgerReleased     State = "ledger_released"
	StateFailed             State = "failed"
	StateReturned           State = "returned"
)

type SagaConfig struct {
	PolicyTimeout     time.Duration
	LedgerTimeout     time.Duration
	SettlementTimeout time.Duration
	ComplianceTimeout time.Duration
	TimeoutBatchSize  int
}

type SagaCoordinator struct {
	db                *sql.DB
	policyTimeout     time.Duration
	ledgerTimeout     time.Duration
	settlementTimeout time.Duration
	complianceTimeout time.Duration
	timeoutBatchSize  int
	now               func() time.Time
	newID             func(string) (string, error)
}

func NewSagaCoordinator(db *sql.DB, config SagaConfig) (*SagaCoordinator, error) {
	if db == nil {
		return nil, errors.New("saga database is required")
	}
	if config.PolicyTimeout < 0 || config.LedgerTimeout < 0 || config.SettlementTimeout < 0 || config.ComplianceTimeout < 0 || config.TimeoutBatchSize < 0 {
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
	if config.ComplianceTimeout == 0 {
		config.ComplianceTimeout = 24 * time.Hour
	}
	if config.TimeoutBatchSize == 0 {
		config.TimeoutBatchSize = 100
	}
	return &SagaCoordinator{
		db: db, policyTimeout: config.PolicyTimeout, ledgerTimeout: config.LedgerTimeout,
		settlementTimeout: config.SettlementTimeout, complianceTimeout: config.ComplianceTimeout, timeoutBatchSize: config.TimeoutBatchSize,
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
func (c *SagaCoordinator) Handle(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
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
		SELECT id, correlation_id, state FROM settlement_sagas
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

func (c *SagaCoordinator) start(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
	sagaID, err := c.newID("saga_")
	if err != nil {
		return fmt.Errorf("generate saga ID: %w", err)
	}
	correlationID, err := c.newID("corr_")
	if err != nil {
		return fmt.Errorf("generate correlation ID: %w", err)
	}
	now := c.now()
	result, err := tx.ExecContext(ctx, `INSERT INTO settlement_sagas
		(id, payment_id, direction, correlation_id, state, deadline_at, created_at, updated_at)
		VALUES ($1, $2, 'payout', $3, $4, $5, $6, $6) ON CONFLICT (payment_id) DO NOTHING`,
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

func (c *SagaCoordinator) transition(state State, eventType, reason string) (State, string, time.Duration, string, error) {
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
		return StateSettlingPayment, "payment.settle", c.ledgerTimeout, "", nil
	case state == StateAwaitingSettlement && eventType == "settlement.on_hold":
		return StateOnHold, "", c.complianceTimeout, reasonOrDefault(reason, "settlement on hold"), nil
	case state == StateOnHold && eventType == "settlement.completed":
		return StateSettlingPayment, "payment.settle", c.ledgerTimeout, "", nil
	case state == StateOnHold && eventType == "settlement.failed":
		return StateFailed, "payment.fail_reserved", 0, reasonOrDefault(reason, "settlement failed"), nil
	case state == StateOnHold && eventType == "settlement.returned":
		return StateReturning, "ledger.release", c.ledgerTimeout, reasonOrDefault(reason, "settlement funds returned"), nil
	case state == StateSettlingPayment && eventType == "payment.succeeded":
		return StateCompleted, "", 0, "", nil
	case state == StateAwaitingSettlement && eventType == "settlement.failed":
		if reason == "submission_failed" {
			return StateFailed, "payment.fail", 0, reason, nil
		}
		return StateFailed, "payment.fail_reserved", 0, reasonOrDefault(reason, "settlement failed"), nil
	case state == StateAwaitingSettlement && eventType == "settlement.returned":
		return StateReturning, "ledger.release", c.ledgerTimeout, reasonOrDefault(reason, "settlement funds returned"), nil
	case state == StateFailed && eventType == "settlement.returned":
		return StateReturning, "ledger.release", c.ledgerTimeout, reasonOrDefault(reason, "settlement funds returned after payment failure"), nil
	case state == StateReleasingLedger && eventType == "ledger.released":
		return StateLedgerReleased, "payment.fail", 0, reason, nil
	case state == StateReturning && eventType == "ledger.released":
		return StateReturned, "payment.return", 0, reason, nil
	default:
		return "", "", 0, "", fmt.Errorf("event %s is invalid while saga is %s", eventType, state)
	}
}

func (c *SagaCoordinator) updateAndCommand(ctx context.Context, tx *sql.Tx, sagaID, correlationID, paymentID, causedBy string, state State, command string, timeout time.Duration, failure string) error {
	now := c.now()
	var deadline any
	if timeout > 0 {
		deadline = now.Add(timeout)
	}
	_, err := tx.ExecContext(ctx, `UPDATE settlement_sagas SET state = $1, deadline_at = $2,
		failure_reason = COALESCE(NULLIF($3, ''), failure_reason), updated_at = $4 WHERE id = $5`, state, deadline, failure, now, sagaID)
	if err != nil {
		return fmt.Errorf("update payment saga: %w", err)
	}
	if command == "" {
		return nil
	}
	return c.enqueue(ctx, tx, sagaID, correlationID, paymentID, causedBy, command, failure, now)
}

func (c *SagaCoordinator) enqueue(ctx context.Context, tx *sql.Tx, sagaID, correlationID, paymentID, causedBy, command, reason string, now time.Time) error {
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
		VALUES ($1, $2, $3, $4, $5, 'payment', $6, $7)`, eventID, workflow.CommandTopic, command, sagaCommandVersion(command), paymentID, payload, now)
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
	case "payment.fail", "payment.fail_reserved":
		return eventbus.PaymentFailVersion
	case "payment.settle":
		return eventbus.PaymentSettleVersion
	case "payment.return":
		return eventbus.PaymentReturnVersion
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

// ResolveManualReview applies an audited operator decision and atomically
// enqueues the next workflow command when one is required.
func (c *SagaCoordinator) ResolveManualReview(ctx context.Context, paymentID, action, operator, note string) error {
	if paymentID == "" || operator == "" || note == "" {
		return errors.New("payment ID, operator, and note are required")
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin manual review resolution: %w", err)
	}
	defer tx.Rollback()
	var sagaID, correlationID string
	var state State
	if err := tx.QueryRowContext(ctx, `SELECT id,correlation_id,state FROM settlement_sagas WHERE payment_id=$1 FOR UPDATE`, paymentID).Scan(&sagaID, &correlationID, &state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSagaNotFound
		}
		return fmt.Errorf("lock manual review saga: %w", err)
	}
	if state != StateManualReview {
		return ErrNotInManualReview
	}
	next, command, timeout := StateOnHold, "", c.complianceTimeout
	switch action {
	case "retry":
	case "complete":
		next, command, timeout = StateSettlingPayment, "payment.settle", c.ledgerTimeout
	case "fail":
		next, command, timeout = StateFailed, "payment.fail_reserved", 0
	case "return":
		next, command, timeout = StateReturning, "ledger.release", c.ledgerTimeout
	default:
		return errors.New("manual review action must be retry, complete, fail, or return")
	}
	now := c.now()
	if _, err := tx.ExecContext(ctx, `INSERT INTO saga_manual_review_actions(saga_id,action,operator,note,occurred_at) VALUES($1,$2,$3,$4,$5)`, sagaID, action, operator, note, now); err != nil {
		return fmt.Errorf("audit manual review resolution: %w", err)
	}
	if err := c.updateAndCommand(ctx, tx, sagaID, correlationID, paymentID, "manual_review", next, command, timeout, note); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit manual review resolution: %w", err)
	}
	return nil
}

// ExpireOnce claims overdue active sagas and emits their failure or
// compensation command. Multiple timeout workers may run concurrently.
func (c *SagaCoordinator) ExpireOnce(ctx context.Context) (int, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin saga timeout transaction: %w", err)
	}
	defer tx.Rollback()
	now := c.now()
	rows, err := tx.QueryContext(ctx, `SELECT id, payment_id, correlation_id, state
		FROM settlement_sagas WHERE direction='payout' AND deadline_at <= $1
		  AND state IN ('awaiting_policy', 'awaiting_ledger', 'awaiting_settlement', 'on_hold', 'releasing_ledger', 'returning', 'settling_payment')
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
			next, command, timeout = StateFailed, "payment.fail_reserved", 0
		}
		if s.state == StateOnHold {
			next, command, timeout = StateManualReview, "", 0
		}
		if s.state == StateReleasingLedger {
			next, command = StateFailed, "payment.fail"
		}
		if s.state == StateReturning {
			next, command, timeout = StateReturning, "ledger.release", c.ledgerTimeout
		}
		if s.state == StateSettlingPayment {
			next, command, timeout = StateSettlingPayment, "payment.settle", c.ledgerTimeout
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
