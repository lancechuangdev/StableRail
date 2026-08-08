package paymentcore

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

const PaymentEventsTopic eventbus.Topic = "payment-events"

// PostgresService persists payment state and its domain event atomically.
type PostgresService struct {
	db    *sql.DB
	now   func() time.Time
	newID func(string) (string, error)
}

func NewPostgresService(db *sql.DB) *PostgresService {
	return &PostgresService{
		db:  db,
		now: func() time.Time { return time.Now().UTC() },
		newID: func(prefix string) (string, error) {
			bytes := make([]byte, 16)
			if _, err := rand.Read(bytes); err != nil {
				return "", err
			}
			return prefix + hex.EncodeToString(bytes), nil
		},
	}
}

func (s *PostgresService) CreatePayment(
	ctx context.Context,
	externalRef, currency string,
	amountMinor int64,
	customerID, idempotencyKey string,
) (*Payment, error) {
	if externalRef == "" || currency == "" || amountMinor <= 0 || customerID == "" || idempotencyKey == "" {
		return nil, errors.New("invalid payment payload")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin create payment transaction: %w", err)
	}
	defer tx.Rollback()

	paymentID, err := s.newID("pay_")
	if err != nil {
		return nil, fmt.Errorf("generate payment ID: %w", err)
	}
	now := s.now()
	payment := &Payment{
		ID: paymentID, ExternalReference: externalRef, Currency: currency,
		AmountMinor: amountMinor, CustomerID: customerID, State: StateCreated,
		IdempotencyKey: idempotencyKey, CreatedAt: now, UpdatedAt: now,
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO payments
			(id, external_reference, currency, amount_minor, customer_id, state,
			 ledger_balance, idempotency_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		payment.ID, payment.ExternalReference, payment.Currency, payment.AmountMinor,
		payment.CustomerID, payment.State, payment.LedgerBalance, payment.IdempotencyKey, now,
	)
	if err != nil {
		return nil, fmt.Errorf("insert payment: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("inspect payment insert: %w", err)
	}
	if rows == 0 {
		existing, err := getPaymentByIdempotencyKey(ctx, tx, idempotencyKey)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit idempotent payment lookup: %w", err)
		}
		return existing, nil
	}

	if err := insertHistory(ctx, tx, payment.ID, "created", "payment intent created", StateCreated, "payment created", now); err != nil {
		return nil, err
	}
	payment.AuditLog = []AuditEvent{{Event: "created", Message: "payment intent created", At: now}}
	payment.Timeline = []TimelineEntry{{State: StateCreated, At: now, Note: "payment created"}}

	payload, err := json.Marshal(struct {
		ExternalReference string `json:"external_reference"`
		Currency          string `json:"currency"`
		AmountMinor       int64  `json:"amount_minor"`
		CustomerID        string `json:"customer_id"`
	}{externalRef, currency, amountMinor, customerID})
	if err != nil {
		return nil, fmt.Errorf("marshal payment created payload: %w", err)
	}
	if err := s.enqueue(ctx, tx, payment.ID, "payment.created", payload, now); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit create payment transaction: %w", err)
	}
	return clonePayment(payment), nil
}

func (s *PostgresService) Process(ctx context.Context, paymentID string) error {
	return s.transition(ctx, paymentID, StateCreated, StateProcessing, "processing", "payment processing started", "payment processing")
}

func (s *PostgresService) Settle(ctx context.Context, paymentID string) error {
	return s.transition(ctx, paymentID, StateProcessing, StateSettled, "settled", "payment settled successfully", "payment settled")
}

func (s *PostgresService) transition(
	ctx context.Context,
	paymentID string,
	from, to PaymentState,
	auditEvent, auditMessage, timelineNote string,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin payment transition: %w", err)
	}
	defer tx.Rollback()

	var current PaymentState
	var amountMinor int64
	if err := tx.QueryRowContext(ctx,
		`SELECT state, amount_minor FROM payments WHERE id = $1 FOR UPDATE`, paymentID,
	).Scan(&current, &amountMinor); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("payment %s not found", paymentID)
		}
		return fmt.Errorf("lock payment: %w", err)
	}
	if current != from {
		return fmt.Errorf("payment %s cannot transition from %s", paymentID, current)
	}

	now := s.now()
	if _, err := tx.ExecContext(ctx,
		`UPDATE payments SET state = $1, ledger_balance = $2, updated_at = $3 WHERE id = $4`,
		to, amountMinor, now, paymentID,
	); err != nil {
		return fmt.Errorf("update payment: %w", err)
	}
	if err := insertHistory(ctx, tx, paymentID, auditEvent, auditMessage, to, timelineNote, now); err != nil {
		return err
	}
	payload, err := json.Marshal(struct {
		State PaymentState `json:"state"`
	}{to})
	if err != nil {
		return fmt.Errorf("marshal payment transition payload: %w", err)
	}
	if err := s.enqueue(ctx, tx, paymentID, "payment."+string(to), payload, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit payment transition: %w", err)
	}
	return nil
}

func (s *PostgresService) enqueue(ctx context.Context, tx *sql.Tx, paymentID, eventType string, payload []byte, now time.Time) error {
	eventID, err := s.newID("evt_")
	if err != nil {
		return fmt.Errorf("generate event ID: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox_events
			(id, topic, event_type, event_version, aggregate_id, aggregate_type, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		eventID, PaymentEventsTopic, eventType, 1, paymentID, "payment", payload, now,
	)
	if err != nil {
		return fmt.Errorf("enqueue outbox event: %w", err)
	}
	return nil
}

func insertHistory(
	ctx context.Context, tx *sql.Tx, paymentID, event, message string,
	state PaymentState, note string, now time.Time,
) error {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO payment_audit_events (payment_id, event, message, occurred_at) VALUES ($1, $2, $3, $4)`,
		paymentID, event, message, now,
	); err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO payment_timeline_entries (payment_id, state, note, occurred_at) VALUES ($1, $2, $3, $4)`,
		paymentID, state, note, now,
	); err != nil {
		return fmt.Errorf("insert timeline entry: %w", err)
	}
	return nil
}

func getPaymentByIdempotencyKey(ctx context.Context, tx *sql.Tx, key string) (*Payment, error) {
	payment := &Payment{}
	err := tx.QueryRowContext(ctx, `
		SELECT id, external_reference, currency, amount_minor, customer_id, state,
		       ledger_balance, idempotency_key, created_at, updated_at
		FROM payments WHERE idempotency_key = $1`, key,
	).Scan(
		&payment.ID, &payment.ExternalReference, &payment.Currency, &payment.AmountMinor,
		&payment.CustomerID, &payment.State, &payment.LedgerBalance,
		&payment.IdempotencyKey, &payment.CreatedAt, &payment.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("get idempotent payment: %w", err)
	}
	return payment, nil
}
