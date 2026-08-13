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
	tenantID, idempotencyKey string,
) (*Payment, error) {
	return s.createPayment(ctx, externalRef, currency, amountMinor, tenantID, idempotencyKey, "", nil)
}

// CreatePaymentWithPayoutQuote atomically consumes one provider-bound payout quote.
func (s *PostgresService) CreatePaymentWithPayoutQuote(ctx context.Context, externalRef, currency string, amountMinor int64, tenantID, idempotencyKey, payoutQuoteID string) (*Payment, error) {
	if payoutQuoteID == "" {
		return nil, errors.New("payout quote ID is required")
	}
	return s.createPayment(ctx, externalRef, currency, amountMinor, tenantID, idempotencyKey, payoutQuoteID, nil)
}

func (s *PostgresService) CreatePaymentWithDestination(ctx context.Context, externalRef, currency string, amountMinor int64, tenantID, idempotencyKey string, destination Destination) (*Payment, error) {
	if err := destination.Validate(); err != nil {
		return nil, err
	}
	return s.createPayment(ctx, externalRef, currency, amountMinor, tenantID, idempotencyKey, "", &destination)
}

func (s *PostgresService) createPayment(
	ctx context.Context, externalRef, currency string, amountMinor int64, tenantID, idempotencyKey, payoutQuoteID string, destination *Destination,
) (*Payment, error) {
	if externalRef == "" || currency == "" || amountMinor <= 0 || tenantID == "" || idempotencyKey == "" {
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
		AmountMinor: amountMinor, TenantID: tenantID, State: StateCreated,
		IdempotencyKey: idempotencyKey, PayoutQuoteID: payoutQuoteID, CreatedAt: now, UpdatedAt: now,
		Destination: destination,
	}
	if payoutQuoteID != "" {
		var quoteTenant, quoteCurrency string
		var quoteAmount int64
		var quoteStatus string
		var expiresAt time.Time
		err := tx.QueryRowContext(ctx, `SELECT tenant_id,source_currency,sender_amount_minor,status,expires_at FROM blindpay_quotes WHERE id=$1 FOR UPDATE`, payoutQuoteID).Scan(&quoteTenant, &quoteCurrency, &quoteAmount, &quoteStatus, &expiresAt)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("payout quote not found")
		}
		if err != nil {
			return nil, fmt.Errorf("lock payout quote: %w", err)
		}
		if quoteStatus == "accepted" {
			existing, lookupErr := getPaymentByIdempotencyKey(ctx, tx, idempotencyKey)
			if lookupErr == nil && paymentRequestMatches(existing, externalRef, currency, amountMinor, tenantID, payoutQuoteID, destination) {
				if err := tx.Commit(); err != nil {
					return nil, fmt.Errorf("commit idempotent payout payment lookup: %w", err)
				}
				return existing, nil
			}
			return nil, errors.New("payout quote already accepted")
		}
		if quoteStatus != "open" || !expiresAt.After(now) {
			if _, err := tx.ExecContext(ctx, `UPDATE blindpay_quotes SET status='expired',updated_at=$1 WHERE id=$2 AND status='open'`, now, payoutQuoteID); err != nil {
				return nil, fmt.Errorf("expire payout quote: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return nil, fmt.Errorf("commit payout quote expiration: %w", err)
			}
			return nil, errors.New("payout quote expired")
		}
		if quoteTenant != tenantID || quoteCurrency != currency || quoteAmount != amountMinor {
			return nil, errors.New("payment tenant, amount, or currency does not match payout quote")
		}
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO payments
			(id, external_reference, currency, amount_minor, tenant_id, state,
			 idempotency_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		payment.ID, payment.ExternalReference, payment.Currency, payment.AmountMinor,
		payment.TenantID, payment.State, payment.IdempotencyKey, now,
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
		if !paymentRequestMatches(existing, externalRef, currency, amountMinor, tenantID, payoutQuoteID, destination) {
			return nil, ErrIdempotencyConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit idempotent payment lookup: %w", err)
		}
		return existing, nil
	}
	if destination != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO payment_destinations(payment_id,kind,chain,address,created_at) VALUES($1,$2,NULLIF($3,''),NULLIF($4,''),$5)`, payment.ID, destination.Type, destination.Chain, destination.Address, now); err != nil {
			return nil, fmt.Errorf("insert payment destination: %w", err)
		}
	}
	if payoutQuoteID != "" {
		result, err := tx.ExecContext(ctx, `UPDATE blindpay_quotes SET status='accepted',payment_id=$1,updated_at=$2 WHERE id=$3 AND status='open'`, payment.ID, now, payoutQuoteID)
		if err != nil {
			return nil, fmt.Errorf("accept payout quote: %w", err)
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 1 {
			return nil, errors.New("payout quote already accepted")
		}
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
		TenantID          string `json:"tenant_id"`
	}{externalRef, currency, amountMinor, tenantID})
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

// GetPayment returns the durable payment and its history.
func (s *PostgresService) GetPayment(ctx context.Context, paymentID string) (*Payment, error) {
	p := &Payment{}
	err := s.db.QueryRowContext(ctx, `SELECT id, external_reference, currency, amount_minor,
		tenant_id, state, idempotency_key, created_at, updated_at FROM payments WHERE id = $1`, paymentID).
		Scan(&p.ID, &p.ExternalReference, &p.Currency, &p.AmountMinor, &p.TenantID,
			&p.State, &p.IdempotencyKey, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrPaymentNotFound, paymentID)
	}
	var destination Destination
	err = s.db.QueryRowContext(ctx, `SELECT kind,COALESCE(chain,''),COALESCE(address,'') FROM payment_destinations WHERE payment_id=$1`, paymentID).Scan(&destination.Type, &destination.Chain, &destination.Address)
	if err == nil {
		p.Destination = &destination
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get payment destination: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("get payment: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM blindpay_quotes WHERE payment_id=$1`, paymentID).Scan(&p.PayoutQuoteID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get payment payout quote: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT state, occurred_at, note FROM payment_timeline_entries
		WHERE payment_id = $1 ORDER BY occurred_at, id`, paymentID)
	if err != nil {
		return nil, fmt.Errorf("get payment timeline: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry TimelineEntry
		if err := rows.Scan(&entry.State, &entry.At, &entry.Note); err != nil {
			return nil, fmt.Errorf("scan payment timeline: %w", err)
		}
		p.Timeline = append(p.Timeline, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read payment timeline: %w", err)
	}
	return p, nil
}

func (s *PostgresService) Timeline(ctx context.Context, paymentID string) ([]TimelineEntry, error) {
	p, err := s.GetPayment(ctx, paymentID)
	if err != nil {
		return nil, err
	}
	return p.Timeline, nil
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
	var currency string
	if err := tx.QueryRowContext(ctx,
		`SELECT state, amount_minor, currency FROM payments WHERE id = $1 FOR UPDATE`, paymentID,
	).Scan(&current, &amountMinor, &currency); err != nil {
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
		`UPDATE payments SET state = $1, updated_at = $2 WHERE id = $3`,
		to, now, paymentID,
	); err != nil {
		return fmt.Errorf("update payment: %w", err)
	}
	debitAccount, creditAccount, err := transitionAccounts(to)
	if err != nil {
		return err
	}
	if err := s.insertJournal(ctx, tx, paymentID, "payment."+string(to), debitAccount, creditAccount, amountMinor, currency, now); err != nil {
		return err
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

func transitionAccounts(state PaymentState) (string, string, error) {
	switch state {
	case StateProcessing:
		return CashOperatingAccount, SettlementAccount, nil
	case StateSettled:
		return SettlementAccount, CashOperatingAccount, nil
	default:
		return "", "", fmt.Errorf("no ledger posting defined for state %s", state)
	}
}

func (s *PostgresService) insertJournal(
	ctx context.Context, tx *sql.Tx, paymentID, eventType, debitAccount, creditAccount string,
	amountMinor int64, currency string, now time.Time,
) error {
	journalID, err := s.newID("jrn_")
	if err != nil {
		return fmt.Errorf("generate journal ID: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ledger_transactions (id, payment_id, event_type, occurred_at)
		VALUES ($1, $2, $3, $4)`,
		journalID, paymentID, eventType, now,
	); err != nil {
		return fmt.Errorf("insert ledger transaction: %w", err)
	}

	lines := []struct {
		id, account string
		side        EntrySide
	}{
		{journalID + ":debit", debitAccount, EntryDebit},
		{journalID + ":credit", creditAccount, EntryCredit},
	}
	for _, line := range lines {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO ledger_entries
				(id, transaction_id, account_code, side, amount_minor, currency)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			line.id, journalID, line.account, line.side, amountMinor, currency,
		); err != nil {
			return fmt.Errorf("insert %s ledger line: %w", line.side, err)
		}
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
		eventID, PaymentEventsTopic, eventType, paymentEventVersion(eventType), paymentID, "payment", payload, now,
	)
	if err != nil {
		return fmt.Errorf("enqueue outbox event: %w", err)
	}
	return nil
}

func paymentEventVersion(eventType string) int {
	switch eventType {
	case "payment.created":
		return eventbus.PaymentCreatedVersion
	case "payment.processing":
		return eventbus.PaymentProcessingVersion
	case "payment.settled":
		return eventbus.PaymentSettledVersion
	default:
		panic("unknown payment event type: " + eventType)
	}
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
		SELECT id, external_reference, currency, amount_minor, tenant_id, state,
		       idempotency_key, created_at, updated_at
		FROM payments WHERE idempotency_key = $1`, key,
	).Scan(
		&payment.ID, &payment.ExternalReference, &payment.Currency, &payment.AmountMinor,
		&payment.TenantID, &payment.State, &payment.IdempotencyKey,
		&payment.CreatedAt, &payment.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("get idempotent payment: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT id FROM blindpay_quotes WHERE payment_id=$1`, payment.ID).Scan(&payment.PayoutQuoteID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get idempotent payment payout quote: %w", err)
	}
	var destination Destination
	if err := tx.QueryRowContext(ctx, `SELECT kind,COALESCE(chain,''),COALESCE(address,'') FROM payment_destinations WHERE payment_id=$1`, payment.ID).Scan(&destination.Type, &destination.Chain, &destination.Address); err == nil {
		payment.Destination = &destination
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get idempotent payment destination: %w", err)
	}
	return payment, nil
}

func paymentRequestMatches(payment *Payment, externalRef, currency string, amountMinor int64, tenantID, payoutQuoteID string, destination *Destination) bool {
	if payment.ExternalReference != externalRef || payment.Currency != currency || payment.AmountMinor != amountMinor || payment.TenantID != tenantID || payment.PayoutQuoteID != payoutQuoteID {
		return false
	}
	if payment.Destination == nil || destination == nil {
		return payment.Destination == nil && destination == nil
	}
	return *payment.Destination == *destination
}
