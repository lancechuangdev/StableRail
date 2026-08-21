package paymentcore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Repository reads shared payment aggregates independently of their direction.
type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, errors.New("payment repository database is required")
	}
	return &Repository{db: db}, nil
}

// GetPayment returns the durable payment and its history.
func (r *Repository) GetPayment(ctx context.Context, paymentID string) (*Payment, error) {
	p := &Payment{}
	err := r.db.QueryRowContext(ctx, `SELECT id, direction, external_reference, currency, amount_minor,
		tenant_id, payment_status, idempotency_key, created_at, updated_at FROM payments WHERE id = $1`, paymentID).
		Scan(&p.ID, &p.Direction, &p.ExternalReference, &p.Currency, &p.AmountMinor, &p.TenantID,
			&p.PaymentStatus, &p.IdempotencyKey, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrPaymentNotFound, paymentID)
	}
	if err != nil {
		return nil, fmt.Errorf("get payment: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, `SELECT id FROM payment_quotes WHERE payment_id=$1`, paymentID).Scan(&p.QuoteID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get payment quote: %w", err)
	}
	var operation SettlementOperation
	if p.Direction == PaymentDirectionPayin {
		var instructions []byte
		err = r.db.QueryRowContext(ctx, `SELECT provider,COALESCE(provider_payin_id,''),settlement_status,reconciliation_status,instructions FROM payins WHERE payment_id=$1`, paymentID).Scan(&operation.Provider, &operation.ProviderReference, &operation.Status, &operation.ReconciliationStatus, &instructions)
		if err == nil && len(instructions) > 0 {
			_ = json.Unmarshal(instructions, &operation.Instructions)
		}
	} else {
		err = r.db.QueryRowContext(ctx, `SELECT provider,COALESCE(provider_payout_id,''),settlement_status,reconciliation_status FROM payouts WHERE payment_id=$1`, paymentID).Scan(&operation.Provider, &operation.ProviderReference, &operation.Status, &operation.ReconciliationStatus)
	}
	if err == nil {
		p.Settlement = &operation
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get %s operation: %w", p.Direction, err)
	}
	rows, err := r.db.QueryContext(ctx, `SELECT payment_status, occurred_at, note FROM payment_timeline_entries
		WHERE payment_id = $1 ORDER BY occurred_at, id`, paymentID)
	if err != nil {
		return nil, fmt.Errorf("get payment timeline: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry TimelineEntry
		if err := rows.Scan(&entry.PaymentStatus, &entry.At, &entry.Note); err != nil {
			return nil, fmt.Errorf("scan payment timeline: %w", err)
		}
		p.Timeline = append(p.Timeline, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read payment timeline: %w", err)
	}
	return p, nil
}

func (r *Repository) Timeline(ctx context.Context, paymentID string) ([]TimelineEntry, error) {
	p, err := r.GetPayment(ctx, paymentID)
	if err != nil {
		return nil, err
	}
	return p.Timeline, nil
}
