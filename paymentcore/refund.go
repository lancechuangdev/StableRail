package paymentcore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"stablerail/eventbus"
)

// CreateRefund creates a linked refund and atomically enqueues its provider command.
// Partial refunds are allowed up to the original payment amount.
func (s *PostgresService) CreateRefund(ctx context.Context, paymentID, tenantID, idempotencyKey string, amountMinor int64, reason string) (*Refund, error) {
	if paymentID == "" || tenantID == "" || idempotencyKey == "" || amountMinor <= 0 || reason == "" {
		return nil, errors.New("invalid refund payload")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin refund transaction: %w", err)
	}
	defer tx.Rollback()

	var paymentTenant, currency string
	var paymentAmount int64
	var paymentStatus PaymentStatus
	var fundsStatus FundsStatus
	err = tx.QueryRowContext(ctx, `SELECT tenant_id,currency,amount_minor,payment_status,funds_status FROM payments WHERE id=$1 FOR UPDATE`, paymentID).
		Scan(&paymentTenant, &currency, &paymentAmount, &paymentStatus, &fundsStatus)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && paymentTenant != tenantID) {
		return nil, fmt.Errorf("%w: %s", ErrPaymentNotFound, paymentID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock refund payment: %w", err)
	}

	existing, err := loadRefundByIdempotencyKey(ctx, tx, tenantID, idempotencyKey)
	if err == nil {
		if existing.PaymentID != paymentID || existing.AmountMinor != amountMinor || existing.Reason != reason {
			return nil, ErrIdempotencyConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit idempotent refund lookup: %w", err)
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if paymentStatus != PaymentStatusSucceeded || fundsStatus != FundsStatusConsumed {
		return nil, fmt.Errorf("%w: payment must be succeeded with consumed funds", ErrPaymentNotRefundable)
	}
	var refunded int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(amount_minor),0) FROM payment_refunds WHERE payment_id=$1 AND status IN ('created','processing','succeeded')`, paymentID).Scan(&refunded); err != nil {
		return nil, fmt.Errorf("sum prior refunds: %w", err)
	}
	if amountMinor > paymentAmount-refunded {
		return nil, ErrRefundAmountExceeded
	}

	refundID, err := s.newID("ref_")
	if err != nil {
		return nil, fmt.Errorf("generate refund ID: %w", err)
	}
	eventID, err := s.newID("evt_")
	if err != nil {
		return nil, fmt.Errorf("generate refund event ID: %w", err)
	}
	now := s.now()
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_refunds(id,payment_id,tenant_id,idempotency_key,amount_minor,currency,status,reason,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,'created',$7,$8,$8)`, refundID, paymentID, tenantID, idempotencyKey, amountMinor, currency, reason, now); err != nil {
		return nil, fmt.Errorf("insert refund: %w", err)
	}
	payload, err := json.Marshal(map[string]any{"refund_id": refundID, "payment_id": paymentID, "amount_minor": amountMinor, "currency": currency, "reason": reason})
	if err != nil {
		return nil, fmt.Errorf("encode refund event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,'payment-commands','refund.execute',$2,$3,'payment',$4,$5)`, eventID, eventbus.RefundExecuteVersion, paymentID, payload, now); err != nil {
		return nil, fmt.Errorf("enqueue refund event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit refund: %w", err)
	}
	return &Refund{ID: refundID, PaymentID: paymentID, AmountMinor: amountMinor, Currency: currency, Status: "created", Reason: reason, CreatedAt: now, UpdatedAt: now, IdempotencyKey: idempotencyKey}, nil
}

func loadRefundByIdempotencyKey(ctx context.Context, tx *sql.Tx, tenantID, key string) (*Refund, error) {
	r := &Refund{IdempotencyKey: key}
	err := tx.QueryRowContext(ctx, `SELECT id,payment_id,amount_minor,currency,status,reason,created_at,updated_at FROM payment_refunds WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, key).
		Scan(&r.ID, &r.PaymentID, &r.AmountMinor, &r.Currency, &r.Status, &r.Reason, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}
