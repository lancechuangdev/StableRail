package payout

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"stablerail/paymentcore"
)

type Refund = paymentcore.Refund

var (
	ErrPaymentNotRefundable = paymentcore.ErrPaymentNotRefundable
	ErrRefundAmountExceeded = paymentcore.ErrRefundAmountExceeded
)

type refundLookup struct {
	refund        Refund
	amountMinor   int64
	payoutQuoteID string
}

// CreateRefund creates a new payment linked to the original payment. The new
// payment enters the ordinary payment saga and is settled as a normal payout.
func (s *Service) CreateRefund(ctx context.Context, paymentID, tenantID, idempotencyKey string, amountMinor int64, reason, payoutQuoteID string) (*Refund, error) {
	// Validate the request
	if paymentID == "" || tenantID == "" || idempotencyKey == "" || amountMinor <= 0 || reason == "" {
		return nil, errors.New("invalid refund payload")
	}

	// Start one database transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin refund transaction: %w", err)
	}
	defer tx.Rollback()

	// Lock and load the original payment
	var paymentTenant, externalReference, currency string
	var paymentAmount int64
	var paymentStatus PaymentStatus
	err = tx.QueryRowContext(ctx, `SELECT tenant_id,external_reference,currency,amount_minor,payment_status FROM payments WHERE id=$1 FOR UPDATE`, paymentID).
		Scan(&paymentTenant, &externalReference, &currency, &paymentAmount, &paymentStatus)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && paymentTenant != tenantID) {
		return nil, fmt.Errorf("%w: %s", ErrPaymentNotFound, paymentID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock refund payment: %w", err)
	}

	// Handle idempotent retries
	existing, err := loadRefundByIdempotencyKey(ctx, tx, tenantID, idempotencyKey)
	if err == nil {
		if existing.refund.PaymentID != paymentID || existing.amountMinor != amountMinor || existing.refund.Reason != reason || existing.payoutQuoteID != payoutQuoteID {
			return nil, ErrIdempotencyConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit idempotent refund lookup: %w", err)
		}
		return &existing.refund, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	// Verify that the original payment is refundable
	var settled bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ledger_transactions WHERE payment_id=$1 AND event_type='payment.succeeded' AND ledger_status='posted')`, paymentID).Scan(&settled); err != nil {
		return nil, fmt.Errorf("inspect settlement journal: %w", err)
	}
	if paymentStatus != PaymentStatusSucceeded || !settled {
		return nil, fmt.Errorf("%w: payment must be succeeded with a posted settlement journal", ErrPaymentNotRefundable)
	}

	// Calculate the remaining refundable amount
	var refunded int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(r.amount_minor),0) FROM payment_refunds r JOIN payments p ON p.id=r.refund_payment_id WHERE r.payment_id=$1 AND p.payment_status <> 'failed'`, paymentID).Scan(&refunded); err != nil {
		return nil, fmt.Errorf("sum prior refunds: %w", err)
	}
	if amountMinor > paymentAmount-refunded {
		return nil, ErrRefundAmountExceeded
	}

	// Validate the payout quote if provided
	if payoutQuoteID != "" {
		var quoteTenant, quoteCurrency, quoteStatus string
		var quoteAmount int64
		var expiresAt time.Time
		if err := tx.QueryRowContext(ctx, `SELECT tenant_id,source_currency,sender_amount_minor,status,expires_at FROM payment_quotes WHERE direction='payout' AND id=$1 FOR UPDATE`, payoutQuoteID).Scan(&quoteTenant, &quoteCurrency, &quoteAmount, &quoteStatus, &expiresAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, errors.New("payout quote not found")
			}
			return nil, fmt.Errorf("lock refund payout quote: %w", err)
		}
		if quoteStatus != "open" || !expiresAt.After(s.now()) {
			return nil, errors.New("payout quote expired or already accepted")
		}
		if quoteTenant != tenantID || quoteCurrency != currency || quoteAmount != amountMinor {
			return nil, errors.New("refund tenant, amount, or currency does not match payout quote")
		}
	}

	// Create the refund payment
	refundID, err := s.newID("ref_")
	if err != nil {
		return nil, fmt.Errorf("generate refund ID: %w", err)
	}
	refundPaymentID, err := s.newID("pay_")
	if err != nil {
		return nil, fmt.Errorf("generate refund payment ID: %w", err)
	}
	now := s.now()
	refundReference := externalReference + ":refund:" + refundID
	if _, err := tx.ExecContext(ctx, `INSERT INTO payments(id,direction,external_reference,currency,amount_minor,tenant_id,payment_status,idempotency_key,created_at,updated_at) VALUES($1,'payout',$2,$3,$4,$5,$6,$7,$8,$8)`, refundPaymentID, refundReference, currency, amountMinor, tenantID, PaymentStatusCreated, idempotencyKey, now); err != nil {
		return nil, fmt.Errorf("insert refund payment: %w", err)
	}

	// Link the refund payment to the original payment
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_refunds(id,payment_id,refund_payment_id,tenant_id,idempotency_key,amount_minor,currency,reason,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)`, refundID, paymentID, refundPaymentID, tenantID, idempotencyKey, amountMinor, currency, reason, now); err != nil {
		return nil, fmt.Errorf("link refund payment: %w", err)
	}
	if payoutQuoteID != "" {
		result, err := tx.ExecContext(ctx, `UPDATE payment_quotes SET status='accepted',payment_id=$1,updated_at=$2 WHERE id=$3 AND status='open'`, refundPaymentID, now, payoutQuoteID)
		if err != nil {
			return nil, fmt.Errorf("accept refund payout quote: %w", err)
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 1 {
			return nil, errors.New("payout quote already accepted")
		}
	}

	// Record the refund payment’s history
	if err := paymentcore.NewHistoryService().Record(ctx, tx,
		paymentcore.AuditRecord{PaymentID: refundPaymentID, Event: "created", Message: "merchant refund payment created: " + reason, At: now},
		paymentcore.TimelineRecord{PaymentID: refundPaymentID, PaymentStatus: PaymentStatusCreated, Note: "refund payment created", At: now}); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(map[string]any{"external_reference": refundReference, "currency": currency, "amount_minor": amountMinor, "tenant_id": tenantID, "payment_status": PaymentStatusCreated, "refund_id": refundID, "original_payment_id": paymentID})
	if err != nil {
		return nil, fmt.Errorf("encode refund payment event: %w", err)
	}

	// Publish the normal payment.created event
	if err := s.enqueue(ctx, tx, refundPaymentID, "payment.created", payload, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit refund payment: %w", err)
	}
	return &Refund{ID: refundID, PaymentID: paymentID, RefundPaymentID: refundPaymentID, Reason: reason, CreatedAt: now, UpdatedAt: now, IdempotencyKey: idempotencyKey}, nil
}

func loadRefundByIdempotencyKey(ctx context.Context, tx *sql.Tx, tenantID, key string) (*refundLookup, error) {
	r := &refundLookup{refund: Refund{IdempotencyKey: key}}
	err := tx.QueryRowContext(ctx, `SELECT r.id,r.payment_id,r.refund_payment_id,r.reason,r.created_at,r.updated_at,p.amount_minor,COALESCE(q.id,'') FROM payment_refunds r JOIN payments p ON p.id=r.refund_payment_id LEFT JOIN payment_quotes q ON q.payment_id=r.refund_payment_id WHERE r.tenant_id=$1 AND r.idempotency_key=$2`, tenantID, key).
		Scan(&r.refund.ID, &r.refund.PaymentID, &r.refund.RefundPaymentID, &r.refund.Reason, &r.refund.CreatedAt, &r.refund.UpdatedAt, &r.amountMinor, &r.payoutQuoteID)
	return r, err
}
