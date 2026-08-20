package payout

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"stablerail/paymentcore"
)

func (s *Service) Process(ctx context.Context, paymentID string) error {
	return s.transition(ctx, paymentID, paymentcore.PaymentStatusCreated, paymentcore.PaymentStatusProcessing, "processing", "payment processing started", "payment processing")
}

func (s *Service) Settle(ctx context.Context, paymentID string) error {
	return s.transition(ctx, paymentID, paymentcore.PaymentStatusProcessing, paymentcore.PaymentStatusSucceeded, "succeeded", "payment succeeded", "payment succeeded")
}

func (s *Service) transition(ctx context.Context, paymentID string, from, to paymentcore.PaymentStatus, auditEvent, auditMessage, timelineNote string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin payment transition: %w", err)
	}
	defer tx.Rollback()
	var current paymentcore.PaymentStatus
	var amountMinor int64
	var currency string
	if err := tx.QueryRowContext(ctx, `SELECT payment_status, amount_minor, currency FROM payments WHERE id = $1 FOR UPDATE`, paymentID).Scan(&current, &amountMinor, &currency); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("payment %s not found", paymentID)
		}
		return fmt.Errorf("lock payment: %w", err)
	}
	if current != from {
		return fmt.Errorf("payment %s cannot transition from %s", paymentID, current)
	}
	now := s.now()
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status = $1, funds_status = $2, updated_at = $3 WHERE id = $4`, to, fundsStatusForPaymentStatus(to), now, paymentID); err != nil {
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
		PaymentStatus paymentcore.PaymentStatus `json:"payment_status"`
		FundsStatus   paymentcore.FundsStatus   `json:"funds_status"`
	}{to, fundsStatusForPaymentStatus(to)})
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

func fundsStatusForPaymentStatus(status paymentcore.PaymentStatus) paymentcore.FundsStatus {
	switch status {
	case paymentcore.PaymentStatusCreated:
		return paymentcore.FundsStatusAvailable
	case paymentcore.PaymentStatusProcessing:
		return paymentcore.FundsStatusReserved
	case paymentcore.PaymentStatusSucceeded:
		return paymentcore.FundsStatusConsumed
	case paymentcore.PaymentStatusFailed:
		return paymentcore.FundsStatusAvailable
	default:
		panic("unknown payment status: " + status)
	}
}
