// Package ledger owns transactional double-entry ledger operations.
package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"stablerail/paymentcore"
)

var ErrInvalidPaymentStatus = errors.New("invalid payment status")

type ReservationRequest struct {
	PaymentID string
	At        time.Time
}
type ReleaseRequest struct {
	PaymentID string
	At        time.Time
}

type LedgerService interface {
	Reserve(context.Context, *sql.Tx, ReservationRequest) error
	Release(context.Context, *sql.Tx, ReleaseRequest) error
}

type PostgresService struct{}

func NewPostgresService() *PostgresService { return &PostgresService{} }

func (*PostgresService) Reserve(ctx context.Context, tx *sql.Tx, request ReservationRequest) error {
	return post(ctx, tx, request.PaymentID, paymentcore.PaymentStatusCreated, paymentcore.PaymentStatusProcessing,
		"payment.processing", "processing", paymentcore.CashOperatingAccount, paymentcore.SettlementAccount,
		"payment processing started", "payment processing", request.At, true)
}

func (*PostgresService) Release(ctx context.Context, tx *sql.Tx, request ReleaseRequest) error {
	return post(ctx, tx, request.PaymentID, paymentcore.PaymentStatusProcessing, paymentcore.PaymentStatusProcessing,
		"payment.released", "released", paymentcore.SettlementAccount, paymentcore.CashOperatingAccount,
		"ledger reservation released", "ledger reservation released", request.At, false)
}

func post(ctx context.Context, tx *sql.Tx, id string, from, to paymentcore.PaymentStatus, eventType, suffix, debit, credit, message, note string, now time.Time, updateState bool) error {
	return postFrom(ctx, tx, id, []paymentcore.PaymentStatus{from}, to, eventType, suffix, debit, credit, message, note, now, updateState)
}

func postFrom(ctx context.Context, tx *sql.Tx, id string, from []paymentcore.PaymentStatus, to paymentcore.PaymentStatus, eventType, suffix, debit, credit, message, note string, now time.Time, updateState bool) error {
	if tx == nil {
		return errors.New("ledger transaction is required")
	}
	var amount int64
	var currency string
	var status paymentcore.PaymentStatus
	if err := tx.QueryRowContext(ctx, `SELECT payment_status, amount_minor, currency FROM payments WHERE id=$1 FOR UPDATE`, id).Scan(&status, &amount, &currency); err != nil {
		return fmt.Errorf("lock payment: %w", err)
	}
	allowed := false
	for _, candidate := range from {
		allowed = allowed || status == candidate
	}
	if !allowed {
		return fmt.Errorf("%w: payment %s cannot transition from %s", ErrInvalidPaymentStatus, id, status)
	}
	journal := "jrn_" + id + "_" + suffix
	if updateState {
		if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status=$1, funds_status=$2, updated_at=$3 WHERE id=$4`, to, paymentcore.FundsStatusReserved, now, id); err != nil {
			return fmt.Errorf("update payment: %w", err)
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions(id,payment_id,event_type,occurred_at) VALUES($1,$2,$3,$4) ON CONFLICT(payment_id,event_type) DO NOTHING`, journal, id, eventType, now)
	if err != nil {
		return fmt.Errorf("insert journal: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect journal insert: %w", err)
	}
	if rows == 0 {
		return nil
	}
	for _, line := range []struct{ suffix, account, side string }{{"debit", debit, "debit"}, {"credit", credit, "credit"}} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_code,side,amount_minor,currency) VALUES($1,$2,$3,$4,$5,$6)`, journal+":"+line.suffix, journal, line.account, line.side, amount, currency); err != nil {
			return fmt.Errorf("insert ledger entry: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_audit_events(payment_id,event,message,occurred_at) VALUES($1,$2,$3,$4)`, id, suffix, message, now); err != nil {
		return fmt.Errorf("insert ledger audit event: %w", err)
	}
	timelineStatus := status
	if updateState {
		timelineStatus = to
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,payment_status,note,occurred_at) VALUES($1,$2,$3,$4)`, id, timelineStatus, note, now); err != nil {
		return fmt.Errorf("insert ledger timeline entry: %w", err)
	}
	return nil
}
