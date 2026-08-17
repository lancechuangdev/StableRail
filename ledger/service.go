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
type ReturnRequest struct {
	ID, PaymentID, Provider, ProviderEventID, Reason string
	At                                               time.Time
}

type LedgerService interface {
	Reserve(context.Context, *sql.Tx, ReservationRequest) error
	Release(context.Context, *sql.Tx, ReleaseRequest) error
	RecordReturn(context.Context, *sql.Tx, ReturnRequest) error
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

// RecordReturn records funds received back after a payout succeeded without
// rewriting the original payment outcome.
func (*PostgresService) RecordReturn(ctx context.Context, tx *sql.Tx, request ReturnRequest) error {
	if tx == nil {
		return errors.New("ledger transaction is required")
	}
	if request.ID == "" || request.PaymentID == "" || request.Provider == "" || request.ProviderEventID == "" || request.Reason == "" {
		return errors.New("return ID, payment ID, provider, provider event ID, and reason are required")
	}
	var paymentStatus paymentcore.PaymentStatus
	var fundsStatus paymentcore.FundsStatus
	var amount int64
	var currency string
	if err := tx.QueryRowContext(ctx, `SELECT payment_status,funds_status,amount_minor,currency FROM payments WHERE id=$1 FOR UPDATE`, request.PaymentID).Scan(&paymentStatus, &fundsStatus, &amount, &currency); err != nil {
		return fmt.Errorf("lock returned payment: %w", err)
	}
	if paymentStatus != paymentcore.PaymentStatusSucceeded || fundsStatus != paymentcore.FundsStatusConsumed {
		return fmt.Errorf("%w: post-success return requires succeeded/consumed payment %s", ErrInvalidPaymentStatus, request.PaymentID)
	}
	journalID := "jrn_" + request.ID
	result, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions(id,payment_id,event_type,occurred_at) VALUES($1,$2,'payment.return.succeeded',$3) ON CONFLICT(payment_id,event_type) DO NOTHING`, journalID, request.PaymentID, request.At)
	if err != nil {
		return fmt.Errorf("insert return journal: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect return journal: %w", err)
	}
	if rows == 0 {
		return nil
	}
	for _, line := range []struct{ suffix, account, side string }{{"debit", paymentcore.CashOperatingAccount, "debit"}, {"credit", paymentcore.SettlementAccount, "credit"}} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_code,side,amount_minor,currency) VALUES($1,$2,$3,$4,$5,$6)`, journalID+":"+line.suffix, journalID, line.account, line.side, amount, currency); err != nil {
			return fmt.Errorf("insert return ledger entry: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_returns(id,payment_id,provider,provider_event_id,amount_minor,currency,status,reason,ledger_transaction_id,occurred_at) VALUES($1,$2,$3,$4,$5,$6,'succeeded',$7,$8,$9)`, request.ID, request.PaymentID, request.Provider, request.ProviderEventID, amount, currency, request.Reason, journalID, request.At); err != nil {
		return fmt.Errorf("insert payment return: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_audit_events(payment_id,event,message,occurred_at) VALUES($1,'return_succeeded',$2,$3)`, request.PaymentID, request.Reason, request.At); err != nil {
		return fmt.Errorf("insert return audit event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,payment_status,note,occurred_at) VALUES($1,'succeeded',$2,$3)`, request.PaymentID, request.Reason, request.At); err != nil {
		return fmt.Errorf("insert return timeline entry: %w", err)
	}
	return nil
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
