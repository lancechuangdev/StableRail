// Package ledger owns transactional double-entry ledger operations.
package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"stablerail/eventbus"
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
type SettlementRequest struct {
	PaymentID string
	At        time.Time
}
type PayinReceiptRequest struct {
	PayinID       string
	CorrelationID string
	At            time.Time
}
type ReturnRequest struct {
	ID, PaymentID, Provider, ProviderEventID, Reason string
	At                                               time.Time
}

type LedgerService interface {
	Reserve(context.Context, *sql.Tx, ReservationRequest) error
	Release(context.Context, *sql.Tx, ReleaseRequest) error
	Settle(context.Context, *sql.Tx, SettlementRequest) error
	RecordPayin(context.Context, *sql.Tx, PayinReceiptRequest) error
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

func (*PostgresService) Settle(ctx context.Context, tx *sql.Tx, request SettlementRequest) error {
	return post(ctx, tx, request.PaymentID, paymentcore.PaymentStatusProcessing, paymentcore.PaymentStatusSucceeded,
		"payment.succeeded", "succeeded", paymentcore.SettlementAccount, paymentcore.CashOperatingAccount,
		"payment succeeded", "payment succeeded", request.At, true)
}

// RecordPayin recognizes provider-confirmed inbound funds and completes the
// pay-in and its public payment in the caller's inbox transaction.
func (*PostgresService) RecordPayin(ctx context.Context, tx *sql.Tx, request PayinReceiptRequest) error {
	if tx == nil {
		return errors.New("ledger transaction is required")
	}
	if request.PayinID == "" || request.CorrelationID == "" {
		return errors.New("payin ID and correlation ID are required")
	}
	var paymentID, status, currency string
	var amount int64
	if err := tx.QueryRowContext(ctx, `SELECT payment_id,settlement_status,destination_amount_minor,destination_currency FROM payins WHERE id=$1 FOR UPDATE`, request.PayinID).Scan(&paymentID, &status, &amount, &currency); err != nil {
		return fmt.Errorf("lock received payin: %w", err)
	}
	if status != "received" {
		return fmt.Errorf("%w: payin %s cannot record ledger from status %s", ErrInvalidPaymentStatus, request.PayinID, status)
	}
	journalID := "jrn_" + request.PayinID + "_succeeded"
	result, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions(id,payment_id,event_type,occurred_at) VALUES($1,$2,'payin.succeeded',$3) ON CONFLICT(payment_id,event_type) DO NOTHING`, journalID, paymentID, request.At)
	if err != nil {
		return fmt.Errorf("insert payin journal: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect payin journal: %w", err)
	}
	if rows == 1 {
		for _, line := range []struct{ suffix, account, side string }{{"debit", paymentcore.CashOperatingAccount, "debit"}, {"credit", paymentcore.SettlementAccount, "credit"}} {
			if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_code,side,amount_minor,currency) VALUES($1,$2,$3,$4,$5,$6)`, journalID+":"+line.suffix, journalID, line.account, line.side, amount, currency); err != nil {
				return fmt.Errorf("insert payin ledger entry: %w", err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status='succeeded',updated_at=$1 WHERE id=$2`, request.At, paymentID); err != nil {
		return fmt.Errorf("complete payin payment: %w", err)
	}
	if err := paymentcore.NewHistoryService().RecordTimeline(ctx, tx, paymentcore.TimelineRecord{PaymentID: paymentID, PaymentStatus: paymentcore.PaymentStatusSucceeded, Note: "payin ledger recorded", At: request.At}); err != nil {
		return err
	}
	eventID, eventType := "evt_"+request.PayinID+"_succeeded", "payin.succeeded"
	body, _ := json.Marshal(map[string]any{"id": eventID, "type": eventType, "payin_id": request.PayinID, "correlation_id": request.CorrelationID, "occurred_at": request.At, "data": map[string]string{"status": "succeeded"}})
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payin',$6,$7) ON CONFLICT(id) DO NOTHING`, eventID, eventbus.PayinEventsTopic, eventType, eventbus.PayinSucceededVersion, paymentID, body, request.At); err != nil {
		return fmt.Errorf("enqueue payin succeeded: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,'payment.succeeded',$3,$4,'payment',$5,$6) ON CONFLICT(id) DO NOTHING`, eventID+"_payment", eventbus.PaymentEventsTopic, eventbus.PaymentSucceededVersion, paymentID, body, request.At); err != nil {
		return fmt.Errorf("enqueue payment succeeded: %w", err)
	}
	return nil
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
	var amount int64
	var currency string
	if err := tx.QueryRowContext(ctx, `SELECT payment_status,amount_minor,currency FROM payments WHERE id=$1 FOR UPDATE`, request.PaymentID).Scan(&paymentStatus, &amount, &currency); err != nil {
		return fmt.Errorf("lock returned payment: %w", err)
	}
	var settled bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ledger_transactions WHERE payment_id=$1 AND event_type='payment.succeeded' AND ledger_status='posted')`, request.PaymentID).Scan(&settled); err != nil {
		return fmt.Errorf("inspect settlement journal: %w", err)
	}
	if paymentStatus != paymentcore.PaymentStatusSucceeded || !settled {
		return fmt.Errorf("%w: post-success return requires a succeeded payment with a posted settlement journal %s", ErrInvalidPaymentStatus, request.PaymentID)
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
	if err := paymentcore.NewHistoryService().Record(ctx, tx,
		paymentcore.AuditRecord{PaymentID: request.PaymentID, Event: "return_succeeded", Message: request.Reason, At: request.At},
		paymentcore.TimelineRecord{PaymentID: request.PaymentID, PaymentStatus: paymentcore.PaymentStatusSucceeded, Note: request.Reason, At: request.At}); err != nil {
		return err
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
	if updateState && status == to {
		return nil
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
		if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status=$1, updated_at=$2 WHERE id=$3`, to, now, id); err != nil {
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
	timelineStatus := status
	if updateState {
		timelineStatus = to
	}
	if err := paymentcore.NewHistoryService().Record(ctx, tx,
		paymentcore.AuditRecord{PaymentID: id, Event: suffix, Message: message, At: now},
		paymentcore.TimelineRecord{PaymentID: id, PaymentStatus: timelineStatus, Note: note, At: now}); err != nil {
		return err
	}
	return nil
}
