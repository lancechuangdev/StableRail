package payout

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"stablerail/paymentcore"
)

func insertHistory(ctx context.Context, tx *sql.Tx, paymentID, event, message string, state paymentcore.PaymentStatus, note string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_audit_events (payment_id, event, message, occurred_at) VALUES ($1, $2, $3, $4)`, paymentID, event, message, now); err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries (payment_id, payment_status, note, occurred_at) VALUES ($1, $2, $3, $4)`, paymentID, state, note, now); err != nil {
		return fmt.Errorf("insert timeline entry: %w", err)
	}
	return nil
}
