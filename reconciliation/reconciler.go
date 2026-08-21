// Package reconciliation compares payment, ledger, and provider records and
// persists discrepancies for operator review.
package reconciliation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var ErrDiscrepancyNotOpen = errors.New("reconciliation discrepancy is not open")

type Config struct{ Interval time.Duration }

type Reconciler struct {
	db       *sql.DB
	interval time.Duration
	now      func() time.Time
	logger   *slog.Logger
}

type Finding struct {
	Fingerprint, Kind, PaymentID string
	Details                      map[string]any
}

func New(db *sql.DB, config Config, logger *slog.Logger) (*Reconciler, error) {
	if db == nil {
		return nil, errors.New("reconciliation database is required")
	}
	if config.Interval < 0 {
		return nil, errors.New("reconciliation interval cannot be negative")
	}
	if config.Interval == 0 {
		config.Interval = time.Minute
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Reconciler{db: db, interval: config.Interval, now: func() time.Time { return time.Now().UTC() }, logger: logger}, nil
}

func (r *Reconciler) RunOnce(ctx context.Context) (int, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return 0, fmt.Errorf("begin reconciliation: %w", err)
	}
	defer tx.Rollback()
	now := r.now()
	var runID int64
	if err = tx.QueryRowContext(ctx, `INSERT INTO reconciliation_runs(started_at) VALUES($1) RETURNING id`, now).Scan(&runID); err != nil {
		return 0, fmt.Errorf("create reconciliation run: %w", err)
	}
	findings, err := find(ctx, tx)
	if err != nil {
		_, _ = tx.ExecContext(ctx, `UPDATE reconciliation_runs SET completed_at=$1,error_message=$2 WHERE id=$3`, r.now(), err.Error(), runID)
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE payins SET reconciliation_status='matched'`); err != nil {
		return 0, fmt.Errorf("mark payins reconciled: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE payouts SET reconciliation_status='matched'`); err != nil {
		return 0, fmt.Errorf("mark payouts reconciled: %w", err)
	}
	for _, f := range findings {
		details, e := json.Marshal(f.Details)
		if e != nil {
			return 0, e
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO reconciliation_discrepancies(fingerprint,kind,payment_id,details,status,first_detected_at,last_detected_at,last_seen_run_id)
		VALUES($1,$2,NULLIF($3,''),$4,'open',$5,$5,$6) ON CONFLICT(fingerprint) DO UPDATE SET kind=EXCLUDED.kind,payment_id=EXCLUDED.payment_id,details=EXCLUDED.details,status='open',last_detected_at=EXCLUDED.last_detected_at,last_seen_run_id=EXCLUDED.last_seen_run_id,resolved_at=NULL,resolved_by=NULL,resolution_note=NULL`, f.Fingerprint, f.Kind, f.PaymentID, details, now, runID)
		if e != nil {
			return 0, fmt.Errorf("record discrepancy: %w", e)
		}
		if f.PaymentID != "" {
			if _, e = tx.ExecContext(ctx, `UPDATE payins SET reconciliation_status='exception' WHERE payment_id=$1`, f.PaymentID); e != nil {
				return 0, e
			}
			if _, e = tx.ExecContext(ctx, `UPDATE payouts SET reconciliation_status='exception' WHERE payment_id=$1`, f.PaymentID); e != nil {
				return 0, e
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE reconciliation_discrepancies SET status='resolved',resolved_at=$1,resolved_by='system',resolution_note='no longer detected' WHERE status='open' AND last_seen_run_id<>$2`, now, runID); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE reconciliation_runs SET completed_at=$1,discrepancy_count=$2 WHERE id=$3`, r.now(), len(findings), runID); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit reconciliation: %w", err)
	}
	if len(findings) > 0 {
		r.logger.WarnContext(ctx, "reconciliation discrepancies detected", "run_id", runID, "discrepancy_count", len(findings))
	} else {
		r.logger.InfoContext(ctx, "reconciliation completed", "run_id", runID, "discrepancy_count", 0)
	}
	return len(findings), nil
}

func find(ctx context.Context, tx *sql.Tx) ([]Finding, error) {
	rows, err := tx.QueryContext(ctx, `SELECT t.id,t.payment_id,
		COALESCE(SUM(CASE WHEN e.side='debit' THEN e.amount_minor ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN e.side='credit' THEN e.amount_minor ELSE 0 END),0)
		FROM ledger_transactions t LEFT JOIN ledger_entries e ON e.transaction_id=t.id GROUP BY t.id,t.payment_id
		HAVING COUNT(e.id)=0 OR COALESCE(SUM(CASE WHEN e.side='debit' THEN e.amount_minor ELSE 0 END),0) <> COALESCE(SUM(CASE WHEN e.side='credit' THEN e.amount_minor ELSE 0 END),0)`)
	if err != nil {
		return nil, fmt.Errorf("compare ledger entries: %w", err)
	}
	var out []Finding
	for rows.Next() {
		var journal, payment string
		var debit, credit int64
		if err := rows.Scan(&journal, &payment, &debit, &credit); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, Finding{"ledger:" + journal, "ledger_imbalance", payment, map[string]any{"journal_id": journal, "debit_minor": debit, "credit_minor": credit}})
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT p.id,p.payment_status,COALESCE(s.status,'missing'),COALESCE(s.provider_reference,'')
		FROM payments p LEFT JOIN LATERAL (
			SELECT status,provider_reference FROM settlement_submissions
			WHERE payment_id=p.id AND provider<>'blindpay' ORDER BY created_at DESC LIMIT 1
		) s ON true
		WHERE s.status IS NOT NULL AND ((p.payment_status='succeeded' AND s.status<>'succeeded') OR (s.status='succeeded' AND p.payment_status<>'succeeded'))`)
	if err != nil {
		return nil, fmt.Errorf("compare provider settlements: %w", err)
	}
	for rows.Next() {
		var payment, paymentStatus, providerStatus, reference string
		if err := rows.Scan(&payment, &paymentStatus, &providerStatus, &reference); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, Finding{"settlement:" + payment, "settlement_status_mismatch", payment, map[string]any{"payment_status": paymentStatus, "settlement_status": providerStatus, "provider_reference": reference}})
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT p.id,p.payment_status,b.settlement_status,COALESCE(b.provider_payout_id,'')
		FROM payments p JOIN payouts b ON b.payment_id=p.id
		WHERE (b.settlement_status='completed' AND p.payment_status<>'succeeded')
		   OR (b.settlement_status='refunded' AND NOT EXISTS (
		       SELECT 1 FROM payment_returns r WHERE r.payment_id=p.id AND r.status='succeeded'
		   ))
		   OR (b.settlement_status='failed' AND p.payment_status<>'failed')`)
	if err != nil {
		return nil, fmt.Errorf("compare BlindPay payout states: %w", err)
	}
	for rows.Next() {
		var payment, paymentStatus, providerStatus, reference string
		if err := rows.Scan(&payment, &paymentStatus, &providerStatus, &reference); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, Finding{"blindpay:" + payment, "blindpay_status_mismatch", payment, map[string]any{"payment_status": paymentStatus, "settlement_status": providerStatus, "provider_reference": reference}})
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, rows.Err()
}

func (r *Reconciler) Resolve(ctx context.Context, id int64, operator, note string) error {
	if id <= 0 || operator == "" || note == "" {
		return errors.New("discrepancy ID, operator, and resolution note are required")
	}
	result, err := r.db.ExecContext(ctx, `UPDATE reconciliation_discrepancies SET status='resolved',resolved_at=$1,resolved_by=$2,resolution_note=$3 WHERE id=$4 AND status='open'`, r.now(), operator, note, id)
	if err != nil {
		return fmt.Errorf("resolve discrepancy: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrDiscrepancyNotOpen
	}
	return nil
}

func (r *Reconciler) Run(ctx context.Context) error {
	for {
		if _, err := r.RunOnce(ctx); err != nil {
			return err
		}
		timer := time.NewTimer(r.interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
