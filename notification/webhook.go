// Package notification provides durable, signed tenant webhook delivery.
package notification

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"stablerail/eventbus"
)

var ErrDeliveryNotFailed = errors.New("webhook delivery is not failed")

type Config struct {
	BatchSize                                        int
	PollInterval, InitialBackoff, MaxBackoff, MaxAge time.Duration
	MaxAttempts                                      int
}

type Dispatcher struct {
	db     *sql.DB
	client *http.Client
	config Config
	now    func() time.Time
}

type Delivery struct {
	ID, EndpointID, EventID, PaymentID, EventType, URL, Secret string
	Payload                                                    []byte
	AttemptCount                                               int
	CreatedAt                                                  time.Time
}

func NewDispatcher(db *sql.DB, client *http.Client, c Config) (*Dispatcher, error) {
	if db == nil {
		return nil, errors.New("webhook database is required")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	if c.BatchSize < 0 || c.PollInterval < 0 || c.InitialBackoff < 0 || c.MaxBackoff < 0 || c.MaxAttempts < 0 || c.MaxAge < 0 {
		return nil, errors.New("webhook settings cannot be negative")
	}
	if c.BatchSize == 0 {
		c.BatchSize = 50
	}
	if c.PollInterval == 0 {
		c.PollInterval = time.Second
	}
	if c.InitialBackoff == 0 {
		c.InitialBackoff = time.Second
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = time.Minute
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 10
	}
	if c.MaxAge == 0 {
		c.MaxAge = 24 * time.Hour
	}
	if c.MaxBackoff < c.InitialBackoff {
		return nil, errors.New("webhook maximum backoff cannot be less than initial backoff")
	}
	return &Dispatcher{db: db, client: client, config: c, now: func() time.Time { return time.Now().UTC() }}, nil
}

// EventHandler creates one delivery per active endpoint in the same transaction
// as inbox deduplication. The endpoint/event uniqueness constraint makes replay safe.
func EventHandler() func(context.Context, *sql.Tx, eventbus.Event) error {
	return func(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
		if tx == nil {
			return errors.New("webhook transaction is required")
		}
		switch event.Type {
		case "payment.created", "ledger.reserved", "payment.succeeded", "payment.failed", "payment.funds_returned", "payment.return.succeeded":
		default:
			return nil
		}
		var tenantID string
		if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM payments WHERE id=$1`, event.AggregateID).Scan(&tenantID); err != nil {
			return fmt.Errorf("load webhook tenant: %w", err)
		}
		publicType := map[string]string{"ledger.reserved": "payment.processing"}[event.Type]
		if publicType == "" {
			publicType = event.Type
		}
		body, err := json.Marshal(map[string]any{"id": event.ID, "type": publicType, "payment_id": event.AggregateID, "occurred_at": event.OccurredAt, "data": json.RawMessage(event.Payload)})
		if err != nil {
			return fmt.Errorf("encode webhook payload: %w", err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,endpoint_id,event_id,payment_id,event_type,payload,next_attempt_at,created_at)
			SELECT 'whd_' || md5(id || $1), id, $1, $2, $3, $4, $5, $5 FROM webhook_endpoints WHERE tenant_id=$6 AND active
			ON CONFLICT(endpoint_id,event_id) DO NOTHING`, event.ID, event.AggregateID, publicType, body, event.OccurredAt, tenantID)
		if err != nil {
			return fmt.Errorf("enqueue webhook deliveries: %w", err)
		}
		return nil
	}
}

func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT d.id,d.endpoint_id,d.event_id,COALESCE(d.payment_id,d.payin_id),d.event_type,d.payload,e.url,e.secret,d.attempt_count,d.created_at
		FROM webhook_deliveries d JOIN webhook_endpoints e ON e.id=d.endpoint_id
		WHERE d.status='pending' AND d.next_attempt_at <= $1 AND e.active ORDER BY d.created_at LIMIT $2`, d.now(), d.config.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("claim webhook deliveries: %w", err)
	}
	var deliveries []Delivery
	for rows.Next() {
		var v Delivery
		if err := rows.Scan(&v.ID, &v.EndpointID, &v.EventID, &v.PaymentID, &v.EventType, &v.Payload, &v.URL, &v.Secret, &v.AttemptCount, &v.CreatedAt); err != nil {
			rows.Close()
			return 0, err
		}
		deliveries = append(deliveries, v)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	completed := 0
	for _, v := range deliveries {
		if err := d.deliver(ctx, v); err != nil {
			return completed, err
		}
		completed++
	}
	return completed, nil
}

func (d *Dispatcher) deliver(ctx context.Context, v Delivery) error {
	timestamp := strconv.FormatInt(d.now().Unix(), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.URL, bytes.NewReader(v.Payload))
	if err != nil {
		return d.recordFailure(ctx, v, err.Error(), 0)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "StableRail-Webhooks/1.0")
	req.Header.Set("X-StableRail-Delivery", v.ID)
	req.Header.Set("X-StableRail-Timestamp", timestamp)
	req.Header.Set("X-StableRail-Signature", Signature(v.Secret, timestamp, v.Payload))
	resp, err := d.client.Do(req)
	if err != nil {
		return d.recordFailure(ctx, v, err.Error(), 0)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, err = d.db.ExecContext(ctx, `UPDATE webhook_deliveries SET status='delivered',attempt_count=attempt_count+1,response_status=$1,delivered_at=$2,last_error=NULL WHERE id=$3 AND status='pending'`, resp.StatusCode, d.now(), v.ID)
		return err
	}
	return d.recordFailure(ctx, v, fmt.Sprintf("unexpected HTTP status %d", resp.StatusCode), resp.StatusCode)
}

// Signature returns the value receivers should compare in constant time.
func Signature(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

func (d *Dispatcher) recordFailure(ctx context.Context, v Delivery, message string, status int) error {
	now, attempts := d.now(), v.AttemptCount+1
	terminal := attempts >= d.config.MaxAttempts || now.Sub(v.CreatedAt) >= d.config.MaxAge
	if terminal {
		_, err := d.db.ExecContext(ctx, `UPDATE webhook_deliveries SET status='failed',attempt_count=$1,response_status=NULLIF($2,0),last_error=$3,failed_at=$4 WHERE id=$5 AND status='pending'`, attempts, status, message, now, v.ID)
		return err
	}
	delay := d.config.InitialBackoff
	for i := 1; i < attempts && delay < d.config.MaxBackoff; i++ {
		delay *= 2
		if delay > d.config.MaxBackoff {
			delay = d.config.MaxBackoff
		}
	}
	_, err := d.db.ExecContext(ctx, `UPDATE webhook_deliveries SET attempt_count=$1,response_status=NULLIF($2,0),last_error=$3,next_attempt_at=$4 WHERE id=$5 AND status='pending'`, attempts, status, message, now.Add(delay), v.ID)
	return err
}

func (d *Dispatcher) Redrive(ctx context.Context, id string) error {
	result, err := d.db.ExecContext(ctx, `UPDATE webhook_deliveries SET status='pending',attempt_count=0,next_attempt_at=$1,last_error=NULL,response_status=NULL,failed_at=NULL WHERE id=$2 AND status='failed'`, d.now(), id)
	if err != nil {
		return fmt.Errorf("redrive webhook delivery: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrDeliveryNotFailed
	}
	return nil
}

func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		n, err := d.DispatchOnce(ctx)
		if err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		timer := time.NewTimer(d.config.PollInterval)
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
