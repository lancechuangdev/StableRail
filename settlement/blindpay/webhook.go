package blindpay

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"stablerail/eventbus"
	"stablerail/paymentcore"
)

type WebhookVerifier struct {
	secret    []byte
	now       func() time.Time
	tolerance time.Duration
}

func NewWebhookVerifier(secret string) (*WebhookVerifier, error) {
	encoded, ok := strings.CutPrefix(strings.TrimSpace(secret), "whsec_")
	if !ok || encoded == "" {
		return nil, errors.New("BlindPay webhook secret must start with whsec_")
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(key) == 0 {
		return nil, errors.New("invalid BlindPay webhook secret")
	}
	return &WebhookVerifier{secret: key, now: time.Now, tolerance: 5 * time.Minute}, nil
}

func (v *WebhookVerifier) Verify(messageID, timestamp, signatures string, raw []byte) error {
	if messageID == "" || timestamp == "" || signatures == "" {
		return errors.New("missing BlindPay webhook signature headers")
	}
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || seconds <= 0 || absDuration(v.now().Sub(time.Unix(seconds, 0))) > v.tolerance {
		return errors.New("BlindPay webhook timestamp is outside tolerance")
	}
	mac := hmac.New(sha256.New, v.secret)
	_, _ = mac.Write([]byte(messageID + "." + timestamp + "."))
	_, _ = mac.Write(raw)
	expected := mac.Sum(nil)
	for _, candidate := range strings.Fields(signatures) {
		version, encoded, ok := strings.Cut(candidate, ",")
		if !ok || version != "v1" {
			continue
		}
		actual, err := base64.StdEncoding.DecodeString(encoded)
		if err == nil && hmac.Equal(actual, expected) {
			return nil
		}
	}
	return errors.New("invalid BlindPay webhook signature")
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

type PayoutWebhookService struct {
	db  *sql.DB
	now func() time.Time
}

func NewWebhookHandler(verifier *WebhookVerifier, service *PayoutWebhookService) (http.Handler, error) {
	if verifier == nil || service == nil {
		return nil, errors.New("BlindPay webhook verifier and service are required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "invalid webhook body", http.StatusBadRequest)
			return
		}
		messageID, timestamp, signature := r.Header.Get("svix-id"), r.Header.Get("svix-timestamp"), r.Header.Get("svix-signature")
		if err := verifier.Verify(messageID, timestamp, signature, raw); err != nil {
			http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
			return
		}
		if err := service.Process(r.Context(), messageID, raw); err != nil {
			http.Error(w, "could not process webhook", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}), nil
}

func NewPayoutWebhookService(db *sql.DB) (*PayoutWebhookService, error) {
	if db == nil {
		return nil, errors.New("BlindPay webhook database is required")
	}
	return &PayoutWebhookService{db: db, now: func() time.Time { return time.Now().UTC() }}, nil
}

type payoutWebhook struct {
	WebhookEvent string `json:"webhook_event"`
	ID           string `json:"id"`
	Status       string `json:"status"`
}

// Process persists every verified delivery and applies payout status changes once.
func (s *PayoutWebhookService) Process(ctx context.Context, svixID string, raw json.RawMessage) error {
	var payload payoutWebhook
	if err := json.Unmarshal(raw, &payload); err != nil || svixID == "" || payload.WebhookEvent == "" {
		return errors.New("invalid BlindPay webhook payload")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin BlindPay webhook: %w", err)
	}
	defer tx.Rollback()
	now := s.now()
	result, err := tx.ExecContext(ctx, `INSERT INTO blindpay_webhook_events(svix_id,webhook_event,provider_payout_id,payload,received_at) VALUES($1,$2,NULLIF($3,''),$4,$5) ON CONFLICT(svix_id) DO NOTHING`, svixID, payload.WebhookEvent, payload.ID, raw, now)
	if err != nil {
		return fmt.Errorf("persist BlindPay webhook: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect BlindPay webhook: %w", err)
	}
	if rows == 0 || !strings.HasPrefix(payload.WebhookEvent, "payout.") {
		return tx.Commit()
	}
	if !strings.HasPrefix(payload.ID, "po_") || !validPayoutStatus(payload.Status) {
		return errors.New("invalid BlindPay payout webhook")
	}
	var paymentID, current string
	err = tx.QueryRowContext(ctx, `SELECT payment_id,provider_status FROM blindpay_payouts WHERE provider_payout_id=$1 FOR UPDATE`, payload.ID).Scan(&paymentID, &current)
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit() // retain unmatched event for reconciliation.
	}
	if err != nil {
		return fmt.Errorf("lock BlindPay payout webhook target: %w", err)
	}
	if payoutStatusRank(payload.Status) <= payoutStatusRank(current) {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE blindpay_payouts SET provider_status=$1,provider_payload=$2,updated_at=$3 WHERE payment_id=$4`, payload.Status, raw, now, paymentID); err != nil {
		return fmt.Errorf("update BlindPay payout status: %w", err)
	}
	if payload.Status == "completed" || payload.Status == "failed" || payload.Status == "refunded" {
		if err := s.enqueueSagaResult(ctx, tx, svixID, paymentID, payload.Status, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func validPayoutStatus(status string) bool { return payoutStatusRank(status) > 0 }
func payoutStatusRank(status string) int {
	switch status {
	case "processing":
		return 1
	case "on_hold":
		return 2
	case "completed", "failed", "refunded":
		return 3
	default:
		return 0
	}
}

func (s *PayoutWebhookService) enqueueSagaResult(ctx context.Context, tx *sql.Tx, svixID, paymentID, status string, now time.Time) error {
	var correlationID string
	if err := tx.QueryRowContext(ctx, `SELECT correlation_id FROM payment_sagas WHERE payment_id=$1`, paymentID).Scan(&correlationID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // reconciliation will repair an early webhook.
		}
		return fmt.Errorf("get payout saga correlation: %w", err)
	}
	eventType, version, reason := "settlement.failed", eventbus.SettlementFailedVersion, status
	if status == "completed" {
		eventType, version, reason = "settlement.completed", eventbus.SettlementCompletedVersion, ""
	}
	body, err := json.Marshal(map[string]string{"correlation_id": correlationID, "reason": reason})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7) ON CONFLICT(id) DO NOTHING`, "evt_blindpay_"+svixID, paymentcore.PaymentEventsTopic, eventType, version, paymentID, body, now)
	if err != nil {
		return fmt.Errorf("enqueue BlindPay payout outcome: %w", err)
	}
	return nil
}
