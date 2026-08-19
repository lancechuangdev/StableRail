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
	"stablerail/ledger"
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

type WebhookService struct {
	db  *sql.DB
	now func() time.Time
}

func NewWebhookHandler(verifier *WebhookVerifier, service *WebhookService) (http.Handler, error) {
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

func NewWebhookService(db *sql.DB) (*WebhookService, error) {
	if db == nil {
		return nil, errors.New("BlindPay webhook database is required")
	}
	return &WebhookService{db: db, now: func() time.Time { return time.Now().UTC() }}, nil
}

type payoutWebhook struct {
	WebhookEvent string `json:"webhook_event"`
	ID           string `json:"id"`
	Status       string `json:"status"`
}

// Process persists every verified delivery and applies payout status changes once.
func (s *WebhookService) Process(ctx context.Context, svixID string, raw json.RawMessage) error {
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
	_, err = tx.ExecContext(ctx, `INSERT INTO blindpay_webhook_events(svix_id,webhook_event,provider_payout_id,payload,received_at) VALUES($1,$2,NULLIF($3,''),$4,$5) ON CONFLICT(svix_id) DO NOTHING`, svixID, payload.WebhookEvent, payload.ID, raw, now)
	if err != nil {
		return fmt.Errorf("persist BlindPay webhook: %w", err)
	}
	if strings.HasPrefix(payload.WebhookEvent, "payin.") {
		if !strings.HasPrefix(payload.ID, "pi_") {
			return errors.New("invalid BlindPay payin webhook")
		}
		if !validProviderPayinStatus(payload.Status) {
			return errors.New("invalid BlindPay payin status")
		}
		applied, err := s.applyPayinWebhook(ctx, tx, payload.ID, payload.Status, raw, now)
		if err != nil {
			return err
		}
		if applied {
			if _, err := tx.ExecContext(ctx, `INSERT INTO provider_webhook_applications(provider,provider_event_id,operation_type,operation_id,applied_at) SELECT 'blindpay',$1,'payin',id,$2 FROM payins WHERE provider='blindpay' AND provider_payin_id=$3 ON CONFLICT(provider,provider_event_id) DO NOTHING`, svixID, now, payload.ID); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	if !strings.HasPrefix(payload.WebhookEvent, "payout.") {
		return tx.Commit()
	}
	if !strings.HasPrefix(payload.ID, "po_") || !validPayoutStatus(payload.Status) {
		return errors.New("invalid BlindPay payout webhook")
	}
	var paymentID, current string
	err = tx.QueryRowContext(ctx, `SELECT payment_id,provider_status FROM payouts WHERE provider='blindpay' AND provider_payout_id=$1 FOR UPDATE`, payload.ID).Scan(&paymentID, &current)
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit() // retain unmatched event for reconciliation.
	}
	if err != nil {
		return fmt.Errorf("lock BlindPay payout webhook target: %w", err)
	}
	postSuccessReturn := current == "completed" && payload.Status == "refunded"
	if !payoutStatusCanAdvance(current, payload.Status) {
		if current == payload.Status && isTerminalPayoutStatus(payload.Status) {
			if err := s.enqueueSagaResult(ctx, tx, svixID, paymentID, payload.Status, now); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payouts SET provider_status=$1,provider_payload=$2,updated_at=$3 WHERE payment_id=$4`, payload.Status, raw, now, paymentID); err != nil {
		return fmt.Errorf("update BlindPay payout status: %w", err)
	}
	if postSuccessReturn {
		reason := "provider returned funds after completed payout"
		returnID := "ret_blindpay_" + svixID
		if err := ledger.NewPostgresService().RecordReturn(ctx, tx, ledger.ReturnRequest{ID: returnID, PaymentID: paymentID, Provider: "blindpay", ProviderEventID: svixID, Reason: reason, At: now}); err != nil {
			return fmt.Errorf("record post-success BlindPay return: %w", err)
		}
		if err := s.enqueuePaymentReturn(ctx, tx, svixID, returnID, paymentID, reason, now); err != nil {
			return err
		}
		return tx.Commit()
	}
	if payload.Status == "completed" || payload.Status == "failed" || payload.Status == "refunded" || payload.Status == "on_hold" {
		if err := s.enqueueSagaResult(ctx, tx, svixID, paymentID, payload.Status, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func validProviderPayinStatus(status string) bool {
	switch status {
	case "processing", "on_hold", "completed", "failed", "refunded":
		return true
	default:
		return false
	}
}

func (s *WebhookService) applyPayinWebhook(ctx context.Context, tx *sql.Tx, providerID, providerStatus string, raw json.RawMessage, now time.Time) (bool, error) {
	status := string(mapPayinStatus(providerStatus))
	lifecycleStatus := status
	if status == "succeeded" {
		lifecycleStatus = "received"
	}
	var id, paymentID, current, currency, currentFunds string
	var amount int64
	err := tx.QueryRowContext(ctx, `SELECT p.id,p.payment_id,p.status,p.destination_amount_minor,p.destination_currency,pm.funds_status FROM payins p JOIN payments pm ON pm.id=p.payment_id WHERE p.provider='blindpay' AND p.provider_payin_id=$1 FOR UPDATE OF p,pm`, providerID).Scan(&id, &paymentID, &current, &amount, &currency, &currentFunds)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock BlindPay payin: %w", err)
	}
	if current == lifecycleStatus || current == "failed" || current == "refunded" || (current == "succeeded" && lifecycleStatus != "refunded") {
		return true, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payins SET status=$1,provider_payload=$2,instructions=$2,failure_reason=CASE WHEN $1 IN ('failed','refunded') THEN $3 ELSE NULL END,updated_at=$4 WHERE id=$5`, lifecycleStatus, raw, providerStatus, now, id); err != nil {
		return false, fmt.Errorf("update BlindPay payin status: %w", err)
	}
	paymentStatus, fundsStatus := "processing", currentFunds
	if lifecycleStatus == "received" {
		fundsStatus = "received"
	}
	if lifecycleStatus == "failed" {
		paymentStatus = "failed"
	}
	if lifecycleStatus == "refunded" {
		paymentStatus, fundsStatus = "failed", "returned"
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status=$1,funds_status=$2,updated_at=$3 WHERE id=$4`, paymentStatus, fundsStatus, now, paymentID); err != nil {
		return false, fmt.Errorf("update payin payment: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,payment_status,note,occurred_at) VALUES($1,$2,$3,$4)`, paymentID, paymentStatus, "payin."+lifecycleStatus, now); err != nil {
		return false, err
	}
	var correlationID string
	if err := tx.QueryRowContext(ctx, `SELECT correlation_id FROM settlement_sagas WHERE payment_id=$1 AND direction='payin'`, paymentID).Scan(&correlationID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("load payin saga correlation: %w", err)
	}
	eventID := "evt_blindpay_" + providerID + "_" + lifecycleStatus
	eventType := "payin." + lifecycleStatus
	body, _ := json.Marshal(map[string]any{"id": eventID, "type": eventType, "payin_id": id, "correlation_id": correlationID, "reason": providerStatus, "occurred_at": now, "data": map[string]any{"status": lifecycleStatus, "provider_status": providerStatus}})
	if correlationID != "" {
		version := map[string]int{"processing": eventbus.PayinProcessingVersion, "on_hold": eventbus.PayinOnHoldVersion, "received": eventbus.PayinReceivedVersion, "failed": eventbus.PayinFailedVersion, "refunded": eventbus.PayinRefundedVersion}[lifecycleStatus]
		if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payin',$6,$7) ON CONFLICT(id) DO NOTHING`, "evt_payin_saga_"+providerID+"_"+lifecycleStatus, eventbus.PayinEventsTopic, eventType, version, paymentID, body, now); err != nil {
			return false, fmt.Errorf("enqueue payin saga event: %w", err)
		}
	}
	publicType := map[string]string{"processing": "payment.processing", "on_hold": "payment.processing", "received": "payment.funds_status_changed", "failed": "payment.failed", "refunded": "payment.failed"}[lifecycleStatus]
	publicVersion := map[string]int{"payment.processing": eventbus.PaymentProcessingVersion, "payment.failed": eventbus.PaymentFailedVersion, "payment.funds_status_changed": eventbus.PaymentFundsStatusChangedVersion}[publicType]
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7) ON CONFLICT(id) DO NOTHING`, eventID+"_payment", eventbus.PaymentEventsTopic, publicType, publicVersion, paymentID, body, now); err != nil {
		return false, fmt.Errorf("enqueue public payin event: %w", err)
	}
	if lifecycleStatus == "refunded" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,'payment.funds_status_changed',$3,$4,'payment',$5,$6) ON CONFLICT(id) DO NOTHING`, eventID+"_funds", eventbus.PaymentEventsTopic, eventbus.PaymentFundsStatusChangedVersion, paymentID, body, now); err != nil {
			return false, fmt.Errorf("enqueue public payin funds event: %w", err)
		}
	}
	if !(current == "succeeded" && lifecycleStatus == "refunded") {
		return true, nil
	}
	eventType, debit, credit := "payin.refunded", paymentcore.SettlementAccount, paymentcore.CashOperatingAccount
	journalID := "jrn_" + id + "_" + strings.TrimPrefix(eventType, "payin.")
	result, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions(id,payment_id,event_type,occurred_at) VALUES($1,$2,$3,$4) ON CONFLICT(payment_id,event_type) DO NOTHING`, journalID, paymentID, eventType, now)
	if err != nil {
		return false, fmt.Errorf("insert payin journal: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows == 0 {
		return false, err
	}
	for _, line := range []struct{ suffix, account, side string }{{"debit", debit, "debit"}, {"credit", credit, "credit"}} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_code,side,amount_minor,currency) VALUES($1,$2,$3,$4,$5,$6)`, journalID+":"+line.suffix, journalID, line.account, line.side, amount, currency); err != nil {
			return false, fmt.Errorf("insert payin ledger entry: %w", err)
		}
	}
	return true, nil
}

func validPayoutStatus(status string) bool { return payoutStatusRank(status) > 0 }
func isTerminalPayoutStatus(status string) bool {
	return status == "completed" || status == "failed" || status == "refunded"
}

func payoutStatusCanAdvance(current, next string) bool {
	if current == next || current == "failed" || current == "refunded" {
		return false
	}
	if current == "completed" {
		return next == "refunded"
	}
	return payoutStatusRank(next) > payoutStatusRank(current)
}

func (s *WebhookService) enqueuePaymentReturn(ctx context.Context, tx *sql.Tx, svixID, returnID, paymentID, reason string, now time.Time) error {
	body, err := json.Marshal(map[string]string{"return_id": returnID, "payment_status": "succeeded", "funds_status": "consumed", "return_status": "succeeded", "reason": reason})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,'payout.return_completed',$3,$4,'payout',$5,$6) ON CONFLICT(id) DO NOTHING`, "evt_blindpay_"+svixID, eventbus.PayoutEventsTopic, eventbus.PayoutReturnCompletedVersion, paymentID, body, now)
	if err != nil {
		return fmt.Errorf("enqueue post-success payment return: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,'payment.funds_status_changed',$3,$4,'payment',$5,$6) ON CONFLICT(id) DO NOTHING`, "evt_blindpay_"+svixID+"_payment", eventbus.PaymentEventsTopic, eventbus.PaymentFundsStatusChangedVersion, paymentID, body, now)
	return err
}

func payoutStatusRank(status string) int {
	switch status {
	case "submission_pending", "unknown":
		return 1
	case "processing":
		return 2
	case "on_hold":
		return 3
	case "completed", "failed", "refunded":
		return 4
	default:
		return 0
	}
}

// ReconcileOnce retries verified terminal deliveries whose derived saga event
// has not yet been written. This repairs webhooks that arrived before their
// payout or saga record was visible.
func (s *WebhookService) ReconcileOnce(ctx context.Context) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT w.svix_id,w.payload
		FROM blindpay_webhook_events w
		WHERE w.webhook_event LIKE 'payout.%'
		  AND (w.payload->>'status') IN ('completed','failed','refunded')
		  AND NOT EXISTS (SELECT 1 FROM outbox_events o WHERE o.id='evt_blindpay_' || w.svix_id)
		ORDER BY w.received_at LIMIT 100`)
	if err != nil {
		return 0, fmt.Errorf("find unreconciled BlindPay webhooks: %w", err)
	}
	type delivery struct {
		id  string
		raw json.RawMessage
	}
	var deliveries []delivery
	for rows.Next() {
		var d delivery
		if err := rows.Scan(&d.id, &d.raw); err != nil {
			rows.Close()
			return 0, err
		}
		deliveries = append(deliveries, d)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	processed := 0
	for _, d := range deliveries {
		if err := s.Process(ctx, d.id, d.raw); err != nil {
			return processed, err
		}
		processed++
	}
	payinRows, err := s.db.QueryContext(ctx, `SELECT w.svix_id,w.payload FROM blindpay_webhook_events w WHERE w.webhook_event LIKE 'payin.%' AND NOT EXISTS (SELECT 1 FROM provider_webhook_applications a WHERE a.provider='blindpay' AND a.provider_event_id=w.svix_id) AND EXISTS (SELECT 1 FROM payins p WHERE p.provider='blindpay' AND p.provider_payin_id=w.provider_payout_id) ORDER BY w.received_at LIMIT 100`)
	if err != nil {
		return processed, fmt.Errorf("find unapplied payin webhooks: %w", err)
	}
	var payinDeliveries []delivery
	for payinRows.Next() {
		var d delivery
		if err := payinRows.Scan(&d.id, &d.raw); err != nil {
			payinRows.Close()
			return processed, err
		}
		payinDeliveries = append(payinDeliveries, d)
	}
	if err := payinRows.Close(); err != nil {
		return processed, err
	}
	if err := payinRows.Err(); err != nil {
		return processed, err
	}
	for _, d := range payinDeliveries {
		if err := s.Process(ctx, d.id, d.raw); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (s *WebhookService) RunReconciler(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Minute
	}
	for {
		if _, err := s.ReconcileOnce(ctx); err != nil {
			return err
		}
		timer := time.NewTimer(interval)
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

func (s *WebhookService) enqueueSagaResult(ctx context.Context, tx *sql.Tx, svixID, paymentID, status string, now time.Time) error {
	var correlationID string
	if err := tx.QueryRowContext(ctx, `SELECT correlation_id FROM settlement_sagas WHERE payment_id=$1`, paymentID).Scan(&correlationID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // reconciliation will repair an early webhook.
		}
		return fmt.Errorf("get payout saga correlation: %w", err)
	}
	eventType, version, reason := "payout.provider_failed", eventbus.PayoutProviderFailedVersion, status
	if status == "completed" {
		eventType, version, reason = "payout.provider_completed", eventbus.PayoutProviderCompletedVersion, ""
	} else if status == "refunded" {
		eventType, version = "payout.provider_returned", eventbus.PayoutProviderReturnedVersion
	} else if status == "on_hold" {
		eventType, version, reason = "payout.on_hold", eventbus.PayoutOnHoldVersion, "settlement on hold"
	}
	body, err := json.Marshal(map[string]string{"correlation_id": correlationID, "reason": reason})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payout',$6,$7) ON CONFLICT(id) DO NOTHING`, "evt_blindpay_"+svixID, eventbus.PayoutEventsTopic, eventType, version, paymentID, body, now)
	if err != nil {
		return fmt.Errorf("enqueue BlindPay payout outcome: %w", err)
	}
	return nil
}
