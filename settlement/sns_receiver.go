package settlement

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"stablerail/eventbus"
	"stablerail/paymentcore"
)

// SNSReceiver authenticates and durably records Circle's AWS SNS envelopes.
type SNSReceiver struct {
	db       *sql.DB
	client   *http.Client
	topicARN string
}

func NewSNSReceiver(db *sql.DB, client *http.Client, topicARN string) (*SNSReceiver, error) {
	if db == nil {
		return nil, errors.New("SNS receiver database is required")
	}
	return &SNSReceiver{db: db, client: client, topicARN: topicARN}, nil
}
func (r *SNSReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, req.Body, 1<<20))
	if err != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	if err = r.Handle(req.Context(), raw); err != nil {
		http.Error(w, "invalid SNS notification", 400)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (r *SNSReceiver) Handle(ctx context.Context, raw []byte) error {
	m, err := DecodeSNS(raw)
	if err != nil {
		return fmt.Errorf("decode SNS message: %w", err)
	}
	if err = m.Verify(ctx, r.client, r.topicARN); err != nil {
		return fmt.Errorf("verify SNS message: %w", err)
	}
	if m.Type == "SubscriptionConfirmation" {
		return ConfirmSNS(ctx, r.client, m.SubscribeURL)
	}
	if m.Type != "Notification" {
		return nil
	}
	var body struct {
		NotificationType string `json:"notificationType"`
		Payout           struct {
			ID        string `json:"id"`
			Status    string `json:"status"`
			ErrorCode string `json:"errorCode"`
		} `json:"payout"`
	}
	if err := json.Unmarshal([]byte(m.Message), &body); err != nil {
		return fmt.Errorf("decode Circle notification: %w", err)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO provider_notifications(provider,notification_id,received_at,payload) VALUES('circle',$1,$2,$3) ON CONFLICT(provider,notification_id) DO NOTHING`, m.MessageID, time.Now().UTC(), payload)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return tx.Commit()
	}
	if body.NotificationType == "payout" && body.Payout.ID != "" {
		status := circleStatus(body.Payout.Status)
		_, err = tx.ExecContext(ctx, `UPDATE settlement_submissions SET status=$1,failure_code=NULLIF($2,''),updated_at=$3 WHERE provider='circle' AND provider_reference=$4`, status, body.Payout.ErrorCode, time.Now().UTC(), body.Payout.ID)
		if err != nil {
			return fmt.Errorf("update Circle payout status: %w", err)
		}
		if status == StatusSucceeded || status == StatusFailed {
			if err := applyPayoutOutcome(ctx, tx, m.MessageID, body.Payout.ID, status, body.Payout.ErrorCode); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func applyPayoutOutcome(ctx context.Context, tx *sql.Tx, notificationID, payoutID string, status Status, reason string) error {
	var paymentID, correlationID string
	if err := tx.QueryRowContext(ctx, `SELECT s.payment_id, p.correlation_id FROM settlement_submissions s JOIN payment_sagas p ON p.payment_id=s.payment_id WHERE s.provider='circle' AND s.provider_reference=$1`, payoutID).Scan(&paymentID, &correlationID); err != nil {
		return fmt.Errorf("find Circle payout payment: %w", err)
	}
	now := time.Now().UTC()
	if status == StatusSucceeded {
		var amount int64
		var currency string
		var state paymentcore.PaymentState
		if err := tx.QueryRowContext(ctx, `SELECT amount_minor,currency,state FROM payments WHERE id=$1 FOR UPDATE`, paymentID).Scan(&amount, &currency, &state); err != nil {
			return err
		}
		if state == paymentcore.StateProcessing {
			if _, err := tx.ExecContext(ctx, `UPDATE payments SET state='settled',updated_at=$1 WHERE id=$2`, now, paymentID); err != nil {
				return err
			}
			journal := paymentID + "_settled"
			if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions(id,payment_id,event_type,occurred_at) VALUES($1,$2,'payment.settled',$3) ON CONFLICT(payment_id,event_type) DO NOTHING`, journal, paymentID, now); err != nil {
				return err
			}
			for _, line := range []struct{ id, account, side string }{{journal + ":debit", paymentcore.SettlementAccount, "debit"}, {journal + ":credit", paymentcore.CashOperatingAccount, "credit"}} {
				if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_code,side,amount_minor,currency) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`, line.id, journal, line.account, line.side, amount, currency); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO payment_audit_events(payment_id,event,message,occurred_at) VALUES($1,'settled','payment settled successfully',$2)`, paymentID, now); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,state,note,occurred_at) VALUES($1,'settled','payment settled',$2)`, paymentID, now); err != nil {
				return err
			}
		}
	}
	eventType := "settlement.completed"
	version := eventbus.SettlementCompletedVersion
	if status == StatusFailed {
		eventType = "settlement.failed"
		version = eventbus.SettlementFailedVersion
		if reason == "" {
			reason = "Circle payout failed"
		}
	}
	payload, _ := json.Marshal(map[string]string{"correlation_id": correlationID, "caused_by_event_id": notificationID, "reason": reason})
	_, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7) ON CONFLICT(id) DO NOTHING`, "evt_circle_"+notificationID, paymentcore.PaymentEventsTopic, eventType, version, paymentID, payload, now)
	return err
}
