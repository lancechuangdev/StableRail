package paymentapi

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"stablerail/paymentcore"
)

// LocalSettlementControl emits provider callbacks for the mock provider. It is
// only mounted by main when the mock provider and operator API are enabled.
type LocalSettlementControl struct{ db *sql.DB }

func NewLocalSettlementControl(db *sql.DB) (*LocalSettlementControl, error) {
	if db == nil {
		return nil, errors.New("local settlement database is required")
	}
	return &LocalSettlementControl{db: db}, nil
}

func (c *LocalSettlementControl) Emit(ctx context.Context, paymentID, status, reason string, delay time.Duration) error {
	eventType := map[string]string{"completed": "settlement.completed", "failed": "settlement.failed", "refunded": "settlement.returned"}[status]
	if eventType == "" {
		return errors.New("status must be completed, failed, or refunded")
	}
	var correlationID string
	if err := c.db.QueryRowContext(ctx, `SELECT correlation_id FROM settlement_sagas WHERE payment_id=$1`, paymentID).Scan(&correlationID); err != nil {
		return err
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]string{"correlation_id": correlationID, "reason": reason})
	now := time.Now().UTC()
	_, err := c.db.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at,next_attempt_at) VALUES($1,$2,$3,1,$4,'payment',$5,$6,$7)`, "evt_"+hex.EncodeToString(b), paymentcore.PaymentEventsTopic, eventType, paymentID, payload, now, now.Add(delay))
	return err
}

func NewLocalSettlementHandler(token string, control *LocalSettlementControl) (http.Handler, error) {
	if token == "" || control == nil {
		return nil, errors.New("operator token and local settlement control are required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validOperatorToken(r, token) {
			problem(w, http.StatusUnauthorized, "valid operator bearer token is required")
			return
		}
		var input struct {
			Status            string `json:"status"`
			Reason            string `json:"reason"`
			DelayMilliseconds int    `json:"delay_milliseconds"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil {
			problem(w, http.StatusBadRequest, "invalid JSON request body")
			return
		}
		if input.DelayMilliseconds < 0 || input.DelayMilliseconds > 10000 {
			problem(w, http.StatusBadRequest, "delay_milliseconds must be between 0 and 10000")
			return
		}
		if err := control.Emit(r.Context(), r.PathValue("id"), input.Status, input.Reason, time.Duration(input.DelayMilliseconds)*time.Millisecond); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				problem(w, http.StatusNotFound, "payment saga not found")
			} else if input.Status != "completed" && input.Status != "failed" && input.Status != "refunded" {
				problem(w, http.StatusBadRequest, err.Error())
			} else {
				problem(w, http.StatusInternalServerError, "could not emit local settlement outcome")
			}
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"payment_id": r.PathValue("id"), "status": input.Status})
	}), nil
}
