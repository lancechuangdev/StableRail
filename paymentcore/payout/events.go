package payout

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"stablerail/eventbus"
)

func (s *Service) enqueue(ctx context.Context, tx *sql.Tx, paymentID, eventType string, payload []byte, now time.Time) error {
	workflowType := map[string]string{"payment.created": "payout.created", "payment.processing": "payout.submitted", "payment.succeeded": "payout.completed"}[eventType]
	if workflowType == "" {
		return fmt.Errorf("unknown payout state event type: %s", eventType)
	}
	workflowID, err := s.newID("evt_")
	if err != nil {
		return fmt.Errorf("generate event ID: %w", err)
	}
	publicID, err := s.newID("evt_")
	if err != nil {
		return fmt.Errorf("generate public event ID: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox_events (id, topic, event_type, event_version, aggregate_id, aggregate_type, payload, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, workflowID, eventbus.PayoutEventsTopic, workflowType, payoutEventVersion(workflowType), paymentID, "payout", payload, now); err != nil {
		return fmt.Errorf("enqueue payout event: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox_events (id, topic, event_type, event_version, aggregate_id, aggregate_type, payload, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, publicID, eventbus.PaymentEventsTopic, eventType, paymentEventVersion(eventType), paymentID, "payment", payload, now); err != nil {
		return fmt.Errorf("enqueue public payment event: %w", err)
	}
	return nil
}

func paymentEventVersion(eventType string) int {
	switch eventType {
	case "payment.created":
		return eventbus.PaymentCreatedVersion
	case "payment.processing":
		return eventbus.PaymentProcessingVersion
	case "payment.succeeded":
		return eventbus.PaymentSucceededVersion
	default:
		panic("unknown payment event type: " + eventType)
	}
}

func payoutEventVersion(eventType string) int {
	switch eventType {
	case "payout.created":
		return eventbus.PayoutCreatedVersion
	case "payout.submitted":
		return eventbus.PayoutSubmittedVersion
	case "payout.completed":
		return eventbus.PayoutCompletedVersion
	default:
		panic("unknown payout event type: " + eventType)
	}
}
