package payin

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"stablerail/eventbus"
)

func enqueuePublicPaymentEvent(ctx context.Context, tx *sql.Tx, eventID, paymentID, eventType string, version int, payload []byte, now time.Time) error {
	if eventType == "" || version == 0 {
		return fmt.Errorf("invalid public payment event %q", eventType)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7) ON CONFLICT(id) DO NOTHING`, eventID, eventbus.PaymentEventsTopic, eventType, version, paymentID, payload, now)
	return err
}
