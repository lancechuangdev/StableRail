package eventbus

import (
	"encoding/json"
	"errors"
	"time"
)

// Event is the stable envelope shared by all asynchronous StableRail messages.
// Version applies to the payload schema, allowing consumers to evolve safely.
type Event struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Version       int             `json:"version"`
	AggregateID   string          `json:"aggregate_id"`
	AggregateType string          `json:"aggregate_type"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

func (e Event) Validate() error {
	if e.ID == "" || e.Type == "" || e.AggregateID == "" || e.AggregateType == "" {
		return errors.New("event identity fields are required")
	}
	if e.Version < 1 {
		return errors.New("event version must be at least 1")
	}
	if e.OccurredAt.IsZero() {
		return errors.New("event occurrence time is required")
	}
	if !json.Valid(e.Payload) {
		return errors.New("event payload must be valid JSON")
	}
	return nil
}
