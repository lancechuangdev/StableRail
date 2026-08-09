// Package inbox provides idempotent, transactional event consumption.
package inbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"stablerail/eventbus"
)

// Handler applies an event's consumer-specific state changes. The supplied
// transaction also contains the inbox insert and must not be committed or
// rolled back by the handler.
type Handler func(context.Context, *sql.Tx, eventbus.Event) error

// Processor records consumed events and applies their side effects atomically.
type Processor struct {
	db  *sql.DB
	now func() time.Time
}

func NewProcessor(db *sql.DB) (*Processor, error) {
	if db == nil {
		return nil, errors.New("inbox database is required")
	}
	return &Processor{
		db:  db,
		now: func() time.Time { return time.Now().UTC() },
	}, nil
}

// Process records event for consumer and invokes handler in one transaction.
// It returns false without invoking handler when that consumer has already
// processed the event.
func (p *Processor) Process(ctx context.Context, consumer string, event eventbus.Event, handler Handler) (bool, error) {
	if consumer == "" {
		return false, errors.New("inbox consumer name is required")
	}
	if handler == nil {
		return false, errors.New("inbox handler is required")
	}
	if err := event.Validate(); err != nil {
		return false, fmt.Errorf("validate inbox event: %w", err)
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin inbox transaction: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO inbox_events (
			consumer_name, event_id, event_type, event_version,
			aggregate_id, aggregate_type, occurred_at, received_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (consumer_name, event_id) DO NOTHING`,
		consumer, event.ID, event.Type, event.Version, event.AggregateID,
		event.AggregateType, event.OccurredAt, p.now())
	if err != nil {
		return false, fmt.Errorf("record inbox event %s for %s: %w", event.ID, consumer, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect inbox event %s insert: %w", event.ID, err)
	}
	if inserted == 0 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit duplicate inbox event %s: %w", event.ID, err)
		}
		return false, nil
	}
	if inserted != 1 {
		return false, fmt.Errorf("record inbox event %s: inserted %d rows", event.ID, inserted)
	}

	if err := handler(ctx, tx, event); err != nil {
		return false, fmt.Errorf("handle inbox event %s: %w", event.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit inbox event %s: %w", event.ID, err)
	}
	return true, nil
}
