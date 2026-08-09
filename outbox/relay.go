// Package outbox publishes transactionally stored events to the event bus.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"stablerail/eventbus"
)

const (
	defaultBatchSize    = 100
	defaultPollInterval = time.Second
)

// Config controls how much work a relay claims and how often an idle relay polls.
type Config struct {
	BatchSize    int
	PollInterval time.Duration
}

// Relay moves pending rows from PostgreSQL to an event producer. The producer
// is owned by the caller and is not closed by Relay.
type Relay struct {
	db           *sql.DB
	producer     eventbus.Producer
	batchSize    int
	pollInterval time.Duration
	now          func() time.Time
}

func NewRelay(db *sql.DB, producer eventbus.Producer, config Config) (*Relay, error) {
	if db == nil {
		return nil, errors.New("outbox database is required")
	}
	if producer == nil {
		return nil, errors.New("outbox producer is required")
	}
	if config.BatchSize < 0 {
		return nil, errors.New("outbox batch size cannot be negative")
	}
	if config.PollInterval < 0 {
		return nil, errors.New("outbox poll interval cannot be negative")
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	return &Relay{
		db: db, producer: producer, batchSize: config.BatchSize,
		pollInterval: config.PollInterval,
		now:          func() time.Time { return time.Now().UTC() },
	}, nil
}

// RelayOnce publishes at most one pending event per aggregate, up to the
// configured batch size. Rows stay locked until their publication markers are
// committed, allowing multiple relay instances to work without claiming the
// same event concurrently.
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin outbox relay transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT candidate.id, candidate.topic, candidate.event_type,
		       candidate.event_version, candidate.aggregate_id,
		       candidate.aggregate_type, candidate.payload, candidate.occurred_at
		FROM outbox_events AS candidate
		WHERE candidate.published_at IS NULL
		  AND NOT EXISTS (
		      SELECT 1
		      FROM outbox_events AS earlier
		      WHERE earlier.aggregate_type = candidate.aggregate_type
		        AND earlier.aggregate_id = candidate.aggregate_id
		        AND earlier.published_at IS NULL
		        AND earlier.sequence_number < candidate.sequence_number
		  )
		ORDER BY candidate.sequence_number
		FOR UPDATE OF candidate SKIP LOCKED
		LIMIT $1`, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("claim outbox events: %w", err)
	}

	events := make([]pendingEvent, 0, r.batchSize)
	for rows.Next() {
		var pending pendingEvent
		if err := rows.Scan(
			&pending.event.ID, &pending.topic, &pending.event.Type,
			&pending.event.Version, &pending.event.AggregateID,
			&pending.event.AggregateType, &pending.event.Payload,
			&pending.event.OccurredAt,
		); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, pending)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close outbox rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read outbox events: %w", err)
	}

	for _, pending := range events {
		if err := r.producer.Publish(ctx, eventbus.Topic(pending.topic), pending.event); err != nil {
			return 0, fmt.Errorf("publish outbox event %s: %w", pending.event.ID, err)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE outbox_events
			SET published_at = $1
			WHERE id = $2 AND published_at IS NULL`, r.now(), pending.event.ID)
		if err != nil {
			return 0, fmt.Errorf("mark outbox event %s published: %w", pending.event.ID, err)
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("inspect outbox event %s update: %w", pending.event.ID, err)
		}
		if updated != 1 {
			return 0, fmt.Errorf("mark outbox event %s published: updated %d rows", pending.event.ID, updated)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit outbox relay transaction: %w", err)
	}
	return len(events), nil
}

// Run polls until the context is canceled. A batch error is returned so the
// caller can apply its process-level restart or retry policy.
func (r *Relay) Run(ctx context.Context) error {
	for {
		count, err := r.RelayOnce(ctx)
		if err != nil {
			return err
		}
		// A batch contains at most one event per aggregate. Even a partial
		// batch may reveal the next event for those aggregates after commit,
		// so only wait when there was no work at all.
		if count > 0 {
			continue
		}

		timer := time.NewTimer(r.pollInterval)
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

type pendingEvent struct {
	topic string
	event eventbus.Event
}
