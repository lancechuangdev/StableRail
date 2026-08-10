// Package outbox publishes transactionally stored events to the event bus.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"stablerail/eventbus"
)

const (
	defaultBatchSize       = 100
	defaultPollInterval    = time.Second
	defaultInitialBackoff  = time.Second
	defaultMaxBackoff      = time.Minute
	defaultMaxAttempts     = 10
	defaultMaxAge          = 24 * time.Hour
	defaultDeadLetterTopic = eventbus.Topic("stablerail-dead-letter")
)

var ErrEventNotDeadLettered = errors.New("outbox event is not dead-lettered")

// Config controls how much work a relay claims and how often an idle relay polls.
type Config struct {
	BatchSize       int
	PollInterval    time.Duration
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
	MaxAttempts     int
	MaxAge          time.Duration
	DeadLetterTopic eventbus.Topic
}

// Relay moves pending rows from PostgreSQL to an event producer. The producer
// is owned by the caller and is not closed by Relay.
type Relay struct {
	db              *sql.DB
	producer        eventbus.Producer
	batchSize       int
	pollInterval    time.Duration
	initialBackoff  time.Duration
	maxBackoff      time.Duration
	maxAttempts     int
	maxAge          time.Duration
	deadLetterTopic eventbus.Topic
	now             func() time.Time
	jitter          func(time.Duration) time.Duration
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
	if config.InitialBackoff < 0 || config.MaxBackoff < 0 || config.MaxAge < 0 || config.MaxAttempts < 0 {
		return nil, errors.New("outbox retry settings cannot be negative")
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.InitialBackoff == 0 {
		config.InitialBackoff = defaultInitialBackoff
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = defaultMaxBackoff
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = defaultMaxAttempts
	}
	if config.MaxAge == 0 {
		config.MaxAge = defaultMaxAge
	}
	if config.DeadLetterTopic == "" {
		config.DeadLetterTopic = defaultDeadLetterTopic
	}
	if config.MaxBackoff < config.InitialBackoff {
		return nil, errors.New("outbox maximum backoff cannot be less than initial backoff")
	}
	return &Relay{
		db: db, producer: producer, batchSize: config.BatchSize,
		pollInterval:   config.PollInterval,
		initialBackoff: config.InitialBackoff, maxBackoff: config.MaxBackoff,
		maxAttempts: config.MaxAttempts, maxAge: config.MaxAge,
		deadLetterTopic: config.DeadLetterTopic,
		now:             func() time.Time { return time.Now().UTC() },
		jitter: func(delay time.Duration) time.Duration {
			return delay/2 + time.Duration(time.Now().UnixNano()%int64(delay/2+1))
		},
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
		       candidate.aggregate_type, candidate.payload, candidate.occurred_at,
		       candidate.attempt_count, COALESCE(candidate.redriven_at, candidate.created_at)
		FROM outbox_events AS candidate
		WHERE candidate.published_at IS NULL
		  AND candidate.failed_at IS NULL
		  AND candidate.next_attempt_at <= $2
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
		LIMIT $1`, r.batchSize, r.now())
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
			&pending.event.OccurredAt, &pending.attemptCount, &pending.createdAt,
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

	published := 0
	for _, pending := range events {
		if err := r.producer.Publish(ctx, eventbus.Topic(pending.topic), pending.event); err != nil {
			if recordErr := r.recordFailure(ctx, tx, pending, err); recordErr != nil {
				return 0, recordErr
			}
			continue
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
		published++
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit outbox relay transaction: %w", err)
	}
	return published, nil
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
	topic        string
	event        eventbus.Event
	attemptCount int
	createdAt    time.Time
}

func (r *Relay) recordFailure(ctx context.Context, tx *sql.Tx, pending pendingEvent, publishErr error) error {
	now := r.now()
	attempts := pending.attemptCount + 1
	exhausted := attempts >= r.maxAttempts || now.Sub(pending.createdAt) >= r.maxAge
	if exhausted {
		payload, err := json.Marshal(DeadLetterPayload{
			OriginalTopic: pending.topic, OriginalEvent: pending.event,
			AttemptCount: attempts, LastError: publishErr.Error(), FailedAt: now,
		})
		if err != nil {
			return fmt.Errorf("encode dead-letter event %s: %w", pending.event.ID, err)
		}
		deadLetter := eventbus.Event{
			ID: pending.event.ID + ".dlq", Type: "outbox.dead_lettered", Version: eventbus.DeadLetterVersion,
			AggregateID: pending.event.AggregateID, AggregateType: pending.event.AggregateType,
			OccurredAt: now, Payload: payload,
		}
		if err := r.producer.Publish(ctx, r.deadLetterTopic, deadLetter); err != nil {
			return fmt.Errorf("publish outbox event %s to dead-letter topic: %w", pending.event.ID, err)
		}
		_, err = tx.ExecContext(ctx, `UPDATE outbox_events
			SET attempt_count = $1, last_error = $2, failed_at = $3, dlq_published_at = $3
			WHERE id = $4 AND published_at IS NULL`, attempts, publishErr.Error(), now, pending.event.ID)
		if err != nil {
			return fmt.Errorf("mark outbox event %s failed: %w", pending.event.ID, err)
		}
		return nil
	}
	delay := r.initialBackoff
	for i := 1; i < attempts && delay < r.maxBackoff; i++ {
		if delay > r.maxBackoff/2 {
			delay = r.maxBackoff
		} else {
			delay *= 2
		}
	}
	nextAttempt := now.Add(r.jitter(delay))
	_, err := tx.ExecContext(ctx, `UPDATE outbox_events
		SET attempt_count = $1, last_error = $2, next_attempt_at = $3
		WHERE id = $4 AND published_at IS NULL`, attempts, publishErr.Error(), nextAttempt, pending.event.ID)
	if err != nil {
		return fmt.Errorf("schedule outbox event %s retry: %w", pending.event.ID, err)
	}
	return nil
}

// DeadLetterPayload preserves the rejected event and the delivery failure that
// exhausted its retry policy.
type DeadLetterPayload struct {
	OriginalTopic string         `json:"original_topic"`
	OriginalEvent eventbus.Event `json:"original_event"`
	AttemptCount  int            `json:"attempt_count"`
	LastError     string         `json:"last_error"`
	FailedAt      time.Time      `json:"failed_at"`
}

// Redrive makes a dead-lettered event eligible for delivery again. It is an
// explicit operator action; successfully published and pending events cannot
// be redriven.
func (r *Relay) Redrive(ctx context.Context, eventID string) error {
	if eventID == "" {
		return errors.New("outbox event ID is required")
	}
	now := r.now()
	result, err := r.db.ExecContext(ctx, `UPDATE outbox_events
		SET attempt_count = 0, next_attempt_at = $1, last_error = NULL,
		    failed_at = NULL, dlq_published_at = NULL, redriven_at = $1
		WHERE id = $2 AND published_at IS NULL AND failed_at IS NOT NULL`, now, eventID)
	if err != nil {
		return fmt.Errorf("redrive outbox event %s: %w", eventID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect outbox event %s redrive: %w", eventID, err)
	}
	if updated != 1 {
		return fmt.Errorf("%w: %s", ErrEventNotDeadLettered, eventID)
	}
	return nil
}
