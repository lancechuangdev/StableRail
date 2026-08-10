# StableRail

A reference implementation exploring the building blocks of a cross-border payment platform. The repository currently implements an in-memory payment lifecycle; the broader architecture below is a roadmap, not a claim that every component is available today.

## Architecture
```
                        REST/gRPC API
                               │
                     Payment Command Service
                               │
                     Internal Ledger Service
                               │
                Kafka Event Bus (or NATS later)
          ┌────────────┬──────────────┬────────────┐
          │            │              │            │
    Quote Engine   Policy Engine   Settlement   Notification
                                       │
                        Provider Adapter Interface
                      ┌────────┬────────┬────────┐
                      │        │        │
                   Bridge   Circle   MockProvider
                      │
               Blockchain Adapter
                      │
          Ethereum / Base / Sepolia
```

## Components
1. Payment Core

A foundation for the payment lifecycle.

Flow:

```
CreatePayment
↓
PaymentIntent
↓
Process
↓
Settle
↓
Settled
```

Implemented capabilities:

- Payment state machine with created → processing → settled
- Immutable double-entry ledger postings for processing and settlement
- Idempotency-key protection for duplicate submissions
- Audit log for lifecycle events
- Timeline API for payment history
- Concurrency-safe service operations and snapshot-based reads

The original `Service` remains useful for isolated in-memory tests. Phase 2 also provides `PostgresService` for durable payment commands and transactional event creation. No REST/gRPC, provider, blockchain, or reconciliation integration has been implemented yet.

## Running the payment core

From the repository root:

```bash
go test ./...
```

The current implementation lives in the paymentcore package and is covered by unit tests.

For race detection and static analysis:

```bash
go test -race ./...
go vet ./...
```

## Phase 2: distributed payments

Phase 2 is being delivered incrementally. Step 1 adds the Kafka boundary: a versioned event envelope, a producer interface, and a shared Kafka producer that can publish to multiple topics. Create one producer per application process and pass the destination topic to each publish call; do not create a producer per message. Payment state changes are not wired directly to Kafka because that would create a dual-write consistency gap.

Step 2 adds PostgreSQL-backed payment commands and a transactional outbox. `PostgresService` writes the payment, double-entry ledger posting, audit history, timeline, and versioned outbox event in one database transaction. Kafka publication remains outside the request transaction and is performed by the outbox relay.

The initial chart of accounts defines operating cash as an asset and settlement payable as a liability. Payment processing debits operating cash and credits settlement payable, recognizing cash received and the matching obligation. Settlement debits the payable and credits operating cash, clearing both. Each journal transaction contains separate, equal debit and credit lines. Corrections should be represented by reversing journal transactions.

### Phase 2: distributed foundations

1. **Kafka foundation — complete**
   - Shared multi-topic Kafka producer
   - Versioned event envelope
   - Local Kafka development environment
2. **Transactional outbox — complete**
   - PostgreSQL-backed payment commands
   - Atomic payment and outbox writes
   - Persistent double-entry ledger, audit history, and timeline
3. **Outbox relay — complete**
   - Poll pending outbox rows in bounded batches
   - Lock rows safely across multiple worker instances
   - Publish events to Kafka and mark successful rows as published
   - Preserve per-payment event ordering
4. **Retry queue and worker — complete**
   - Record failed delivery attempts
   - Retry transient failures with exponential backoff and jitter
   - Apply configurable attempt and age limits
5. **Dead letter queue — complete**
   - Move permanently failed events to a Kafka DLQ
   - Preserve the original event and failure metadata
   - Provide an operator-controlled redrive path
6. **Consumer inbox — complete**
   - Store consumed event IDs before applying side effects
   - Make duplicate Kafka deliveries harmless
   - Commit inbox records and consumer state changes atomically
7. **Payment saga — complete**
   - Coordinate policy, ledger, and settlement steps through events
   - Persist saga state and correlation identifiers
   - Define timeouts and compensating actions for failed workflows
8. **Event-version evolution — complete**
   - Maintain explicit payload versions per event type
   - Add compatibility tests and consumer upcasters
   - Document rules for backward-compatible schema changes

### Phase 3: runnable application

9. **Payment API and application runtime — planned next**
   - Expose payment creation, lookup, and timeline endpoints
   - Enforce request validation and HTTP idempotency keys
   - Run the API, outbox relay, and saga timeout worker with shared dependencies and graceful shutdown
   - Add health, readiness, configuration, and end-to-end tests
10. **Kafka consumer runtime and core workers — planned**
   - Provide a reusable consumer loop with decoding, inbox processing, offset commits, and graceful shutdown
   - Connect payment events to the saga coordinator
   - Implement policy, ledger, and payment-command handlers so the saga can complete without test doubles
   - Define retryable versus permanent consumer failures

### Phase 4: payment capabilities

11. **Settlement provider boundary — planned**
   - Define provider request, response, status, and error contracts
   - Implement a deterministic mock provider for local and integration testing
   - Consume settlement commands and correlate asynchronous provider results with the payment saga
   - Make provider submission and webhook handling idempotent
12. **Quote and FX lifecycle — planned**
   - Create expiring quotes with source amount, destination amount, rate, and fees
   - Bind accepted quotes to payments so execution uses immutable pricing
   - Add precision, rounding, expiration, and concurrency tests

### Phase 5: operations and recovery

13. **Notifications and external webhooks — planned**
   - Publish customer-facing payment status updates
   - Sign webhook deliveries and retry transient failures
   - Provide delivery history, idempotency, and operator redrive controls
14. **Reconciliation and observability — planned**
   - Compare internal ledger, provider, and settlement records
   - Record discrepancies and support operator resolution workflows
   - Add structured logs, metrics, traces, and alerts across the payment path
15. **Event replay CLI — planned**
   - Select events by topic, type, aggregate, and time range
   - Replay into a separate destination topic by default
   - Support dry runs, checkpoints, rate limits, and resumable execution

### Phase 6: production settlement rails

16. **Production provider and blockchain adapters — planned**
   - Implement one real provider behind the settlement boundary
   - Manage credentials, rate limits, webhooks, and provider-specific failure mapping
   - Add chain submission and confirmation tracking only where the chosen settlement rail requires it

Each step will be implemented and verified independently before work begins on the next one.

### Event schema evolution

`Event.Version` identifies the payload schema for one event type. Current producer versions are named constants in `eventbus/versions.go`; producers must use those constants and increment only the event type whose payload changes. An envelope change does not change a payload version.

Payload changes are backward compatible when existing fields retain their meaning and type, and new fields are optional or have a safe default. Removing or renaming fields, changing their type or meaning, or making an optional field required needs a new version. 

Producers deploy after consumers can accept the new version. Suppose `payment.created` moves from version 1 to version 2:
1. Deploy consumers that understand both v1 and v2.
2. Confirm those consumers are healthy.
3. Deploy the producer that starts publishing v2.
4. Later, retire v1 support once old events can no longer arrive or be replayed.

Consumers that support historical payloads register every sequential upcast to their current model and use the schema-aware inbox constructor:

```go
schemas := eventbus.NewSchemaRegistry()
err := schemas.Register("payment.created", 2, map[int]eventbus.Upcaster{
	1: upcastPaymentCreatedV1ToV2,
})
if err != nil {
	return err
}

processor, err := inbox.NewProcessorWithSchemas(db, schemas)
```

The inbox persists the version actually received for auditability, while the handler receives the upcasted event. Unknown event types, future versions, a missing step in an upcast chain, and invalid upcaster output are rejected. Keep fixtures for every supported historical version and verify that each reaches the current consumer model; `eventbus/schema_test.go` demonstrates this compatibility contract.

Start the local single-node Kafka broker with:

```bash
docker compose up -d kafka
```

Start PostgreSQL and apply the schema with:

```bash
docker compose up -d postgres
psql postgresql://stablerail:stablerail@localhost:5432/stablerail \
  -f migrations/001_payment_core.sql
psql postgresql://stablerail:stablerail@localhost:5432/stablerail \
  -f migrations/002_outbox.sql
psql postgresql://stablerail:stablerail@localhost:5432/stablerail \
  -f migrations/003_consumer_inbox.sql
psql postgresql://stablerail:stablerail@localhost:5432/stablerail \
  -f migrations/004_payment_sagas.sql
```

Create the persistent payment service with the shared connection pool:

```go
db, err := postgresdb.Open(ctx, databaseURL)
if err != nil {
    return err
}
defer db.Close()

payments := paymentcore.NewPostgresService(db)
```

Create and run an outbox relay with the same connection pool and the shared Kafka producer:

```go
relay, err := outbox.NewRelay(db, producer, outbox.Config{
	BatchSize:       100,
	PollInterval:    time.Second,
	InitialBackoff:  time.Second,
	MaxBackoff:      time.Minute,
	MaxAttempts:     10,
	MaxAge:          24 * time.Hour,
	DeadLetterTopic: "stablerail-dead-letter",
})
if err != nil {
	return err
}
if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
	return err
}
```

Multiple relay processes can run concurrently. Pending rows are claimed with `FOR UPDATE SKIP LOCKED`, and only the earliest pending event for each payment is eligible, preserving per-payment order. Delivery is at least once: consumers must tolerate a duplicate if the process stops after Kafka accepts an event but before its `published_at` update commits.

Failed publications are retried with exponential backoff and jitter. Attempt and event-age limits are configurable. Exhausted events are published to the configured Kafka dead-letter topic with the original topic, event envelope, attempt count, error, and failure time, then marked failed. They continue to block later events for the same payment until an operator calls `relay.Redrive(ctx, eventID)`. Redrive clears the failure state and starts a new retry window; it does not bypass normal per-payment ordering.

Consumers can make at-least-once Kafka delivery idempotent with the inbox processor. The handler receives the transaction containing the inbox record, so all consumer state changes must use that transaction:

```go
processor, err := inbox.NewProcessor(db)
if err != nil {
	return err
}

processed, err := processor.Process(ctx, "settlement", event,
	func(ctx context.Context, tx *sql.Tx, event eventbus.Event) error {
		_, err := tx.ExecContext(ctx,
			"UPDATE settlements SET confirmed = TRUE WHERE payment_id = $1",
			event.AggregateID,
		)
		return err
	})
```

`processed` is false for a duplicate event already handled by the named consumer. Different consumers can process the same event independently.

The payment saga coordinator persists the workflow and emits commands through the transactional outbox. Use it as an inbox handler so the consumed event, saga transition, and next command commit together:

```go
coordinator, err := saga.NewCoordinator(db, saga.Config{
	PolicyTimeout:     time.Minute,
	LedgerTimeout:     time.Minute,
	SettlementTimeout: 10 * time.Minute,
})
if err != nil {
	return err
}

_, err = processor.Process(ctx, "payment-saga", event, coordinator.Handle)
```

The workflow emits `policy.evaluate`, `ledger.reserve`, and `settlement.execute` commands. Policy or ledger failure emits `payment.fail`. A settlement failure or timeout emits `ledger.release`; after `ledger.released`, the saga records compensation and emits `payment.fail`. Replies must include the command's `correlation_id` in their payload. Run `coordinator.ExpireOnce(ctx)` periodically to claim overdue sagas safely across multiple workers and initiate failure or compensation.

The default development broker address is `localhost:9092`. Kafka topics are auto-created in this local setup; production environments should provision and configure topics explicitly.
