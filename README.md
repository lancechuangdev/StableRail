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

The original `Service` remains useful for isolated in-memory tests. Phase 2 also
provides `PostgresService` for durable payment commands and transactional event
creation. No REST/gRPC, provider, blockchain, or reconciliation integration has
been implemented yet.

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

Phase 2 is being delivered incrementally. Step 1 adds the Kafka boundary: a
versioned event envelope, a producer interface, and a shared Kafka producer
that can publish to multiple topics. Create one producer per application
process and pass the destination topic to each publish call; do not create a
producer per message.
Payment state changes are not wired directly to Kafka because that would create
a dual-write consistency gap.

Step 2 adds PostgreSQL-backed payment commands and a transactional outbox.
`PostgresService` writes the payment, double-entry ledger posting, audit
history, timeline, and versioned outbox event in one database transaction.
Kafka publication remains outside the request transaction and is performed by
the outbox relay.

The initial chart of accounts defines operating cash as an asset and settlement
payable as a liability. Payment processing debits operating cash and credits
settlement payable, recognizing cash received and the matching obligation.
Settlement debits the payable and credits operating cash, clearing both.
Each journal transaction contains separate, equal debit and credit lines.
Corrections should be represented by reversing journal transactions.

### Phase 2 delivery plan

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
5. **Dead letter queue — planned**
   - Move permanently failed events to a Kafka DLQ
   - Preserve the original event and failure metadata
   - Provide an operator-controlled redrive path
6. **Consumer inbox — planned**
   - Store consumed event IDs before applying side effects
   - Make duplicate Kafka deliveries harmless
   - Commit inbox records and consumer state changes atomically
7. **Payment saga — planned**
   - Coordinate policy, ledger, and settlement steps through events
   - Persist saga state and correlation identifiers
   - Define timeouts and compensating actions for failed workflows
8. **Event-version evolution — planned**
   - Maintain explicit payload versions per event type
   - Add compatibility tests and consumer upcasters
   - Document rules for backward-compatible schema changes
9. **Event replay CLI — planned**
   - Select events by topic, type, aggregate, and time range
   - Replay into a separate destination topic by default
   - Support dry runs, checkpoints, rate limits, and resumable execution

Each step will be implemented and verified independently before work begins on
the next one.

Start the local single-node Kafka broker with:

```bash
docker compose up -d kafka
```

Start PostgreSQL and apply the schema with:

```bash
docker compose up -d postgres
psql postgresql://stablerail:stablerail@localhost:5432/stablerail \
  -f migrations/001_payments_and_outbox.sql
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

Create and run an outbox relay with the same connection pool and the shared
Kafka producer:

```go
relay, err := outbox.NewRelay(db, producer, outbox.Config{
    BatchSize:    100,
    PollInterval: time.Second,
	InitialBackoff: time.Second,
	MaxBackoff:     time.Minute,
	MaxAttempts:    10,
	MaxAge:         24 * time.Hour,
})
if err != nil {
    return err
}
if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    return err
}
```

Multiple relay processes can run concurrently. Pending rows are claimed with
`FOR UPDATE SKIP LOCKED`, and only the earliest pending event for each payment
is eligible, preserving per-payment order. Delivery is at least once: consumers
must tolerate a duplicate if the process stops after Kafka accepts an event but
before its `published_at` update commits.

Failed publications are retried with exponential backoff and jitter. Attempt
and event-age limits are configurable; exhausted events remain marked as failed
and block later events for the same payment until the dead-letter workflow is
implemented in the next phase.

The default development broker address is `localhost:9092`. Kafka topics are
auto-created in this local setup; production environments should provision and
configure topics explicitly.
