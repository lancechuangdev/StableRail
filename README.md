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
- Simple per-payment ledger balance tracking
- Idempotency-key protection for duplicate submissions
- Audit log for lifecycle events
- Timeline API for payment history
- Concurrency-safe service operations and snapshot-based reads

The service is intentionally in-memory: data does not survive restarts, IDs are local to a service instance, and no REST/gRPC, event bus, provider, blockchain, or reconciliation integration has been implemented yet.

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
a dual-write consistency gap. The next step will introduce a transactional
outbox and connect payment transactions to event publication safely.

Start the local single-node Kafka broker with:

```bash
docker compose up -d kafka
```

The default development broker address is `localhost:9092`. Kafka topics are
auto-created in this local setup; production environments should provision and
configure topics explicitly.
