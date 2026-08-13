# StableRail

A Go reference implementation of a durable, event-driven payment workflow. The current application exposes an HTTP API, stores payment and ledger state in PostgreSQL, publishes through a transactional outbox, consumes Kafka events with a transactional inbox, and coordinates policy, ledger, and settlement steps with a persisted saga.

Policy approval and settlement use deterministic local implementations in the current phase. A provider boundary and idempotent mock provider are implemented; real provider integrations and blockchain adapters remain roadmap items.

## Current capabilities

- HTTP payment creation, lookup, and timeline endpoints
- HTTP idempotency keys backed by a PostgreSQL uniqueness constraint
- Payment lifecycle: `created → processing → settled`
- Immutable, balanced double-entry ledger postings
- Transactional outbox publication with retry, dead-letter, and redrive support
- Transactional inbox deduplication and manual Kafka offset commits
- Persisted payment saga with timeouts and compensating commands
- Injected policy evaluator, transactional ledger service, and settlement provider boundaries
- Versioned event payloads and consumer upcasting support
- One runnable process with health checks and graceful shutdown

## Target architecture

The following is the broader roadmap, not a claim that every component is implemented:

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
                   Policy Engine   Settlement   Notification
                                       │
                        Provider Adapter Interface
                              │
                         MockProvider
```

## Implemented Phase 3 data flow

Numbered arrows show the initial request followed by the repeating outbox–Kafka–inbox workflow.

```mermaid
flowchart TD
    Client["Client"]

    subgraph API["HTTP API"]
        Handler["paymentapi/handler.go<br/>Handler.create"]
        PaymentService["paymentcore/postgres_service.go<br/>PostgresService.CreatePayment"]
        TimelineHandler["paymentapi/handler.go<br/>Handler.timeline"]
    end

    subgraph Database["PostgreSQL"]
        Payments[("payments")]
        Timeline[("payment_timeline_entries")]
        Outbox[("outbox_events")]
        Inbox[("inbox_events")]
        Sagas[("payment_sagas")]
        Ledger[("ledger_transactions<br/>ledger_entries")]
    end

    subgraph Publication["Outbox publication"]
        Relay["outbox/relay.go<br/>Relay.Run"]
        Producer["eventbus/kafka.go<br/>KafkaProducer.Publish"]
    end

    subgraph Kafka["Kafka"]
        PaymentEvents[["payment-events"]]
        PaymentCommands[["payment-commands"]]
    end

    subgraph SagaRuntime["Saga event consumer"]
        SagaLoop["consumer/consumer.go<br/>Loop.Run"]
        SagaInbox["inbox/bound_processor.go<br/>BoundProcessor"]
        SagaHandler["workers/handlers.go<br/>SagaHandler"]
        Coordinator["saga/coordinator.go<br/>Coordinator.Handle"]
    end

    subgraph CommandRuntime["Core command consumer"]
        CommandLoop["consumer/consumer.go<br/>Loop.Run"]
        CommandInbox["inbox/bound_processor.go<br/>BoundProcessor"]
        Worker["workers/handlers.go<br/>CommandHandler.Handle"]
    end

    Client -->|"1. POST JSON + Idempotency-Key"| Handler
    Handler -->|"2. Validate request"| PaymentService

    PaymentService -->|"3a. Insert state = created"| Payments
    PaymentService -->|"3b. Insert created timeline entry"| Timeline
    PaymentService -->|"3c. Insert payment.created"| Outbox
    PaymentService -->|"4. Return payment"| Handler
    Handler -->|"5. HTTP 201 Created"| Client

    Outbox -->|"6. Claim unpublished event"| Relay
    Relay -->|"7. Pass event to producer"| Producer
    Producer -->|"8a. Publish payment event/reply"| PaymentEvents
    Producer -->|"8b. Publish workflow command"| PaymentCommands

    PaymentEvents -->|"9. Fetch event"| SagaLoop
    SagaLoop -->|"10. Decode and validate"| SagaInbox
    SagaInbox -->|"11a. Insert deduplication record"| Inbox
    SagaInbox -->|"11b. Invoke handler in same transaction"| SagaHandler
    SagaHandler -->|"12. Route supported saga event"| Coordinator

    Coordinator -->|"13a. Create or advance saga"| Sagas
    Coordinator -->|"13b. Insert next command"| Outbox

    PaymentCommands -->|"14. Fetch command"| CommandLoop
    CommandLoop -->|"15. Decode and validate"| CommandInbox
    CommandInbox -->|"16a. Insert deduplication record"| Inbox
    CommandInbox -->|"16b. Invoke worker in same transaction"| Worker

    Worker -->|"17a. Change payment state"| Payments
    Worker -->|"17b. Append timeline entry"| Timeline
    Worker -->|"17c. Write balanced ledger posting"| Ledger
    Worker -->|"17d. Insert saga reply"| Outbox

    Outbox -.->|"18. Repeat steps 6–17 for each workflow stage"| Relay

    Client -->|"19. GET /v1/payments/{id}/timeline"| TimelineHandler
    TimelineHandler -->|"20. Query ordered timeline"| Timeline
    Timeline -->|"21. Return created → processing → settled"| TimelineHandler
    TimelineHandler -->|"22. HTTP 200 timeline"| Client
```

<details>
<summary>Detailed successful-payment transaction sequence</summary>

The shaded, dotted regions represent PostgreSQL transaction boundaries. Kafka offset commits deliberately happen after the associated database transaction commits.

```mermaid
%%{init: {
  "themeCSS": ".rect { stroke-dasharray: 6 4; stroke-width: 1.5px; }"
}}%%
sequenceDiagram
    autonumber

    participant C as Client
    participant API as Payment API
    participant DB as PostgreSQL
    participant R as Outbox Relay
    participant K as Kafka
    participant I as Transactional Inbox
    participant S as Saga Coordinator
    participant W as Core Workers

    C->>API: POST /v1/payments + Idempotency-Key

    rect rgba(220, 235, 255, 0.25)
        Note over API,DB: Payment creation transaction
        API->>DB: Begin transaction
        API->>DB: Insert payment with state created
        API->>DB: Insert created timeline entry
        API->>DB: Insert payment.created into outbox
        API->>DB: Commit transaction
    end

    API-->>C: 201 Created

    rect rgba(240, 240, 240, 0.25)
        Note over R,DB: Outbox publication transaction
        R->>DB: Begin transaction and claim payment.created
        R->>K: Publish payment.created
        R->>DB: Mark outbox row published
        R->>DB: Commit transaction
    end

    K->>I: Deliver payment.created to saga consumer

    rect rgba(225, 245, 225, 0.25)
        Note over I,S: Saga inbox transaction
        I->>DB: Begin transaction
        I->>DB: Insert saga inbox record
        I->>S: Handle payment.created using inbox transaction
        S->>DB: Create payment saga
        S->>DB: Insert policy.evaluate into outbox
        I->>DB: Commit inbox, saga, and outbox atomically
    end

    I->>K: Commit Kafka offset

    rect rgba(240, 240, 240, 0.25)
        Note over R,DB: Outbox publication transaction
        R->>DB: Begin transaction and claim policy.evaluate
        R->>K: Publish policy.evaluate
        R->>DB: Mark outbox row published
        R->>DB: Commit transaction
    end

    K->>I: Deliver policy.evaluate to core-worker consumer

    rect rgba(255, 240, 220, 0.25)
        Note over I,W: Core-worker inbox transaction
        I->>DB: Begin transaction
        I->>DB: Insert core-worker inbox record
        I->>W: Handle policy.evaluate using inbox transaction
        W->>DB: Insert policy.approved into outbox
        I->>DB: Commit inbox and reply atomically
    end

    I->>K: Commit Kafka offset

    rect rgba(240, 240, 240, 0.25)
        Note over R,DB: Outbox publication transaction
        R->>DB: Begin transaction and claim policy.approved
        R->>K: Publish policy.approved
        R->>DB: Mark outbox row published
        R->>DB: Commit transaction
    end

    K->>I: Deliver policy.approved to saga consumer

    rect rgba(225, 245, 225, 0.25)
        Note over I,S: Saga inbox transaction
        I->>DB: Begin transaction
        I->>DB: Insert saga inbox record
        I->>S: Handle policy.approved using inbox transaction
        S->>DB: Saga → awaiting_ledger
        S->>DB: Insert ledger.reserve into outbox
        I->>DB: Commit inbox, saga, and command atomically
    end

    I->>K: Commit Kafka offset

    rect rgba(240, 240, 240, 0.25)
        Note over R,DB: Outbox publication transaction
        R->>DB: Begin transaction and claim ledger.reserve
        R->>K: Publish ledger.reserve
        R->>DB: Mark outbox row published
        R->>DB: Commit transaction
    end

    K->>I: Deliver ledger.reserve to core-worker consumer

    rect rgba(255, 240, 220, 0.25)
        Note over I,W: Core-worker inbox transaction
        I->>DB: Begin transaction
        I->>DB: Insert core-worker inbox record
        I->>W: Handle ledger.reserve using inbox transaction
        W->>DB: Payment → processing
        W->>DB: Insert debit and credit ledger entries
        W->>DB: Insert processing audit event
        W->>DB: Insert processing timeline entry
        W->>DB: Insert ledger.reserved into outbox
        I->>DB: Commit all changes atomically
    end

    I->>K: Commit Kafka offset

    rect rgba(240, 240, 240, 0.25)
        Note over R,DB: Outbox publication transaction
        R->>DB: Begin transaction and claim ledger.reserved
        R->>K: Publish ledger.reserved
        R->>DB: Mark outbox row published
        R->>DB: Commit transaction
    end

    K->>I: Deliver ledger.reserved to saga consumer

    rect rgba(225, 245, 225, 0.25)
        Note over I,S: Saga inbox transaction
        I->>DB: Begin transaction
        I->>DB: Insert saga inbox record
        I->>S: Handle ledger.reserved using inbox transaction
        S->>DB: Saga → awaiting_settlement
        S->>DB: Insert settlement.execute into outbox
        I->>DB: Commit all changes atomically
    end

    I->>K: Commit Kafka offset

    rect rgba(240, 240, 240, 0.25)
        Note over R,DB: Outbox publication transaction
        R->>DB: Begin transaction and claim settlement.execute
        R->>K: Publish settlement.execute
        R->>DB: Mark outbox row published
        R->>DB: Commit transaction
    end

    K->>I: Deliver settlement.execute to core-worker consumer

    rect rgba(255, 240, 220, 0.25)
        Note over I,W: Core-worker inbox transaction
        I->>DB: Begin transaction
        I->>DB: Insert core-worker inbox record
        I->>W: Handle settlement.execute using inbox transaction
        W->>DB: Payment → settled
        W->>DB: Insert clearing ledger entries
        W->>DB: Insert settled audit event
        W->>DB: Insert settled timeline entry
        W->>DB: Insert settlement.completed into outbox
        I->>DB: Commit all changes atomically
    end

    I->>K: Commit Kafka offset

    rect rgba(240, 240, 240, 0.25)
        Note over R,DB: Outbox publication transaction
        R->>DB: Begin transaction and claim settlement.completed
        R->>K: Publish settlement.completed
        R->>DB: Mark outbox row published
        R->>DB: Commit transaction
    end

    K->>I: Deliver settlement.completed to saga consumer

    rect rgba(225, 245, 225, 0.25)
        Note over I,S: Saga inbox transaction
        I->>DB: Begin transaction
        I->>DB: Insert saga inbox record
        I->>S: Handle settlement.completed using inbox transaction
        S->>DB: Saga → completed
        I->>DB: Commit inbox and saga atomically
    end

    I->>K: Commit Kafka offset

    C->>API: GET /v1/payments/{id}/timeline
    API->>DB: Query ordered timeline entries
    DB-->>API: created, processing, settled
    API-->>C: 200 timeline
```

</details>

## Payment core

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

The original `Service` remains useful for isolated in-memory tests. `PostgresService` provides durable payment commands, reads, and transactional event creation. The HTTP API and asynchronous application runtime use the PostgreSQL implementation.

## Testing

From the repository root:

```bash
go test ./...
```

For race detection and static analysis:

```bash
go test -race ./...
go vet ./...
```

Run the opt-in HTTP/PostgreSQL end-to-end test against a migrated disposable database:

```bash
STABLERAIL_E2E_DATABASE_URL=postgresql://stablerail:stablerail@localhost:5432/stablerail \
  go test -v ./paymentapi -run EndToEnd
```

## Delivery roadmap

Phase 2 introduced the Kafka boundary, PostgreSQL persistence, transactional outbox and inbox, saga coordination, and event-version evolution. The runtime creates one shared Kafka producer per process. Payment state changes never publish directly to Kafka because doing so would create a dual-write consistency gap.

`PostgresService` writes payment state, ledger postings, audit history, timeline entries, and versioned outbox events in database transactions. Kafka publication happens outside command transactions and is performed by the outbox relay.

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

9. **Payment API and application runtime — complete**
   - Expose payment creation, lookup, and timeline endpoints
   - Enforce request validation and HTTP idempotency keys
   - Run the API, outbox relay, and saga timeout worker with shared dependencies and graceful shutdown
   - Add health, readiness, configuration, and end-to-end tests
10. **Kafka consumer runtime and core workers — complete**
   - Provide a reusable consumer loop with decoding, inbox processing, offset commits, and graceful shutdown
   - Connect payment events to the saga coordinator
   - Implement policy, ledger, and payment-command handlers so the saga can complete without test doubles
   - Define retryable versus permanent consumer failures

### Phase 4: payment capabilities

11. **Settlement provider boundary — complete**
   - Define provider request, response, status, and error contracts
   - Implement a deterministic mock provider for local and integration testing
   - Consume settlement commands and correlate asynchronous provider results with the payment saga
   - Make provider submission and webhook handling idempotent
12. **Provider-bound quote and FX lifecycle — planned**
   - Create BlindPay payout quotes with exact amounts, rates, fees, and expiration
   - Bind each provider quote to one payment and payout execution
   - Add precision, expiration, idempotency, and concurrency tests

### Phase 5: operations and recovery

13. **Notifications and external webhooks — complete**
   - Publish customer-facing payment status updates
   - Sign webhook deliveries and retry transient failures
   - Provide delivery history, idempotency, and operator redrive controls
14. **Reconciliation and observability — complete**
   - Compare internal ledger, provider, and settlement records
   - Record discrepancies and support operator resolution workflows
   - Add structured logs, metrics, traces, and alerts across the payment path
15. **Event replay CLI — deferred**
   - Select events by topic, type, aggregate, and time range
   - Replay into a separate destination topic by default
   - Support dry runs, checkpoints, rate limits, and resumable execution

### Phase 6: production settlement rails

16. **Production provider and blockchain adapters — planned**
   - Implement one real provider behind the settlement boundary
   - Manage credentials, rate limits, webhooks, and provider-specific failure mapping
   - Add chain submission and confirmation tracking only where the chosen settlement rail requires it

Each step will be implemented and verified independently before work begins on the next one.

### Recommended BlindPay architecture

BlindPay is the planned first production payout rail. StableRail will fund payouts
from a BlindPay-managed wallet associated with the configured customer. This avoids
an application-side approval or signing step: StableRail creates a provider-bound
quote and submits the payout using the managed wallet's address. Managed wallets do
not give the customer direct access to their private keys and may be maintained by a
payment vendor or partner, so their custody and production availability must be
confirmed contractually before launch.

BlindPay's payout quote is the authoritative transactional FX quote. It binds the
recipient bank account, funding network and token, requested amount side, fee policy,
exact sender and receiver amounts, provider quote ID, and five-minute expiration.
StableRail must persist those returned values without recalculating them locally.
StableRail does not currently expose a generic quote service; BlindPay quote support
will be introduced with the payout integration.

```text
BlindPay customer + approved bank account
                    │
                    ▼
          BlindPay payout quote
      (exact amounts, rate, fees, expiry)
                    │
                    ▼
             StableRail payment
                    │
          policy + ledger reservation
                    │
                    ▼
          BlindPay payout submission
       (managed wallet address; no signing)
                    │
          processing / on_hold
                    │
                    ▼
     verified webhook + reconciliation
          │             │             │
      completed       failed       refunded
```

The payout workflow must represent `processing`, `on_hold`, `completed`, `failed`,
and `refunded` separately. `on_hold` is not a transient API failure and must use a
compliance-oriented deadline instead of the normal settlement timeout. `refunded`
is distinct from `failed` because funds were captured and returned. A failed or
stalled payout must be reconciled before StableRail assumes that source funds are
available again.

Provider calls must not occur inside the database transaction that records their
result. StableRail first commits a durable submission attempt, performs the remote
call, and then records the BlindPay payout ID and response. An ambiguous response is
resolved through provider lookup or reconciliation rather than blind resubmission;
each BlindPay quote is single-use.

#### Register the BlindPay webhook URL

`POST /v1/providers/blindpay/webhooks` is an **inbound provider webhook**. It is
not an API that a StableRail client should call. After you deploy StableRail to a
public HTTPS address, register this exact URL in the BlindPay instance dashboard:

```text
https://www.example.com/v1/providers/blindpay/webhooks
```

BlindPay then sends signed `POST` deliveries to that URL for payout events such as
`payout.new`, `payout.update`, and `payout.complete`. Set
`STABLERAIL_BLINDPAY_WEBHOOK_SECRET` to the `whsec_...` secret displayed for that
registered BlindPay webhook endpoint. StableRail uses it to verify the `svix-id`,
`svix-timestamp`, and `svix-signature` headers before it stores or acts on a
delivery.

```text
StableRail client ──► /v1/blindpay/payout-quotes, /v1/payments

BlindPay ──► https://www.example.com/v1/providers/blindpay/webhooks
             (signed payout status notifications)
```

The webhook URL must be reachable by BlindPay over HTTPS. Do not expose the webhook
secret to clients, and do not accept a payout status as final until a verified webhook
or reconciliation confirms it.

#### BlindPay integration steps

1. **Provider client and configuration**
   - Add instance-scoped API key, instance ID, base URL, webhook secret, network,
     token, managed-wallet ID, and managed-wallet address configuration.
   - Implement bounded HTTP timeouts, stable error classification, request
     idempotency where supported, and contract tests against a fake server.
2. **Customer and payout destination references**
   - Store opaque BlindPay customer (`re_...`) and bank-account (`ba_...`) IDs plus
     display-safe metadata; do not copy full bank credentials into StableRail.
   - Require approved KYC/KYB and an approved bank account before creating a payout
     quote.
3. **Provider-bound payout quotes**
   - Add a payout quote interface containing bank account, amount side, network,
     token, `cover_fees`, and optional partner-fee reference.
   - Persist BlindPay's quote ID, exact sender/receiver amounts, commercial and net
     rates, fee components, expiration, and immutable raw response. Decode rates
     without binary floating-point arithmetic.
4. **Managed-wallet funding checks**
   - Store and validate the configured managed-wallet (`bl_...`) ID and address, and
     require its network and token to match every payout quote.
   - Check the provider-reported balance before submission and treat insufficient
     funds as an operational condition rather than repeatedly creating payouts.
5. **Durable payout submission**
   - Commit a unique submission attempt keyed by payment and provider quote before
     calling BlindPay, then submit the quote ID and sender wallet address.
   - Persist the returned payout (`po_...`) ID and map provider errors into retryable,
     permanent, user-action-required, and ambiguous-outcome categories.
6. **Webhook processing**
   - Expose `POST /v1/providers/blindpay/webhooks`; verify the raw body using
     `svix-id`, `svix-timestamp`, and `svix-signature` with a replay tolerance and
     constant-time signature comparison.
   - Deduplicate by `svix-id`, durably store verified payloads, apply monotonic payout
     transitions, and emit internal events through the transactional outbox.
7. **Saga and accounting completion**
   - Extend the saga for submission, processing, compliance hold, completion,
     failure, refund, and manual-review states.
   - Track provider wallet assets and payout funds in transit separately; post final
     settlement only on `completed`, and use distinct accounting for returned funds.
8. **Reconciliation and operations**
   - Poll nonterminal and ambiguous payouts to repair missed webhooks and compare
     provider IDs, amounts, statuses, and return transactions with local records.
   - Alert on expired quotes, prolonged holds, unresolved failures, unknown outcomes,
     and provider/internal balance mismatches.
9. **Verification and rollout**
   - In a development instance, test successful payouts plus BlindPay's `66600`
     failed and `77700` refunded scenarios, webhook replay, expired quotes, duplicate
     commands, lost responses, insufficient balance, and wallet/network mismatch.
   - Run a limited production pilot before enabling general traffic; development
     instances simulate fiat completion and do not validate real bank-rail timing.

#### Refund lifecycle

StableRail distinguishes releasing a reservation for a payout that never settled
from recording funds returned after settlement. Reservation release debits
`settlement:payable` and credits `cash:operating`; post-settlement refund accounting
reverses the settlement journal by debiting `cash:operating` and crediting
`settlement:payable`. The two paths use distinct ledger commands and event types.

After `settlement.completed`, the saga remains in `settling_payment` until the
`payment.settle` command produces `payment.settled`. Refund accounting commands are
retried on timeout, and `payment.refund` is emitted only after the applicable ledger
operation succeeds. The resulting `payment.refunded` event is delivered to active
customer webhook endpoints. Regression tests cover settlement acknowledgement,
both refund accounting paths, timeout retries, early-webhook reconciliation, and
customer refund notification creation.

### Reconciliation and observability

The reconciliation worker periodically compares debit and credit totals for every
ledger transaction and compares the latest provider submission with the durable
payment state. Findings have stable fingerprints in
`reconciliation_discrepancies`: repeated findings update the existing record,
cleared findings resolve automatically, and findings resolved by an operator reopen
if the mismatch remains. Operators can resolve investigated findings with
`reconciliation.Reconciler.Resolve`, which requires an identity and note. Every run
is recorded in `reconciliation_runs` and emits a warning-level structured log when
action is required.

HTTP requests emit JSON logs with method, path, status, duration, and an
`X-Request-ID` correlation value. `GET /metrics` exposes Prometheus-format request,
server-error, and cumulative-duration counters. The runtime interval defaults to one
minute and can be changed with `STABLERAIL_RECONCILIATION_INTERVAL`.

### Webhook delivery

Active rows in `webhook_endpoints` subscribe a customer to payment status updates. The
webhook consumer transactionally creates one delivery per endpoint and event; a
uniqueness constraint makes Kafka replay harmless. The dispatcher sends the stored
JSON body with `X-StableRail-Delivery`, `X-StableRail-Timestamp`, and
`X-StableRail-Signature` headers. The signature is lowercase hexadecimal
`HMAC-SHA256(secret, timestamp + "." + raw_body)`, prefixed with `v1=`.

Non-2xx responses and transport errors use bounded exponential retry. Exhausted
deliveries remain in `webhook_deliveries` with status `failed`, their attempt and
response history, and can be explicitly made pending again through
`notification.Dispatcher.Redrive`. Endpoint secrets are never included in delivery
payloads or history.

## Running the application

### 1. Start infrastructure

Start PostgreSQL and Kafka, then wait for PostgreSQL to report `healthy`:

```bash
docker compose up -d
docker compose ps
```

### 2. Apply database migrations

Use the `psql` client included in the PostgreSQL container; no host installation is required:

```bash
for migration in migrations/*.sql; do
  echo "Applying $migration"
  docker compose exec -T postgres \
    psql -U stablerail -d stablerail \
    -f - < "$migration"
done
```

### 3. Create Kafka topics

Create the Kafka topics before starting StableRail:

```bash
for topic in payment-events payment-commands stablerail-dead-letter; do
  docker compose exec kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1
done
```

Creating the topics explicitly avoids a startup race where consumers receive an empty partition assignment before Kafka auto-creates their topics.

### 4. Start StableRail

```bash
export STABLERAIL_DATABASE_URL=postgresql://stablerail:stablerail@localhost:5432/stablerail
export STABLERAIL_KAFKA_BROKERS=localhost:9092
go run ./cmd/stablerail
```

The API listens on `:8080` by default. `STABLERAIL_HTTP_ADDRESS`, `STABLERAIL_SHUTDOWN_TIMEOUT`, and `STABLERAIL_SAGA_POLL_INTERVAL` override the runtime defaults. The process runs the HTTP server, outbox relay, saga timeout worker, saga event consumer, and core command consumer together and drains them on SIGINT or SIGTERM.

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/payments` | Create a payment; requires `Idempotency-Key` |
| `GET /v1/payments/{id}` | Read the current payment snapshot |
| `GET /v1/payments/{id}/timeline` | Read ordered lifecycle history |
| `GET /healthz` | Check whether the process is running |
| `GET /readyz` | Check whether PostgreSQL is reachable |
| `GET /metrics` | Read Prometheus-format HTTP metrics |

Repeating a request with the same idempotency key returns the original payment. The current implementation does not yet compare the repeated request body with the original body; clients must not reuse a key for a different operation.

### 5. Create and inspect a payment

```bash
curl -i -X POST http://localhost:8080/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: request-123' \
  -d '{"external_reference":"order-123","currency":"USD","amount_minor":2500,"customer_id":"customer-1"}'

curl http://localhost:8080/v1/payments/PAYMENT_ID
curl http://localhost:8080/v1/payments/PAYMENT_ID/timeline
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

The create response initially has state `created`. Policy, ledger, and settlement execute asynchronously; poll the lookup or timeline endpoint to observe `processing` and `settled`.

### 6. Stop the local environment

Stop containers while preserving PostgreSQL data:

```bash
docker compose down
```

Stop containers and permanently delete local data:

```bash
docker compose down --volumes
```

## Design notes

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

### Shared PostgreSQL service

Create the persistent payment service with the shared connection pool:

```go
db, err := postgresdb.Open(ctx, databaseURL)
if err != nil {
    return err
}
defer db.Close()

payments := paymentcore.NewPostgresService(db)
```

### Transactional outbox

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

### Transactional inbox

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

### Saga coordination

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

The default development broker address is `localhost:9092`. Although the local broker permits automatic topic creation, explicitly create the application topics before startup so consumer groups receive their partition assignments. Production environments should provision topics with appropriate partition, replication, retention, and access-control settings.
