# StableRail

A Go reference implementation of a durable, event-driven payment workflow. StableRail
accepts payment intents over HTTP, persists payment and ledger state in PostgreSQL,
publishes events through a transactional outbox, consumes Kafka with a transactional
inbox, and coordinates policy, ledger, BlindPay settlement, refunds, and manual review
through a persisted saga.

The runtime supports both a deterministic mock provider and BlindPay managed-wallet
payouts. BlindPay quotes, durable submission attempts, signed provider webhooks,
ambiguous-outcome recovery, compliance holds, refunds, and provider-state
reconciliation are implemented. Production deployment still requires environment-
specific security, alerting, rate limiting, and rollout controls.

## Current capabilities

- HTTP payment creation, lookup, and timeline endpoints
- Tenant API-key authentication with hashed-at-rest secrets
- HTTP idempotency keys backed by a PostgreSQL uniqueness constraint
- Payment outcomes: `created → processing → settled | failed | refunded`
- Immutable, balanced double-entry ledger postings
- Transactional outbox publication with retry, dead-letter, and redrive support
- Transactional inbox deduplication and manual Kafka offset commits
- Persisted payment saga with timeouts, refund accounting, compliance holds, and manual review
- Injected policy evaluator, transactional ledger service, and settlement provider boundaries
- Versioned event payloads and consumer upcasting support
- BlindPay provider-bound quotes, payout submission, signed webhooks, and recovery workers
- Signed tenant webhooks with retry and redrive
- Authenticated, audited manual-review resolution endpoint
- One runnable process with health checks, metrics, and graceful shutdown

## Architecture

StableRail runs these components in one process today, but their transactional and
message boundaries allow them to be split into independently deployed services later.

```mermaid
flowchart LR
    Client["Payment client"]
    Operator["Operator"]
    BlindPay["BlindPay API + webhooks"]
    Tenant["Tenant webhook endpoint"]

    subgraph StableRail["StableRail process"]
        API["Payment API"]
        OperatorAPI["Operator API"]
        Saga["Saga coordinator<br/>+ timeout worker"]
        Workers["Policy / ledger /<br/>settlement workers"]
        OutboxRelay["Outbox relay"]
        Consumers["Kafka consumers<br/>+ inbox processor"]
        ProviderRecovery["BlindPay submission +<br/>webhook recovery"]
        Reconciler["Reconciliation worker"]
        Notifications["Tenant webhook dispatcher"]
    end

    DB[("PostgreSQL<br/>payments · ledger · sagas<br/>outbox · inbox · provider records")]
    Kafka[["Kafka<br/>payment-events · payment-commands · DLQ"]]

    Client -->|"tenant API key<br/>quotes, payments, reads"| API
    Operator -->|"Bearer token + audited decision"| OperatorAPI
    API --> DB
    OperatorAPI --> Saga
    Saga --> DB
    Workers --> DB
    Consumers --> Saga
    Consumers --> Workers
    DB --> OutboxRelay
    OutboxRelay --> Kafka
    Kafka --> Consumers
    Workers <-->|"idempotent payout request"| BlindPay
    BlindPay -->|"signed payout webhook"| API
    API --> ProviderRecovery
    ProviderRecovery --> DB
    Reconciler --> DB
    Notifications --> Tenant
    DB --> Notifications
```

### Reliability boundaries

```mermaid
flowchart LR
    Producer["Producer transaction"] -->|"business update + event"| Outbox[("outbox_events")]
    Outbox --> Relay["Outbox relay"]
    Relay --> Kafka[["Kafka"]]
    Kafka --> Inbox[("inbox_events")]
    Inbox -->|"same transaction"| Consumer["Consumer side effects"]

    Lost["Prevents DB update<br/>without publication"] -.-> Outbox
    Duplicate["Prevents duplicate<br/>consumer effects"] -.-> Inbox
```

The outbox answers “what must leave this component?” The inbox answers “what has
this named consumer already processed?” Delivery is at least once; deterministic
event IDs, database constraints, and inbox records make retries safe.

## Core payment data flow

Numbered arrows show payment creation followed by the repeating
outbox–Kafka–inbox workflow.

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

## Saga lifecycle

The saga state is operational workflow state; it is deliberately more detailed than
the public payment state. Commands are shown on transition arrows where StableRail
must perform another durable action.

```mermaid
stateDiagram-v2
    [*] --> awaiting_policy: payment.created / policy.evaluate
    awaiting_policy --> awaiting_ledger: policy.approved / ledger.reserve
    awaiting_policy --> failed: policy.rejected / payment.fail
    awaiting_ledger --> awaiting_settlement: ledger.reserved / settlement.execute
    awaiting_ledger --> failed: ledger.failed / payment.fail

    awaiting_settlement --> settling_payment: settlement.completed / payment.settle
    settling_payment --> completed: payment.settled

    awaiting_settlement --> releasing_ledger: settlement.failed or timeout / ledger.release
    releasing_ledger --> ledger_released: ledger.released / payment.fail

    awaiting_settlement --> refunding: settlement.refunded / ledger.release
    refunding --> refunded: ledger.released / payment.refund

    completed --> recording_refund: settlement.refunded / ledger.record_refund
    recording_refund --> refunded: ledger.refund_recorded / payment.refund

    awaiting_settlement --> on_hold: settlement.on_hold
    on_hold --> settling_payment: settlement.completed / payment.settle
    on_hold --> releasing_ledger: settlement.failed / ledger.release
    on_hold --> refunding: settlement.refunded / ledger.release
    on_hold --> manual_review: compliance timeout
    manual_review --> on_hold: operator retry
    manual_review --> settling_payment: operator complete / payment.settle
    manual_review --> releasing_ledger: operator fail / ledger.release
    manual_review --> refunding: operator refund / ledger.release
```

Timeout behavior is intentionally conservative:

- A settlement timeout releases a reservation before failing the payment.
- Settlement-recording and refund-accounting timeouts retry their idempotent command.
- A compliance-hold timeout moves to `manual_review`; it never assumes funds are safe.

## BlindPay payout and webhook flow

The provider API call cannot participate in a PostgreSQL transaction. StableRail
therefore commits a durable submission attempt first and uses the same idempotency key
for recovery if the HTTP outcome is ambiguous.

```mermaid
sequenceDiagram
    autonumber
    participant W as Settlement worker
    participant DB as PostgreSQL
    participant BP as BlindPay
    participant WH as Webhook handler
    participant RR as Recovery workers
    participant K as Kafka

    W->>DB: Commit submission_pending + idempotency key
    W->>BP: POST payout with Idempotency-Key
    alt Response arrives normally
        BP-->>W: po_... + processing/on_hold/terminal
        W->>DB: Persist payout ID and provider status
    else Transport outcome is ambiguous
        W->>DB: Mark payout unknown
        RR->>BP: Retry with original Idempotency-Key
        BP-->>RR: Return original payout
        RR->>DB: Persist payout ID and status
    end

    BP->>WH: Signed payout webhook
    WH->>DB: Store verified delivery by svix-id
    alt payout ID is already associated
        WH->>DB: Update monotonic provider status + insert outbox event
    else webhook arrived before payout ID persistence
        WH->>DB: Commit unmatched verified delivery
        RR->>DB: Find terminal webhook with no derived outbox event
        RR->>DB: Reprocess after payout association is visible
    end
    DB->>K: Outbox relay publishes settlement outcome
```

For a webhook with `svix-id = msg_123`, the derived event ID is
`evt_blindpay_msg_123`. That deterministic mapping prevents reconciliation or webhook
redelivery from creating duplicate internal outcomes.

<details>
<summary>Detailed synchronous mock-provider success sequence</summary>

This expanded sequence shows the deterministic mock provider, which returns success
inside the `settlement.execute` command. BlindPay normally returns a pending result;
its later signed webhook produces `settlement.completed`, after which the saga emits
and awaits `payment.settle` as shown in the state diagram above.

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
        S->>DB: Saga → settling_payment
        S->>DB: Insert payment.settle into outbox
        I->>DB: Commit inbox, saga, and command atomically
    end

    I->>K: Commit Kafka offset

    Note over R,W: payment.settle follows the same outbox → Kafka → inbox path
    W->>DB: Confirm payment state and insert payment.settled into outbox
    K->>I: Deliver payment.settled to saga consumer
    I->>S: Handle payment.settled
    S->>DB: Saga → completed

    C->>API: GET /v1/payments/{id}/timeline
    API->>DB: Query ordered timeline entries
    DB-->>API: created, processing, settled
    API-->>C: 200 timeline
```

</details>

## Payment core

A foundation for the payment lifecycle.

Public payment lifecycle:

```mermaid
stateDiagram-v2
    [*] --> created
    created --> processing: ledger reservation
    processing --> settled: provider completed
    processing --> failed: policy, ledger, or settlement failure
    processing --> refunded: provider returned reserved funds
    settled --> refunded: provider returned settled funds
```

Implemented capabilities:

- Payment state machine with settled, failed, and refunded terminal outcomes
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

Run the focused HTTP/PostgreSQL integration test against a migrated disposable database:

```bash
STABLERAIL_E2E_DATABASE_URL=postgresql://stablerail:stablerail@localhost:5432/stablerail \
  go test -v ./paymentapi -run PostgresPaymentHTTPIntegration
```

Run the provider-free local payment lifecycle suite against the Compose PostgreSQL
and Kafka services:

```bash
./scripts/test-e2e-local.sh
```

Set `STABLERAIL_E2E_KEEP_STACK=1` to retain the isolated PostgreSQL and Kafka
containers after the test for manual inspection.

Run a single scenario by passing normal `go test` arguments to the runner:

```bash
./scripts/test-e2e-local.sh -run '^TestLOCAL001SuccessfulPaymentLifecycle$'
```

The executable scenario specification is documented in
[`docs/testing/local-payment-lifecycle.md`](docs/testing/local-payment-lifecycle.md).

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
   - Define timeouts, reservation release, refund reversal, and manual-review actions
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
12. **Provider-bound quote and FX lifecycle — complete**
   - Create BlindPay payout quotes with exact amounts, rates, fees, and expiration
   - Bind each provider quote to one payment and payout execution
   - Add precision, expiration, idempotency, and concurrency tests

### Phase 5: operations and recovery

13. **Notifications and external webhooks — complete**
   - Publish tenant-facing payment status updates
   - Sign webhook deliveries and retry transient failures
   - Provide delivery history, idempotency, and operator redrive controls
14. **Reconciliation and observability — partial**
   - Compare internal ledger, provider, and settlement records
   - Record discrepancies and support operator resolution workflows
   - Structured HTTP logs, request metrics, and durable discrepancy records are implemented
   - Distributed traces and production alert integrations remain operational follow-up work
15. **Event replay CLI — deferred**
   - Select events by topic, type, aggregate, and time range
   - Replay into a separate destination topic by default
   - Support dry runs, checkpoints, rate limits, and resumable execution

### Phase 6: production settlement rails

16. **Production provider and blockchain adapters — partial**
   - BlindPay managed-wallet payout quotes, submissions, status webhooks, and recovery are implemented
   - Production credential rotation, provider rate limiting, alerting, and rollout controls remain
   - External-wallet chain submission and confirmation tracking remains out of scope for the managed-wallet path

Items marked partial or deferred are the remaining production-hardening roadmap; the
core payment, saga, BlindPay managed-wallet, refund, and manual-review paths are
implemented.

### Recommended BlindPay architecture

BlindPay is the first implemented payout rail. StableRail funds payouts
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
StableRail exposes provider-bound BlindPay payout quotes rather than a generic quote
abstraction.

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

The payout workflow represents `processing`, `on_hold`, `completed`, `failed`,
and `refunded` separately. `on_hold` is not a transient API failure and uses a
compliance-oriented deadline instead of the normal settlement timeout. `refunded`
is distinct from `failed` because funds were captured and returned. A failed or
stalled payout must be reconciled before StableRail assumes that source funds are
available again.

Provider calls must not occur inside the database transaction that records their
result. StableRail first commits a durable submission attempt, performs the remote
call, and then records the BlindPay payout ID and response. An ambiguous response is
resolved by retrying with the original provider idempotency key and reconciling the
result; each BlindPay quote is single-use.

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

#### BlindPay implementation map

1. **Provider client and configuration**
   - Uses an instance-scoped API key, instance ID, base URL, webhook secret, network,
     token, managed-wallet ID, and managed-wallet address configuration.
   - Uses bounded HTTP timeouts, stable error classification, request
     idempotency where supported, and contract tests against a fake server.
2. **Customer and payout destination references**
   - Store opaque BlindPay customer (`re_...`) and bank-account (`ba_...`) IDs plus
     display-safe metadata; do not copy full bank credentials into StableRail.
   - Require approved KYC/KYB and an approved bank account before creating a payout
     quote.
3. **Provider-bound payout quotes**
   - Defines a payout quote interface containing bank account, amount side, network,
     token, `cover_fees`, and optional partner-fee reference.
   - Persists BlindPay's quote ID, exact sender/receiver amounts, commercial and net
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
   - Exposes `POST /v1/providers/blindpay/webhooks`; verifies the raw body using
     `svix-id`, `svix-timestamp`, and `svix-signature` with a replay tolerance and
     constant-time signature comparison.
   - Deduplicates by `svix-id`, durably stores verified payloads, applies monotonic payout
     transitions, and emit internal events through the transactional outbox.
7. **Saga and accounting completion**
   - The saga models submission, processing, compliance hold, completion, failure,
     refund, and manual-review states.
   - Track provider wallet assets and payout funds in transit separately; post final
     settlement only on `completed`, and use distinct accounting for returned funds.
8. **Reconciliation and operations**
   - Retry ambiguous submissions with the original provider idempotency key, repair
     early terminal webhooks, and compare provider payout status with local payment state.
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
tenant webhook endpoints. Regression tests cover settlement acknowledgement,
both refund accounting paths, timeout retries, early-webhook reconciliation, and
tenant refund notification creation.

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

Active rows in `webhook_endpoints` subscribe a tenant to payment status updates. The
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
# Optional: mounts privileged operator endpoints when configured.
export STABLERAIL_OPERATOR_TOKEN='replace-with-a-secret-token'
go run ./cmd/stablerail
```

The API listens on `:8080` by default. `STABLERAIL_HTTP_ADDRESS`, `STABLERAIL_SHUTDOWN_TIMEOUT`, and `STABLERAIL_SAGA_POLL_INTERVAL` override the runtime defaults. The process runs the HTTP server, outbox relay, saga timeout worker, saga event consumer, and core command consumer together and drains them on SIGINT or SIGTERM.

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/payments` | Create a payment; requires `Idempotency-Key` |
| `GET /v1/payments/{id}` | Read the current payment snapshot |
| `GET /v1/payments/{id}/timeline` | Read ordered lifecycle history |
| `POST /v1/webhook-endpoints` | Register a tenant webhook endpoint and return its signing secret once |
| `GET /v1/webhook-endpoints` | List the authenticated tenant's webhook endpoints without secrets |
| `DELETE /v1/webhook-endpoints/{id}` | Disable one of the authenticated tenant's webhook endpoints |
| `POST /v1/operator/tenants/{id}/api-keys` | Issue a tenant API key; requires the operator Bearer token |
| `DELETE /v1/operator/api-keys/{id}` | Revoke a tenant API key; requires the operator Bearer token |
| `POST /v1/operator/payments/{id}/manual-review` | Resolve a held saga; requires the configured operator Bearer token |
| `GET /healthz` | Check whether the process is running |
| `GET /readyz` | Check whether PostgreSQL is reachable |
| `GET /metrics` | Read Prometheus-format HTTP metrics |

Repeating an equivalent request with the same idempotency key returns the original
payment. Reusing the key with different payment fields, destination, or payout quote
returns `409 Conflict`.

### Bootstrap a tenant API key

Payment, payout-quote, and payment-read endpoints require a tenant API key. An
operator creates the first key for a local tenant identifier:

```bash
curl -i -X POST http://localhost:8080/v1/operator/tenants/tenant-1/api-keys \
  -H "Authorization: Bearer $STABLERAIL_OPERATOR_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"marketplace production"}'
```

The response contains an `api_key` beginning with `srk_`. StableRail returns the
secret once and stores only its SHA-256 digest in `tenant_api_keys`; copy it to the
tenant's secret manager. The key determines `tenant_id` for payment and quote
creation. If a request also supplies `tenant_id`, it must match the authenticated
tenant. Payment lookup and timeline endpoints return 404 for another tenant's
payment. Operators can immediately disable a key with
`DELETE /v1/operator/api-keys/{key_id}`.

### Register tenant webhooks

An authenticated tenant may register up to five active HTTPS endpoints. StableRail
returns the `signing_secret` only in the registration response; list responses never
include it. Events are delivered independently to every active endpoint.

```bash
curl -i -X POST http://localhost:8080/v1/webhook-endpoints \
  -H "Authorization: Bearer $STABLERAIL_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://merchant.example/webhooks/stablerail"}'

curl -H "Authorization: Bearer $STABLERAIL_API_KEY" \
  http://localhost:8080/v1/webhook-endpoints
```

Registration rejects non-HTTPS URLs, local hostnames, and private or otherwise
non-public literal IP addresses. Registering the same active URL twice returns
`409 Conflict`. Deleting an endpoint disables future deliveries and is scoped to
the tenant authenticated by the API key.

The manual-review endpoint is mounted only when `STABLERAIL_OPERATOR_TOKEN` is set.
It requires `Authorization: Bearer <token>` and an audited operator decision:

```bash
curl -i -X POST http://localhost:8080/v1/operator/payments/PAYMENT_ID/manual-review \
  -H "Authorization: Bearer $STABLERAIL_OPERATOR_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"action":"refund","operator":"alice@example.com","note":"BlindPay confirmed returned funds"}'
```

Supported actions are `retry` (restart the compliance deadline), `complete` (record
successful settlement), `fail` (release the reservation and fail the payment), and
`refund` (release returned reserved funds and mark the payment refunded). Every
accepted decision is stored in `saga_manual_review_actions` before the saga state and
next outbox command commit atomically.

```mermaid
sequenceDiagram
    participant O as Operator
    participant API as Operator API
    participant DB as PostgreSQL
    participant R as Outbox relay
    participant K as Kafka

    O->>API: POST decision + Bearer token
    API->>API: Authenticate and validate action
    API->>DB: Lock saga in manual_review
    API->>DB: Insert immutable operator audit row
    API->>DB: Update saga + insert next command
    API->>DB: Commit atomically
    API-->>O: 202 Accepted
    DB->>R: Unpublished command
    R->>K: Publish command
```

The shared token is suitable for local or small internal deployments. Production
installations with multiple operators should place the endpoint behind SSO/OIDC or an
authenticating gateway and derive operator identity from a trusted credential rather
than relying on the request body alone.

### 5. Create and inspect a payment

```bash
curl -i -X POST http://localhost:8080/v1/payments \
  -H "Authorization: Bearer $STABLERAIL_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: request-123' \
  -d '{"external_reference":"order-123","currency":"USD","amount_minor":2500}'

curl -H "Authorization: Bearer $STABLERAIL_API_KEY" \
  http://localhost:8080/v1/payments/PAYMENT_ID
curl -H "Authorization: Bearer $STABLERAIL_API_KEY" \
  http://localhost:8080/v1/payments/PAYMENT_ID/timeline
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

The workflow emits `policy.evaluate`, `ledger.reserve`, and `settlement.execute`
commands. Policy or ledger failure emits `payment.fail`. Settlement completion moves
through `settling_payment` until `payment.settled` is acknowledged. A settlement
failure or timeout releases the open reservation before failing the payment. A refund
before settlement releases the reservation; a refund after settlement records a
distinct reversing journal before emitting `payment.refund`. BlindPay `on_hold`
events use a compliance deadline and escalate to `manual_review` without assuming
funds are available. Replies must include the command's `correlation_id`. Run
`coordinator.ExpireOnce(ctx)` periodically to claim overdue sagas safely across
multiple workers and retry or escalate the appropriate operation.

The default development broker address is `localhost:9092`. Although the local broker permits automatic topic creation, explicitly create the application topics before startup so consumer groups receive their partition assignments. Production environments should provision topics with appropriate partition, replication, retention, and access-control settings.
