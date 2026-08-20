# StableRail

StableRail is a Go reference implementation of a provider-neutral payment orchestration platform. It exposes one HTTP payment model for inbound pay-ins and outbound payouts, keeps payment, double-entry ledger, outbox, and saga state in PostgreSQL, and uses Kafka to coordinate policy checks, accounting, provider execution, compensation, and manual review. Settlement-provider adapters implement the external pay-in and payout operations without changing the public payment API.

## Highlights

- Separate payment and funds statuses so delivery outcome never obscures fund disposition
- Immutable, balanced double-entry ledger postings
- Transactional outbox and inbox with retries, dead-lettering, and redrive
- Persisted sagas with timeouts, provider returns, compliance holds, and audited manual review
- Versioned event payloads with consumer upcasting
- Tenant API keys stored as hashes and HTTP idempotency backed by PostgreSQL
- Signed tenant webhooks with retry and delivery history
- Health, readiness, Prometheus metrics, structured logs, and graceful shutdown

## Architecture

StableRail currently runs as one process, while preserving boundaries that allow its components to be deployed separately later.

```mermaid
flowchart LR
    Client[Client] --> API[Payment API]
    Operator[Operator] --> API
    API --> DB[(PostgreSQL)]
    DB --> Relay[Outbox relay]
    Relay --> Kafka[[Kafka]]
    Kafka --> Consumers[Inbox consumers]
    Consumers --> Saga[Saga and workers]
    Saga --> DB
    Saga <-->|Generic pay-in/payout contract| Provider[Settlement provider]
    Provider --> External[External provider adapter]
    External -->|Signed webhook| API
    DB --> Webhooks[Tenant webhooks]
```

Every business update and outgoing event commits in one PostgreSQL transaction. Consumers record an event in their inbox and apply its effects in another single transaction. Delivery is at least once; deterministic event IDs, uniqueness constraints, and inbox records make retries safe.

### Package ownership

| Package | Responsibility |
| --- | --- |
| `paymentapi` | HTTP transport, authentication, and tenant/operator endpoints |
| `paymentcore` | Shared payment, funds, refund, ledger, and destination models, plus direction-neutral payment queries |
| `paymentcore/payin` | Inbound quote and operation model, saga coordinator, provider-result persistence, ledger completion, and recovery |
| `paymentcore/payout` | Outbound payment creation, refunds, quotes, saga coordination, provider execution, payout persistence, and recovery |
| `ledger` | Transactional double-entry reservations, releases, and provider-return journals |
| `policy` | Payment policy evaluation contracts |
| `reconciliation` | Comparison of payment, ledger, and provider records plus discrepancy resolution |
| `eventbus` | Shared event envelopes, topic and version contracts, and Kafka producer/consumer adapters |
| `outbox` | Transactional event publication, retry and dead-letter handling, and operator redrive |
| `settlement` | Provider-neutral payout/pay-in contracts and the deterministic mock provider |
| `settlement/blindpay` | BlindPay client, resource mapping, quote/execution adapters, and webhooks |
| `workers` | Policy, ledger, and provider command execution plus runtime timeout polling for both settlement directions |

`paymentcore` owns the payment and funds lifecycle, with `payin` and `payout` handling their direction-specific workflows. `eventbus` defines shared events and routing, `workers` executes asynchronous work, and `settlement` provides one interface for provider pay-in and payout capabilities.

Pay-in and payout services manage quotes, provider execution, and safe recovery from uncertain outcomes. Provider adapters translate external API responses and webhooks into consistent payment, ledger, and merchant-notification updates.

### Kafka topics

All application topic names are defined in `eventbus/topics.go`. Pay-in and payout topics carry internal workflow events; `payment-events` carries the stable merchant-facing lifecycle shared by both directions.

| Topic | Constant | Producers | Consumers | Purpose |
| --- | --- | --- | --- | --- |
| `payout-events` | `eventbus.PayoutEventsTopic` | Payout services, workers, and provider webhooks | Payout saga coordinator | Internal payout workflow facts |
| `payin-events` | `eventbus.PayinEventsTopic` | Pay-in services, workers, and provider webhooks | Pay-in saga coordinator | Internal pay-in workflow facts |
| `payment-events` | `eventbus.PaymentEventsTopic` | Pay-in and payout workflows | Tenant-webhook dispatcher | Merchant-facing payment and funds lifecycle facts |
| `settlement-commands` | `eventbus.SettlementCommandsTopic` | Pay-in and payout saga coordinators | Policy, ledger, and provider command workers | Durable workflow commands for both settlement directions |
| `stablerail-dead-letter` | `eventbus.DeadLetterTopic` | Outbox relay | Operator inspection and redrive tooling | Events that exhausted outbox publication retries or exceeded the retry age |

Workflow events use direction-specific names such as `payout.created`, `payout.provider_completed`, `payin.created`, and `payin.received`. Merchant integrations receive only the shared `payment.created`, `payment.processing`, `payment.succeeded`, `payment.failed`, and `payment.funds_status_changed` events.

## Payment lifecycle

The API exposes two independent dimensions. Payment status has the same shape in both directions, while funds status describes different inbound and outbound dispositions:

```mermaid
stateDiagram-v2
    state "Payment status" as payment {
        [*] --> created
        created --> processing: submitted
        created --> failed: rejected before submission
        processing --> succeeded: recipient paid
        processing --> failed: settlement unsuccessful
    }

    state "Payout funds status" as payout_funds {
        [*] --> available
        available --> reserved: ledger reservation
        reserved --> consumed: payout succeeded
        reserved --> available: pre-capture failure confirmed
        reserved --> reserved: payout failed, disposition unresolved
        reserved --> returned: provider returned captured funds
    }

    state "Pay-in funds status" as payin_funds {
        [*] --> pending
        pending --> received: provider confirms receipt
        pending --> pending: fails before receipt
        received --> received: ledger succeeds or disposition is unresolved
        received --> returned: provider confirms refund
    }
```

Common combinations are:

| `payment_status` | `funds_status` | Meaning |
| --- | --- | --- |
| `created` | `available` | Payout recorded but not reserved |
| `processing` | `reserved` | Funds committed while the payout runs |
| `succeeded` | `consumed` | Recipient payout completed |
| `failed` | `available` | Payout failed before capture |
| `failed` | `reserved` | Payout failed but disposition remains unresolved |
| `failed` | `returned` | Provider returned captured payout funds |
| `created` | `pending` | Pay-in recorded before provider execution |
| `processing` | `pending` | Pay-in is awaiting incoming funds |
| `processing` | `received` | Provider received funds and ledger posting is pending |
| `succeeded` | `received` | Pay-in funds were received and recorded |
| `failed` | `pending` | Pay-in failed before funds were received |
| `failed` | `received` | Pay-in failed while received funds remain unresolved |
| `failed` | `returned` | Provider confirmed that pay-in funds were refunded |

Before success, BlindPay's external `refunded` payout status maps to `payment_status=failed` and `funds_status=returned`. After success, it creates a separate `payment_returns` record and reversal journal while the original payment remains `payment_status=succeeded` and `funds_status=consumed`.

An ambiguous provider submission remains `payment_status=processing` and `funds_status=reserved`. The uncertainty is recorded on `payouts.provider_status=unknown` until idempotent recovery or a webhook establishes the outcome.

### Post-success returns

A bank or provider can return funds after a payout was already confirmed. StableRail records that as a separate financial operation:

```text
Payment: created -> processing -> succeeded
Return:  created -> processing -> succeeded | failed
```

The return status domain contains only `created`, `processing`, `succeeded`, and `failed`. The current provider webhook path learns about a return after it has completed externally, so it creates the return directly as `succeeded`; initiated `created` and `processing` transitions are not implemented yet. The return journal debits `cash:operating` for the asset received back and credits `settlement:payable` to restore the obligation. The original payment is not rewritten. StableRail emits an internal `payout.return_completed` event and a merchant-facing `payment.funds_status_changed` event.

Merchant-issued refunds are separate linked payments. `POST /v1/payments/{id}/refunds` accepts an idempotency key, a positive amount, a reason, and an optional fresh `payout_quote_id` for BlindPay routing. Partial refunds are supported up to the original payment amount. StableRail creates a new payment, links it through `payment_refunds.refund_payment_id`, binds the payout quote when supplied, and emits `payout.created` for workflow coordination and `payment.created` for merchant notification. From there, policy, ledger reservation, settlement, and failure handling use the ordinary payout workflow; no refund-specific provider operation or reversal journal is involved. The refund response contains `refund_payment_id` but no duplicated status; clients query `GET /v1/payments/{refund_payment_id}` for its payment and funds statuses. The original payment remains `succeeded/consumed`. Provider-originated returns remain separate and continue to use `payment_returns` and reversal accounting.

## Payout saga lifecycle

The persisted payout saga tracks internal workflow progress in more detail than the public payment and funds statuses:

```mermaid
stateDiagram-v2
    state "funds_returned (saga)" as returned
    [*] --> awaiting_policy: payout.created
    awaiting_policy --> awaiting_ledger: payout.policy.approved / ledger.reserve
    awaiting_policy --> failed: payout.policy.rejected / payment.fail
    awaiting_ledger --> awaiting_settlement: payout.funds_reserved / settlement.execute
    awaiting_ledger --> failed: payout.ledger_failed / payment.fail

    awaiting_settlement --> settling_payment: payout.provider_completed / payment.settle
    settling_payment --> completed: payout.completed

    awaiting_settlement --> failed: payout.provider_failed or timeout / payment.fail_reserved
    awaiting_settlement --> failed: submission_failed / payment.fail

    awaiting_settlement --> returning: payout.provider_returned / ledger.release
    failed --> returning: late payout.provider_returned / ledger.release
    returning --> returned: payout.funds_released / payment.return

    awaiting_settlement --> on_hold: payout.on_hold
    on_hold --> settling_payment: payout.provider_completed / payment.settle
    on_hold --> failed: payout.provider_failed / payment.fail_reserved
    on_hold --> returning: payout.provider_returned / ledger.release
    on_hold --> manual_review: compliance timeout
    manual_review --> on_hold: operator retry
    manual_review --> settling_payment: operator complete / payment.settle
    manual_review --> failed: operator fail / payment.fail_reserved
    manual_review --> returning: operator return / ledger.release
```

The saga's `funds_returned` label is persisted internally as `returned`. It is not a payment status: the resulting payment remains `payment_status=failed` while `funds_status=returned`. The transition from `failed` handles a provider return that arrives after a reserved-funds failure was already recorded.

Timeout handling is conservative: settlement timeouts fail the payment but preserve its reservation, compliance timeouts require manual review, and ambiguous submissions remain in processing until recovery or reconciliation establishes an outcome. A reservation becomes available only after a confirmed pre-capture failure, and becomes returned only after the provider confirms the funds came back.

## Pay-in saga lifecycle

The pay-in coordinator uses the same `settlement_sagas` table with `direction=payin`, but has direction-specific states and commands:

```mermaid
stateDiagram-v2
    [*] --> awaiting_policy: payin.created
    awaiting_policy --> awaiting_execution: payin.policy.approved / payin.execute
    awaiting_policy --> failed: rejection or timeout / payin.fail
    awaiting_execution --> processing: payin.processing
    awaiting_execution --> on_hold: payin.on_hold
    awaiting_execution --> awaiting_ledger: payin.received / payin.ledger.record
    awaiting_execution --> awaiting_execution: execution timeout / retry payin.execute
    processing --> processing: provider polling or retry
    processing --> on_hold: payin.on_hold
    processing --> awaiting_ledger: payin.received / payin.ledger.record
    on_hold --> awaiting_ledger: payin.received / payin.ledger.record
    on_hold --> failed: compliance timeout / payin.fail
    awaiting_ledger --> completed: payin.succeeded
    awaiting_ledger --> awaiting_ledger: ledger timeout / retry payin.ledger.record
    awaiting_ledger --> failed: payin.failed
    awaiting_execution --> failed: payin.failed
    processing --> failed: payin.failed
    on_hold --> failed: payin.failed
    awaiting_execution --> refunded: payin.refunded
    processing --> refunded: payin.refunded
    on_hold --> refunded: payin.refunded
    awaiting_ledger --> refunded: payin.refunded
    completed --> refunded: payin.refunded / reverse ledger
```

Pay-in policy and compliance waits fail on timeout. Provider execution and ledger commands are retried with their original idempotent operation identity. Each active pay-in state stores a deadline, and the timeout worker claims only `direction=payin` rows; the payout timeout worker similarly claims only `direction=payout` rows.

## Pay-ins and payouts

Pay-ins and payouts are directions of the same public payment resource. Create a direction-aware quote with `POST /v1/payment-quotes` when pricing, FX, fees, or provider routing must be locked, then create the operation with `POST /v1/payments`. Both directions support quoted creation; they may also use direct routing when the provider supports it. A direct pay-in supplies its amount, currency, funding method, and destination account on the payment request. BlindPay still requires a provider quote, so its adapter creates that quote internally before executing a direct pay-in. Creating either direction persists a `created` payment and a transactional outbox event. A dedicated pay-in coordinator sends `payin.execute` through Kafka only after `payin.policy.evaluate` is approved, and the command worker performs the provider call. Provider confirmation moves the pay-in to `received`; the saga then sends `payin.ledger.record`, and only a successful balanced journal advances the pay-in to `succeeded`. Provider instructions such as an ACH memo, bank details, Pix code, or CLABE become available asynchronously; retrieve current state with `GET /v1/payments/{id}`.

Both direction-specific coordinators store orchestration state in `settlement_sagas`, keyed by payment ID and direction. The provider-facing `payins` and `payouts` tables remain separate because their execution details and provider statuses differ; they are not separate public API resources.

The generic `payin.Provider` boundary uses opaque source-instrument and destination-account IDs resolved through `provider_resources`; provider wallet and bank-account identifiers do not appear in the shared contract. Verified `payin.*` webhooks can advance an executed pay-in to `processing`, `on_hold`, `received`, `failed`, or `refunded`. The saga turns `received` into `succeeded` only after the ledger command debits `cash:operating` and credits `settlement:payable`. A refund after that successful journal posts the inverse journal; a refund before ledger completion has no completed pay-in journal to reverse. Early webhooks are retained and reconciled after the local pay-in becomes visible.

Pay-in and payout records own their source, destination, method, and monetary snapshot. At the generic data-model level, a quote is an optional commercial attachment that locks fees, FX, and source/destination amounts. Accepting a quote copies those terms into the operation, so lifecycle processing and accounting never depend on joining back to the quote. The schema permits providers that execute without a quote. The current pay-in HTTP workflow and BlindPay adapter remain quote-first.

## Provider resources

Shared APIs and workflow tables use provider-resource IDs instead of naming provider-specific bank-account or wallet fields:

```text
Payout: source account -> destination payment instrument
Pay-in: optional source payment instrument -> destination account
```

An account represents a balance-holding resource, such as a managed wallet. A payment instrument represents an external routing endpoint, such as a bank account or blockchain address. `provider_resources` maps those stable IDs to a provider and its reference. For example:

```text
acct_123       -> blindpay / managed wallet / bl_...
instrument_456 -> blindpay / bank account / ba_...
```

Callers must treat resource IDs as opaque; the current BlindPay reference sync may reuse a provider reference as the local resource ID for compatibility, but the adapter still resolves it through `provider_resources`. Adding another provider requires new resource mappings and an adapter, not new columns in `payins`, `payouts`, or their quote tables. Raw provider responses are isolated in `provider_payload`, while raw webhook events remain adapter-owned data.

## Database migrations

Migrations are ordered by dependency. Provider-neutral platform and workflow schema is created before the BlindPay-specific adapter schema.

| Migration | Main purpose |
| --- | --- |
| [001_payment_core.sql](migrations/001_payment_core.sql) | Payments, shared quotes and provider resources, audit/timeline, destinations, and refunds |
| [002_eventing.sql](migrations/002_eventing.sql) | Transactional outbox and consumer inbox |
| [003_payment_workflow.sql](migrations/003_payment_workflow.sql) | Direction-aware settlement sagas, manual review actions, and settlement submission records |
| [004_payouts.sql](migrations/004_payouts.sql) | Payout provider operations linked to payments |
| [005_payins.sql](migrations/005_payins.sql) | Pay-in provider operations linked to payments |
| [006_ledger.sql](migrations/006_ledger.sql) | Accounts, balanced payment journals, and payment returns |
| [007_webhooks.sql](migrations/007_webhooks.sql) | Merchant webhook delivery and provider webhook ingestion/application tracking |
| [008_reconciliation.sql](migrations/008_reconciliation.sql) | Reconciliation runs and discrepancies |
| [009_tenant_access.sql](migrations/009_tenant_access.sql) | Tenant API-key authentication |
| [010_blindpay.sql](migrations/010_blindpay.sql) | BlindPay-owned customers, bank accounts, wallets, and raw webhook events |

## Quick start

Requirements: Go, Docker, and Docker Compose.

Start PostgreSQL and Kafka:

```bash
docker compose up -d
docker compose ps
```

Apply migrations:

```bash
for migration in migrations/*.sql; do
  docker compose exec -T postgres psql -U stablerail -d stablerail -f - < "$migration"
done
```

Create Kafka topics:

```bash
for topic in payout-events payin-events payment-events settlement-commands stablerail-dead-letter; do
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1
done
```

Run StableRail:

```bash
export STABLERAIL_DATABASE_URL=postgresql://stablerail:stablerail@localhost:5432/stablerail
export STABLERAIL_KAFKA_BROKERS=localhost:9092
export STABLERAIL_OPERATOR_TOKEN='replace-with-a-secret-token'
go run ./cmd/stablerail
```

The API listens on `:8080`. Create a tenant API key:

```bash
curl -X POST http://localhost:8080/v1/operator/tenants/tenant-1/api-keys \
  -H "Authorization: Bearer $STABLERAIL_OPERATOR_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"local development"}'
```

Save the returned `srk_...` value as `STABLERAIL_API_KEY`, then create and inspect a payment:

```bash
curl -X POST http://localhost:8080/v1/payments \
  -H "Authorization: Bearer $STABLERAIL_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: request-123' \
  -d '{"direction":"payout","external_reference":"order-123","currency":"USD","amount_minor":2500}'

curl -H "Authorization: Bearer $STABLERAIL_API_KEY" \
  http://localhost:8080/v1/payments/PAYMENT_ID/timeline
```

Payment processing is asynchronous. Poll the payment or timeline endpoint to observe status changes. Reusing an idempotency key with the same request returns the original payment; reusing it with different fields returns `409 Conflict`.

Stop the environment with `docker compose down`. Add `--volumes` to permanently remove local PostgreSQL data.

## API

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/payments` | Create a pay-in or payout payment (`direction: "payin"` or `"payout"`) |
| `GET /v1/payments/{id}` | Read a payment |
| `GET /v1/payments/{id}/timeline` | Read payment history |
| `POST /v1/payments/{id}/refunds` | Create a merchant-issued refund as a linked payment |
| `POST /v1/payment-quotes` | Create a provider-neutral pay-in or payout quote |
| `POST /v1/providers/blindpay/webhooks` | Receive signed BlindPay events |
| `POST /v1/webhook-endpoints` | Register a tenant webhook |
| `GET /v1/webhook-endpoints` | List tenant webhooks |
| `DELETE /v1/webhook-endpoints/{id}` | Disable a tenant webhook |
| `POST /v1/operator/tenants/{id}/api-keys` | Issue a tenant API key |
| `DELETE /v1/operator/api-keys/{id}` | Revoke a tenant API key |
| `POST /v1/operator/payments/{id}/manual-review` | Resolve a held payment |
| `POST /v1/operator/mock-settlements/{id}` | Resolve a local mock settlement when enabled |
| `GET /healthz` | Liveness check |
| `GET /readyz` | PostgreSQL readiness check |
| `GET /metrics` | Prometheus metrics |

Tenant endpoints require `Authorization: Bearer <api-key>`. Operator endpoints are available only when `STABLERAIL_OPERATOR_TOKEN` is set and require that token. Payment reads are tenant-scoped. `direction` should be supplied explicitly; omission currently defaults to `payout` for backward compatibility.

`POST /v1/payments` returns `202 Accepted` for a created pay-in and `201 Created` for a created payout. Provider instructions and later lifecycle statuses are asynchronous; there is no provider network call in the HTTP transaction.

## Settlement providers

The application selects one complete `settlement.SettlementProvider`, which composes `payout.Provider` and `payin.Provider`. Those capabilities expose generic quotes and execution, while workers depend on the application service they use. The runtime includes a deterministic mock provider and a BlindPay adapter with durable payout submission, pay-ins, signed webhooks, compliance holds, reconciliation, and ambiguous-outcome recovery.

BlindPay refund semantics depend on direction and timing. A payout refunded before success becomes `payment_status=failed` with `funds_status=returned`; after payout success, returned funds are recorded as a separate `payment_returns` operation while the original payment remains `succeeded/consumed`. A refunded pay-in becomes `failed/returned`. None of these provider-originated events is a merchant-issued refund.

Configure the BlindPay provider with:

```bash
export STABLERAIL_BLINDPAY_API_KEY='...'
export STABLERAIL_BLINDPAY_INSTANCE_ID='in_...'
export STABLERAIL_BLINDPAY_WEBHOOK_SECRET='whsec_...'
export STABLERAIL_BLINDPAY_NETWORK='base'
export STABLERAIL_BLINDPAY_TOKEN='USDC'
export STABLERAIL_BLINDPAY_MANAGED_WALLET_ID='bl_...'
export STABLERAIL_BLINDPAY_MANAGED_WALLET_ADDRESS='0x...'
```

Register the public HTTPS URL `https://your-host/v1/providers/blindpay/webhooks` in the BlindPay dashboard. This is an inbound provider endpoint, not a client API. StableRail verifies its Svix signature headers before storing or processing a delivery.

StableRail persists a payout submission attempt before calling BlindPay. If the response is lost, recovery retries with the original provider idempotency key and reconciliation confirms the result. A verified webhook or reconciliation result, rather than the initial HTTP response alone, determines the final payment and funds statuses.

See [BlindPay lifecycle testing](docs/testing/blindpay-payment-lifecycle.md) for provider scenarios and expected accounting behavior.

## Testing

Run the unit and package tests:

```bash
go test ./...
```

Run race detection and static analysis:

```bash
go test -race ./...
go vet ./...
```

Run the provider-free PostgreSQL and Kafka lifecycle suite:

```bash
./scripts/test-e2e-local.sh
```

Pass normal `go test` arguments to select a scenario:

```bash
./scripts/test-e2e-local.sh -run '^TestLOCAL001SuccessfulPaymentLifecycle$'
```

Set `STABLERAIL_E2E_KEEP_STACK=1` to retain its isolated containers. See the [local lifecycle test guide](docs/testing/local-payment-lifecycle.md) for the executable scenario specification.

## Operational status

The core payment, saga, managed-wallet payout, pay-in, merchant-issued refund, provider-return, webhook, reconciliation, and manual-review paths are implemented. Merchant refunds are linked payments and reuse the ordinary payout saga; provider-originated returns remain separate operations with reversal accounting. Remaining production work includes distributed tracing, alert integrations, credential rotation, provider rate limiting, rollout controls, direct quote-free pay-in APIs, and external-wallet payout submission.

The mock provider and local Compose environment are intended for development and verification. A production deployment still requires environment-specific security controls, topic provisioning, monitoring, and a limited provider pilot.
