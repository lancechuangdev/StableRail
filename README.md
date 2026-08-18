# StableRail

StableRail is a Go reference implementation of a durable, event-driven payment workflow. It accepts payment intents over HTTP, stores payment and double-entry ledger state in PostgreSQL, and coordinates policy, ledger, settlement, provider returns, and manual review through Kafka and a persisted saga.

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
    Provider --> BlindPay[BlindPay adapter]
    BlindPay -->|Signed webhook| API
    DB --> Webhooks[Tenant webhooks]
```

Every business update and outgoing event commits in one PostgreSQL transaction. Consumers record an event in their inbox and apply its effects in another single transaction. Delivery is at least once; deterministic event IDs, uniqueness constraints, and inbox records make retries safe.

### Package ownership

| Package | Responsibility |
| --- | --- |
| `paymentcore` | Outbound payment lifecycle, refunds, public payment/funds state, and PostgreSQL payment storage |
| `paymentcore/payin` | Inbound pay-in quotes, pay-in lifecycle, and pay-in persistence |
| `settlement` | Provider-neutral payout/pay-in contracts and the deterministic mock provider |
| `settlement/blindpay` | BlindPay client, resource mapping, quote/execution adapters, recovery, and webhooks |
| `saga` and `workers` | Durable outbound orchestration across policy, ledger, and settlement |
| `paymentapi` | HTTP transport, authentication, and tenant/operator endpoints |

There is no separate top-level `payout` package because an outbound payment is
StableRail's payout aggregate: `paymentcore` owns it, while `settlement` only
defines how a provider executes it. Pay-ins are a distinct inbound aggregate
and therefore live as the `paymentcore/payin` subpackage.

## Payment lifecycle

The API exposes two independent dimensions:

```mermaid
stateDiagram-v2
    state "Payment status" as payment {
        [*] --> created
        created --> processing: submitted
        created --> failed: rejected before submission
        processing --> succeeded: recipient paid
        processing --> failed: payout unsuccessful
    }

    state "Funds status" as funds {
        [*] --> available
        available --> reserved: ledger reservation
        reserved --> consumed: payout succeeded
        reserved --> available: pre-capture failure confirmed
        reserved --> reserved: payout failed, disposition unresolved
        reserved --> returned: provider returned captured funds
    }
```

Common combinations are:

| `payment_status` | `funds_status` | Meaning |
| --- | --- | --- |
| `created` | `available` | Recorded but not funded |
| `processing` | `reserved` | Funds committed while the payout runs |
| `succeeded` | `consumed` | Recipient payout completed |
| `failed` | `available` | Payout failed and funds are available |
| `failed` | `reserved` | Payout failed but the funds disposition is unresolved |
| `failed` | `returned` | Payout failed after capture and the provider returned the funds |

Before success, BlindPay's external `refunded` payout status maps to `payment_status=failed` and `funds_status=returned`. After success, it creates a separate `payment_returns` record and reversal journal while the original payment remains `payment_status=succeeded` and `funds_status=consumed`.

An ambiguous provider submission remains `payment_status=processing` and `funds_status=reserved`. The uncertainty is recorded on `payouts.provider_status=unknown` until idempotent recovery or a webhook establishes the outcome.

### Post-success returns

A bank or provider can return funds after a payout was already confirmed. StableRail records that as a separate financial operation:

```text
Payment: created -> processing -> succeeded
Return:  created -> processing -> succeeded | failed
```

The return status domain contains only `created`, `processing`, `succeeded`, and `failed`. The current provider webhook path learns about a return after it has completed externally, so it creates the return directly as `succeeded`; initiated `created` and `processing` transitions are not implemented yet. The return journal debits `cash:operating` for the asset received back and credits `settlement:payable` to restore the obligation. The original payment is not rewritten. StableRail emits `payment.return.succeeded` for tenant notification.

Merchant-issued refunds are separate linked payments. `POST /v1/payments/{id}/refunds` accepts an idempotency key, a positive amount, a reason, and an optional fresh `payout_quote_id` for BlindPay routing. Partial refunds are supported up to the original payment amount. StableRail creates a new payment, links it through `payment_refunds.refund_payment_id`, binds the payout quote when supplied, and emits the normal `payment.created` event for that new payment. From there, policy, ledger reservation, settlement, failure handling, and tenant webhooks use the ordinary payment saga and `ExecutePayout`; no refund-specific provider operation or reversal journal is involved. The refund response contains `refund_payment_id` but no duplicated status; clients query `GET /v1/payments/{refund_payment_id}` for its payment and funds statuses. The original payment remains `succeeded/consumed`. Provider-originated returns remain separate and continue to use `payment_returns` and reversal accounting.

## Saga lifecycle

The persisted saga tracks internal workflow progress in more detail than the public payment and funds statuses:

```mermaid
stateDiagram-v2
    state "funds_returned (saga)" as returned
    [*] --> awaiting_policy: payment.created
    awaiting_policy --> awaiting_ledger: policy.approved / ledger.reserve
    awaiting_policy --> failed: policy.rejected / payment.fail
    awaiting_ledger --> awaiting_settlement: ledger.reserved / settlement.execute
    awaiting_ledger --> failed: ledger.failed / payment.fail

    awaiting_settlement --> settling_payment: settlement.completed / payment.settle
    settling_payment --> completed: payment.succeeded

    awaiting_settlement --> failed: settlement.failed or timeout / payment.fail_reserved
    awaiting_settlement --> failed: submission_failed / payment.fail

    awaiting_settlement --> returning: settlement.returned / ledger.release
    failed --> returning: late settlement.returned / ledger.release
    returning --> returned: ledger.released / payment.return

    awaiting_settlement --> on_hold: settlement.on_hold
    on_hold --> settling_payment: settlement.completed / payment.settle
    on_hold --> failed: settlement.failed / payment.fail_reserved
    on_hold --> returning: settlement.returned / ledger.release
    on_hold --> manual_review: compliance timeout
    manual_review --> on_hold: operator retry
    manual_review --> settling_payment: operator complete / payment.settle
    manual_review --> failed: operator fail / payment.fail_reserved
    manual_review --> returning: operator return / ledger.release
```

The saga's `funds_returned` label is persisted internally as `returned`. It is
not a payment status: the resulting payment remains `payment_status=failed`
while `funds_status=returned`. The transition from `failed` handles a provider
return that arrives after a reserved-funds failure was already recorded.

Timeout handling is conservative: settlement timeouts fail the payment but preserve its reservation, compliance timeouts require manual review, and ambiguous submissions remain in processing until recovery or reconciliation establishes an outcome. A reservation becomes available only after a confirmed pre-capture failure, and becomes returned only after the provider confirms the funds came back.

## Pay-ins

Pay-ins model inbound fiat independently from outbound payments. The current
HTTP flow creates a quote with `POST /v1/payin-quotes`, then consumes it with
`POST /v1/payins`. The quote
selects an opaque destination account and locks the funding method, amounts,
fees, currencies, and expiry. Creating the pay-in
returns provider instructions such as an ACH memo, bank details, Pix code, or
CLABE; retrieve current state with `GET /v1/payins/{id}`.

The generic `settlement.PayinProvider` boundary uses opaque source-instrument
and destination-account IDs resolved through `provider_resources`; provider
wallet and bank-account identifiers do not appear in the shared contract.
Verified `payin.*`
webhooks advance `processing | on_hold` to `succeeded | failed | refunded`.
Successful inbound settlement debits `cash:operating` and credits
`settlement:payable`; a later provider refund posts the inverse journal. Early
webhooks are retained and reconciled after the local pay-in becomes visible.

Pay-in and payout records own their source, destination, method, and monetary
snapshot. A quote is an optional commercial attachment that locks fees, FX,
and source/destination amounts. Accepting a quote copies those terms into the
operation, so lifecycle processing and accounting never depend on joining back
to the quote. The schema permits providers that execute without a quote. The
current pay-in HTTP workflow and BlindPay adapter remain quote-first.

## Provider resources

Shared APIs and workflow tables use provider-resource IDs instead of naming
provider-specific bank-account or wallet fields:

```text
Payout: source account -> destination payment instrument
Pay-in: optional source payment instrument -> destination account
```

An account represents a balance-holding resource, such as a managed wallet. A
payment instrument represents an external routing endpoint, such as a bank
account or blockchain address. `provider_resources` maps those stable IDs to a
provider and its reference. For example:

```text
acct_123       -> blindpay / managed wallet / bl_...
instrument_456 -> blindpay / bank account / ba_...
```

Callers must treat resource IDs as opaque; the current BlindPay reference sync
may reuse a provider reference as the local resource ID for compatibility, but
the adapter still resolves it through `provider_resources`. Adding another
provider requires new resource mappings and an adapter, not new columns in
`payins`, `payouts`, or their quote tables. Raw provider responses are isolated
in `provider_payload`, while raw webhook events remain adapter-owned data.

## Database migrations

Migrations are ordered by dependency. Provider-neutral platform and workflow
schema is created before the BlindPay-specific adapter schema.

| Migration | Main purpose |
| --- | --- |
| [001_payment_core.sql](migrations/001_payment_core.sql) | Payments, audit/timeline, destinations, and refunds |
| [002_eventing.sql](migrations/002_eventing.sql) | Transactional outbox and consumer inbox |
| [003_payment_workflow.sql](migrations/003_payment_workflow.sql) | Payment sagas, manual review actions, and settlement submission records |
| [004_payouts.sql](migrations/004_payouts.sql) | Provider resources, provider-neutral payout quotes, and payouts |
| [005_payins.sql](migrations/005_payins.sql) | Provider-neutral pay-in quotes and pay-ins |
| [006_ledger.sql](migrations/006_ledger.sql) | Accounts, balanced entries, payment/pay-in journals, and payment returns |
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
for topic in payment-events payment-commands stablerail-dead-letter; do
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
  -d '{"external_reference":"order-123","currency":"USD","amount_minor":2500}'

curl -H "Authorization: Bearer $STABLERAIL_API_KEY" \
  http://localhost:8080/v1/payments/PAYMENT_ID/timeline
```

Payment processing is asynchronous. Poll the payment or timeline endpoint to observe status changes. Reusing an idempotency key with the same request returns the original payment; reusing it with different fields returns `409 Conflict`.

Stop the environment with `docker compose down`. Add `--volumes` to permanently remove local PostgreSQL data.

## API

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/payments` | Create a payment |
| `GET /v1/payments/{id}` | Read a payment |
| `GET /v1/payments/{id}/timeline` | Read payment history |
| `POST /v1/payments/{id}/refunds` | Create a merchant-issued refund as a linked payment |
| `POST /v1/payout-quotes` | Create a provider-neutral payout quote |
| `POST /v1/payin-quotes` | Create a provider-neutral pay-in quote |
| `POST /v1/payins` | Create a pay-in from a quote |
| `GET /v1/payins/{id}` | Read a pay-in |
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

Tenant endpoints require `Authorization: Bearer <api-key>`. Operator endpoints are available only when `STABLERAIL_OPERATOR_TOKEN` is set and require that token. Payment reads are tenant-scoped.

## Settlement providers

The application selects one complete `settlement.SettlementProvider`. Its
embedded `PayoutProvider` and `PayinProvider` capabilities expose generic
quotes and execution, while workers depend only on the capability they use.
The runtime includes a deterministic mock provider and a BlindPay adapter with
durable payout submission, pay-ins, signed webhooks, compliance holds,
reconciliation, and ambiguous-outcome recovery. BlindPay's provider status
`refunded` maps either to a failed payment with returned funds or, if completion
was already recorded, to a separate post-success return. It is not a
merchant-initiated refund.

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

The core payment, saga, managed-wallet payout, pay-in, merchant-issued refund,
provider-return, webhook, reconciliation, and manual-review paths are
implemented. Merchant refunds are linked payments and reuse the ordinary payout
saga; provider-originated returns remain separate operations with reversal
accounting. Remaining production work includes distributed tracing, alert
integrations, credential rotation, provider rate limiting, rollout controls,
direct quote-free pay-in APIs, and external-wallet payout submission.

The mock provider and local Compose environment are intended for development and verification. A production deployment still requires environment-specific security controls, topic provisioning, monitoring, and a limited provider pilot.
