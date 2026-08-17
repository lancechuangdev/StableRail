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
    Saga <-->|Payouts and status| BlindPay[BlindPay]
    BlindPay -->|Signed webhook| API
    DB --> Webhooks[Tenant webhooks]
```

Every business update and outgoing event commits in one PostgreSQL transaction. Consumers record an event in their inbox and apply its effects in another single transaction. Delivery is at least once; deterministic event IDs, uniqueness constraints, and inbox records make retries safe.

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
        reserved --> available: payout failed before capture
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

An ambiguous provider submission remains `payment_status=processing` and `funds_status=reserved`. The uncertainty is recorded on `blindpay_payouts.provider_status=unknown` until idempotent recovery or a webhook establishes the outcome.

### Post-success returns

A bank or provider can return funds after a payout was already confirmed. StableRail records that as a separate financial operation:

```text
Payment: created -> processing -> succeeded
Return:  created -> processing -> succeeded | failed
```

Returns support only `created`, `processing`, `succeeded`, and `failed`. When a provider webhook reports a return that has already completed externally, StableRail may create the return directly as `succeeded`. The return journal debits `cash:operating` for the asset received back and credits `settlement:payable` to restore the obligation. The original payment is not rewritten. StableRail emits `payment.return.succeeded` for tenant notification. Merchant-issued refunds require a separate linked refund workflow and are not currently implemented.

## Saga lifecycle

The persisted saga tracks internal workflow progress in more detail than the public payment and funds statuses:

```mermaid
stateDiagram-v2
    [*] --> awaiting_policy: payment.created
    awaiting_policy --> awaiting_ledger: policy.approved / ledger.reserve
    awaiting_policy --> failed: policy.rejected / payment.fail
    awaiting_ledger --> awaiting_settlement: ledger.reserved / settlement.execute
    awaiting_ledger --> failed: ledger.failed / payment.fail

    awaiting_settlement --> settling_payment: settlement.completed / payment.settle
    settling_payment --> completed: payment.succeeded

    awaiting_settlement --> releasing_ledger: settlement.failed or timeout / ledger.release
    releasing_ledger --> ledger_released: ledger.released / payment.fail

    awaiting_settlement --> returning: settlement.returned / ledger.release
    returning --> returned: ledger.released / payment.return

    awaiting_settlement --> on_hold: settlement.on_hold
    on_hold --> settling_payment: settlement.completed / payment.settle
    on_hold --> releasing_ledger: settlement.failed / ledger.release
    on_hold --> returning: settlement.returned / ledger.release
    on_hold --> manual_review: compliance timeout
    manual_review --> on_hold: operator retry
    manual_review --> settling_payment: operator complete / payment.settle
    manual_review --> releasing_ledger: operator fail / ledger.release
    manual_review --> returning: operator return / ledger.release
```

Timeout handling is conservative: settlement timeouts release the reservation before failure, compliance timeouts require manual review, and uncertain provider outcomes remain in processing until recovery or reconciliation produces a terminal result.

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
| `POST /v1/blindpay/payout-quotes` | Create a provider-bound payout quote |
| `POST /v1/providers/blindpay/webhooks` | Receive signed BlindPay events |
| `POST /v1/webhook-endpoints` | Register a tenant webhook |
| `GET /v1/webhook-endpoints` | List tenant webhooks |
| `DELETE /v1/webhook-endpoints/{id}` | Disable a tenant webhook |
| `POST /v1/operator/tenants/{id}/api-keys` | Issue a tenant API key |
| `DELETE /v1/operator/api-keys/{id}` | Revoke a tenant API key |
| `POST /v1/operator/payments/{id}/manual-review` | Resolve a held payment |
| `GET /healthz` | Liveness check |
| `GET /readyz` | PostgreSQL readiness check |
| `GET /metrics` | Prometheus metrics |

Tenant endpoints require `Authorization: Bearer <api-key>`. Operator endpoints are available only when `STABLERAIL_OPERATOR_TOKEN` is set and require that token. Payment reads are tenant-scoped.

## Settlement Providers

The runtime includes a deterministic mock settlement provider for local development and a BlindPay managed-wallet integration with provider-bound quotes, durable payout submission, signed webhooks, compliance holds, reconciliation, and ambiguous-outcome recovery. BlindPay's provider status `refunded` maps either to a failed payment with returned funds or, if completion was already recorded, to a separate post-success return. It is not a merchant-initiated refund.

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

The core payment, saga, managed-wallet payout, provider-return, webhook, and manual-review paths are implemented. Merchant-initiated refunds are not yet modeled; they should be separate resources linked to succeeded payments. Remaining production work includes distributed tracing, alert integrations, credential rotation, provider rate limiting, rollout controls, and external-wallet blockchain submission.

The mock provider and local Compose environment are intended for development and verification. A production deployment still requires environment-specific security controls, topic provisioning, monitoring, and a limited provider pilot.
