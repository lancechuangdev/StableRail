# Local payment lifecycle test suite

This suite verifies StableRail's payment lifecycle with the deterministic local
settlement provider. It deliberately does not configure or call BlindPay.

## Running the suite

Start the local PostgreSQL and Kafka services, apply the migrations, and run
StableRail with the mock settlement provider and an operator token. Then run:

```bash
STABLERAIL_E2E_BASE_URL=http://localhost:8080 \
STABLERAIL_E2E_DATABASE_URL=postgresql://stablerail:stablerail@localhost:5432/stablerail \
STABLERAIL_E2E_OPERATOR_TOKEN=local-e2e-operator \
go test -tags=e2e -count=1 -v ./e2e/local/...
```

`scripts/test-e2e-local.sh` performs those steps with `compose.e2e.yaml`. It uses
isolated PostgreSQL and Kafka containers on ports 15432 and 19092 and removes their
volumes after the run. It does not alter the normal development Compose stack.

To retain the isolated database after a run for inspection:

```bash
STABLERAIL_E2E_KEEP_STACK=1 ./scripts/test-e2e-local.sh
```

The runner forwards additional arguments to `go test`. Run one scenario with:

```bash
./scripts/test-e2e-local.sh -run '^TestLOCAL001SuccessfulPaymentLifecycle$'
```

The temporary StableRail process still stops, but PostgreSQL and Kafka remain
running. Connect to PostgreSQL with:

```bash
docker compose --env-file /dev/null -p stablerail-local-e2e \
  -f compose.e2e.yaml exec postgres psql -U stablerail -d stablerail
```

Remove the retained containers and volumes manually with:

```bash
docker compose --env-file /dev/null -p stablerail-local-e2e \
  -f compose.e2e.yaml down -v
```

## Scenarios

| ID | Scenario | Status |
| --- | --- | --- |
| LOCAL-001 | Successful payment reaches `succeeded`, its saga reaches `completed`, its ledger balances, and its timeline records processing and success | Automated |
| LOCAL-002 | Repeating an equivalent request returns the same payment; changing the request under the same key returns `409` | Automated |
| LOCAL-003 | Tenant payment and timeline reads are isolated; a different tenant receives `404` | Automated |
| LOCAL-004 | Policy rejection fails the payment without settlement | Automated |
| LOCAL-005 | Settlement failure fails the payment but preserves reserved funds until their disposition is confirmed | Automated |
| LOCAL-007 | Manual review resolution resumes or terminates a held saga | Automated |
| LOCAL-008 | Multiple tenant webhook endpoints receive independently signed events | Automated |
| LOCAL-009 | Restarting workers completes durable pending work without duplicate business effects | Automated |

## LOCAL-001: successful lifecycle

1. Issue an API key for a unique tenant through the operator API.
2. Create a USD payment using the tenant key and a unique idempotency key.
3. Poll the payment API until the payment is `succeeded`.
4. Verify the corresponding saga is `completed`.
5. Verify every ledger journal has equal debit and credit totals.
6. Verify the timeline contains `created`, `processing`, and `succeeded` in order.

## LOCAL-002: payment idempotency

1. Submit a payment twice using the same tenant, body, and idempotency key.
2. Verify both responses identify the same payment.
3. Change the amount while reusing the key and verify `409 Conflict`.
4. Verify only one payment row uses the idempotency key.

## LOCAL-003: tenant isolation

1. Issue keys for two unique tenants.
2. Create a payment as the first tenant.
3. Verify the first tenant can read the payment and timeline.
4. Verify the second tenant receives `404` for both resources.

## LOCAL-004 through LOCAL-009

- `LOCAL-004` uses the configured mock policy rejection amount and verifies no ledger or settlement work occurs.
- `LOCAL-005` uses the configured mock settlement failure amount and verifies that the reservation remains in place without a release journal.
- `LOCAL-007` drives a mock settlement on hold through its short local compliance timeout, resolves manual review through the operator API, and verifies settlement resumes.
- `LOCAL-008` registers two local HTTP receivers and verifies every receiver gets independently signed lifecycle events.
- `LOCAL-009` leaves a mock settlement pending, schedules its provider completion, kills and restarts StableRail, and verifies durable work completes without duplicate submissions or journals.

## Test design rules

- Scenario IDs are stable and appear in both this document and Go test names.
- Business assertions belong in Go; runner scripts only prepare infrastructure.
- Tests use public HTTP APIs for commands and reads. Direct database access is
  limited to durable-state assertions that have no public endpoint yet.
- Each test uses unique identifiers and must not depend on execution order.
- Provider-specific behavior belongs in the `e2e/blindpay` suite.
