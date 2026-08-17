# BlindPay payment lifecycle test suite

This suite mirrors the nine local payment lifecycle scenarios through the
BlindPay adapter. It uses a local BlindPay HTTP emulator and sends correctly
signed provider webhooks, so it requires neither BlindPay credentials nor real
or testnet funds.

Run all scenarios from the repository root:

```bash
./scripts/test-e2e-blindpay.sh
```

Run one scenario with:

```bash
./scripts/test-e2e-blindpay.sh -run '^TestBLINDPAY006TerminalReturn$'
```

The runner starts isolated PostgreSQL and Kafka services, the BlindPay API
emulator, and StableRail configured with `STABLERAIL_SETTLEMENT_PROVIDER=blindpay`.
It applies real Svix-compatible signatures to webhook requests and exercises
the production BlindPay quote, payout, webhook, recovery, saga, ledger, and
tenant notification code paths.

| ID | Scenario |
| --- | --- |
| BLINDPAY-001 | Successful payout lifecycle, balanced ledger, and ordered timeline |
| BLINDPAY-002 | Payment idempotency and conflict detection with a bound payout quote |
| BLINDPAY-003 | Tenant isolation for BlindPay-backed payments |
| BLINDPAY-004 | Policy rejection before BlindPay submission |
| BLINDPAY-005 | Failed payout webhook and compensating ledger release |
| BLINDPAY-006 | A post-success BlindPay `refunded` event creates a separate return and reversal journal without changing the payment outcome |
| BLINDPAY-007 | On-hold payout entering and leaving manual review |
| BLINDPAY-008 | Independent signed tenant lifecycle webhooks |
| BLINDPAY-009 | Restart recovery of an early durable BlindPay completion webhook |

Direct database access is limited to provisioning approved provider references
that have no public StableRail API and to durable-state assertions.
