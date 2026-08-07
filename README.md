# A stablecoin payment platform for cross-border payouts.

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

Goal:

Build a production payment lifecycle.

```
POST /payments
↓
PaymentIntent
↓
Internal Ledger
↓
Async Settlement Worker
↓
Completed
```

Deliverables:

- Payment state machine
- Ledger
- Idempotency keys
- Audit log
- Timeline API
