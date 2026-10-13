---
term: "Saga pattern"
aliases: ["saga", "sagas", "compensating transaction", "orchestration saga", "choreography saga"]
category: distributed
short: "A way to manage a multi-service business transaction as a sequence of local transactions with compensating actions on failure."
related: ["two-phase-commit", "outbox-pattern", "idempotency"]
---
## In plain words (Junior)
Booking a trip involves flight, hotel and car separately. There's no single operation to buy all three, so you book them one by one, and if the car fails, you cancel the hotel and the flight. A saga is this strategy for software: do steps one by one, and undo earlier steps with explicit "cancel" actions if a later one fails.

## How it works (Mid)
A saga splits a distributed transaction into local ACID transactions, each in one service with its own database. Each step has a compensating transaction that semantically undoes it (refund, release, cancel). Two coordination styles: choreography (services react to each other's events, simple but hard to trace) and orchestration (a central coordinator or workflow engine like Temporal or Step Functions drives steps and compensations, easier to reason about).

```
Order saga (orchestrated):
 1. Orders:    create order (PENDING)           compensate: cancel order
 2. Payments:  charge card                      compensate: refund
 3. Inventory: reserve items                    compensate: release stock
 4. Shipping:  schedule delivery                compensate: cancel shipment
 failure at step 3 -> run compensations for 2, then 1 (in reverse)
```

Sagas give eventual consistency without distributed locks. Intermediate states are visible (order PENDING), so there is no isolation; design for it with semantic locks, status fields and commutative operations. Steps and compensations must be idempotent and retryable because messages are delivered at-least-once.

## Senior angle & interview tips
Key design points: compensation isn't rollback (an email sent can't be unsent; compensate with an apology), some steps are non-compensatable and should come last (pivot transaction), failure of a compensation needs retries and human escalation, and each step must atomically update its DB and publish its event (outbox pattern) to avoid lost messages. Observability needs correlation ids and a saga log/state. For data engineering, the same thinking applies to multi-system pipelines (load, publish, notify) where 2PC isn't available: make steps idempotent and have a recovery path.

Interview question: "How do you keep orders and payments consistent across services?" Model answer: an orchestrated saga with idempotent steps and compensations (refund, cancel), using the outbox pattern for reliable event publication, and accepting eventual consistency with visible pending states instead of a distributed 2PC.
