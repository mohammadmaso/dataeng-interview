---
term: "Outbox pattern"
aliases: ["transactional outbox", "outbox table", "outbox"]
category: distributed
short: "Writing business data and an outgoing event in one local transaction, then publishing the event reliably from the outbox table."
related: ["cdc", "saga", "two-phase-commit", "exactly-once"]
---
## In plain words (Junior)
Suppose you must mail a letter every time you record a sale. If you record the sale and then the power fails before mailing, the letter is lost; if you mail first and the sale fails, you sent a false letter. The fix: write the letter into an outgoing tray in the same notebook entry as the sale, and let a helper reliably mail whatever is in the tray.

## How it works (Mid)
The dual-write problem: updating a database and publishing to Kafka can't be done atomically, because the two systems have no shared transaction. A crash between them leaves state and events inconsistent. The outbox pattern stores the event as a row in an `outbox` table in the same local transaction as the business change, so they commit atomically.

```
BEGIN;
  INSERT INTO orders (id, status, total) VALUES (42, 'PLACED', 99.0);
  INSERT INTO outbox (id, aggregate_id, type, payload, created_at)
  VALUES (gen_random_uuid(), 42, 'OrderPlaced', '{"order_id":42,"total":99.0}', now());
COMMIT;      -- both or neither

-- A relay (Debezium reading the WAL, or a poller) publishes outbox rows to Kafka,
-- then marks/deletes them. Consumers dedupe by event id.
```

A separate relay then publishes outbox rows: either log-based CDC (Debezium outbox event router tails the WAL, low latency, no polling load) or a polling publisher. Delivery to the broker is at-least-once, so consumers must be idempotent (dedupe by event id). Ordering per aggregate is preserved by keying messages with the aggregate id.

## Senior angle & interview tips
It trades two-phase commit's coupling for eventual consistency and at-least-once events. Operational details: cleaning up outbox rows (delete after publish or via CDC with delete + TTL), outbox table growth and vacuum pressure, relay lag monitoring, schema of the event payload as a published contract (avoid leaking internal table structure, unlike raw table CDC), and polling publishers' scaling and ordering issues. The inverse for consumers is the inbox pattern: store processed message ids in the same transaction as the effect to deduplicate.

Interview question: "How do you guarantee an event is published when a row is saved?" Model answer: write the event to an outbox table in the same DB transaction, publish it asynchronously with CDC or a poller to Kafka, and make consumers idempotent; this avoids the dual-write inconsistency without distributed transactions.
