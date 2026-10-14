---
term: "Database transaction"
aliases: ["transaction","commit","rollback"]
category: distributed
short: "A group of operations treated as one all-or-nothing unit."
related: ["acid","isolation-level","wal","two-phase-commit"]
---
## In plain words (Junior)
Moving money between accounts: both the withdrawal and the deposit happen together, or neither does.

## How it works (Mid)
BEGIN starts it, COMMIT makes changes permanent, ROLLBACK discards them. The database uses logs and locks or MVCC to provide atomicity and isolation. Long transactions hold resources, bloat undo logs and block cleanup.

## Senior angle & interview tips
Keep transactions short, avoid user think-time inside them, retry on serialization failures and deadlocks, and make the retry idempotent. Across services use sagas or the outbox pattern rather than distributed transactions.

Interview question: "Why are long transactions harmful?" Answer: they hold locks/snapshots, block vacuum and compaction, and raise conflict rates.
