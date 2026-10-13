---
term: "ACID"
aliases: ["acid transactions", "acid properties", "atomicity"]
category: storage
short: "Four transaction guarantees: Atomicity, Consistency, Isolation, Durability."
related: ["oltp", "mvcc", "wal", "isolation-level"]
---
## In plain words (Junior)
A bank transfer must either fully happen or not happen at all, never leave money missing midway. It must follow the rules, not be disturbed by other transfers happening at the same time, and still be there after a power cut. Those four promises are ACID.

## How it works (Mid)
Atomicity: all operations in a transaction apply or none do (via undo/rollback). Consistency: a transaction takes the database from one valid state to another, with constraints enforced (partly the application's job). Isolation: concurrent transactions don't see each other's partial work, to a degree set by the isolation level. Durability: once committed, data survives crashes, via the write-ahead log and fsync or replication.

```
BEGIN;
  UPDATE accounts SET bal = bal - 100 WHERE id = 1;   -- A: both or neither
  UPDATE accounts SET bal = bal + 100 WHERE id = 2;
  -- C: CHECK (bal >= 0) and FKs must hold at commit
COMMIT;  -- D: after this returns, survives crash (WAL fsync)
-- I: concurrent transactions behave per the isolation level
```

In data engineering ACID matters for table formats (Iceberg/Delta give atomic commits on object storage), for sinks (atomic publish of a batch), and when comparing with BASE-style NoSQL stores that relax some properties.

## Senior angle & interview tips
ACID "C" differs from CAP's "C" (linearizability); mixing them up is a classic mistake. Isolation is the loosest and most misunderstood letter: default levels in many databases (read committed, snapshot) allow anomalies like write skew. Distributed ACID needs 2PC or consensus-replicated transactions and costs latency. Many NoSQL systems offer atomicity only per row/document. Lakehouse ACID is typically optimistic: serializable or snapshot isolation at table level, with conflicts on concurrent writes.

Interview question: "What does the D in ACID require?" Model answer: committed data must survive crashes, so the commit is acknowledged only after the change is in a durable log (fsync'd WAL or replicated to a quorum); relaxing it (async commit) trades possible loss of recent commits for latency.
