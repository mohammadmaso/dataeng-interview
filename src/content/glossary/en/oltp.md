---
term: "OLTP (online transaction processing)"
aliases: ["oltp", "transactional database", "transactional workload"]
category: storage
short: "Workload of many small, concurrent, low-latency reads and writes on current data, protected by ACID transactions."
related: ["olap", "mvcc", "wal"]
---
## In plain words (Junior)
OLTP is the cash register of the digital world: lots of small, quick operations such as placing an order, paying, or updating an address, each one needing to be correct right away. Many customers do this at the same time, and every operation touches just a few records.

## How it works (Mid)
Characteristics: row-oriented storage, B-tree indexes, normalized schemas (3NF), short ACID transactions, high concurrency, millisecond latency, and data that is current (not history). Systems: PostgreSQL, MySQL, Oracle, SQL Server, and distributed SQL like CockroachDB or Spanner.

```
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;      -- atomic, durable (WAL), isolated (MVCC/locks)

-- typical: point lookups by PK, small indexed ranges, ms latency, thousands of TPS
```

ACID: atomicity (all or nothing), consistency (constraints hold), isolation (concurrent transactions do not interfere, per isolation level), durability (committed data survives crashes via WAL). Concurrency uses MVCC and/or locks. OLTP databases are the usual sources for pipelines: extracting heavy analytics directly from them hurts the application, so use replicas or CDC into an OLAP system.

## Senior angle & interview tips
For data engineers, OLTP matters as a source system: avoid long analytical queries on the primary, use read replicas or log-based CDC, understand isolation levels (read committed vs repeatable read vs serializable) and what consistent snapshot extraction needs, plan for schema changes owned by app teams, and handle deletes. Anomalies by isolation level: dirty reads, non-repeatable reads, phantoms, write skew. Row stores are poor for large scans, which is why analytics moves to columnar OLAP stores; HTAP systems try to serve both with trade-offs.

Interview question: "Why not run analytics directly on the production database?" Model answer: heavy scans compete with transactional traffic for CPU, I/O and locks, row storage is inefficient for aggregation, and schemas are normalized for writes; replicate via CDC to a columnar store optimized for scans.
