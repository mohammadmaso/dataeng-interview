---
term: "Change data capture (CDC)"
aliases: ["cdc", "change data capture", "log-based cdc"]
category: ingestion
short: "Capturing row-level inserts, updates and deletes from a database, usually by reading its transaction log, and streaming them downstream."
related: ["wal", "upsert", "tombstone", "outbox-pattern"]
---
## In plain words (Junior)
Instead of copying a whole notebook every night to see what changed, imagine a friend who reads over your shoulder and tells you each edit as it happens: "line 5 changed, line 9 deleted." CDC does that for databases. It sends only the changes, quickly, so other systems stay in sync without heavy full copies.

## How it works (Mid)
Approaches: query-based (poll `updated_at`; misses hard deletes and intermediate states), trigger-based (adds load to the source), and log-based (read the WAL/binlog; low impact, captures every change in commit order, including deletes). Debezium is the common log-based tool, publishing events to Kafka.

```
{
  "op": "u",
  "before": {"id": 7, "status": "NEW"},
  "after":  {"id": 7, "status": "PAID"},
  "source": {"db": "shop", "table": "orders", "lsn": 23984112},
  "ts_ms": 1715000000123
}
```

Each event carries operation type, before/after images and a log position. A sink applies them as an upsert/delete by primary key (Iceberg/Hudi/Delta `MERGE`, ClickHouse ReplacingMergeTree) to maintain a replica. Usually an initial snapshot is taken first, then streaming from the recorded log position.

## Senior angle & interview tips
Hard parts: consistent initial snapshot plus handoff to streaming, schema changes in the source, ordering per key (partition by primary key), TOASTed/unchanged columns in Postgres, replication slot bloat when a consumer stalls (WAL piles up and can fill the disk), and tombstones for deletes in compacted Kafka topics. Delivery is at-least-once, so sinks must be idempotent; apply in log order and use the log position to drop stale updates. CDC exposes internal table schemas, so use contracts or the outbox pattern for domain events.

Interview question: "How do you replicate Postgres into the lakehouse?" Model answer: Debezium on the logical replication slot to Kafka keyed by primary key, then an idempotent merge into Iceberg, with monitoring for slot lag and a compaction plan.
