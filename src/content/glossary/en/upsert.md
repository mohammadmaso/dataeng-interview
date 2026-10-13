---
term: "Upsert"
aliases: ["upserts", "merge into", "insert on conflict", "insert or update"]
category: ingestion
short: "An operation that updates a row if its key already exists, otherwise inserts it, in one atomic step."
related: ["idempotency", "cdc", "tombstone"]
---
## In plain words (Junior)
Updating a contact list: if the person is already in your phone, you edit their number; if not, you add them as new. You don't need to check first, and you never end up with two entries. Upsert is "update or insert" as one operation.

## How it works (Mid)
Upsert merges incoming rows into a target by a unique key. It makes loads idempotent (rerun the same batch and the result is the same) and is the core of CDC apply, SCD handling and incremental models. Forms: `INSERT ... ON CONFLICT` (Postgres), `ON DUPLICATE KEY UPDATE` (MySQL), standard `MERGE`, and key-based "last write wins" in stores like Cassandra or Redis.

```
-- PostgreSQL
INSERT INTO users (id, email, updated_at) VALUES (42, 'a@x.com', now())
ON CONFLICT (id) DO UPDATE
  SET email = EXCLUDED.email, updated_at = EXCLUDED.updated_at
  WHERE users.updated_at < EXCLUDED.updated_at;     -- ignore stale events

-- Lakehouse / warehouse
MERGE INTO dim_user t USING staged s ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Requires a reliable unique key and deterministic input: if the source batch has several rows for one key, `MERGE` errors or picks arbitrarily, so dedupe first (latest by version/timestamp). Include a version or timestamp check so out-of-order events don't overwrite newer data with older.

## Senior angle & interview tips
Cost depends on the store. Row stores do it cheaply via index. In columnar lakehouse tables, `MERGE` rewrites affected files (copy-on-write) or writes delete files merged at read (merge-on-read): expensive, so batch upserts and cluster/partition on the key to limit rewritten files. ClickHouse ReplacingMergeTree dedupes eventually at merge time (use `FINAL` or `argMax` for exactness). Concurrent `MERGE`s on the same rows conflict. Deletes need a flag or op column. Watch for non-matching keys due to type or case differences.

Interview question: "How do you apply CDC events to an Iceberg table?" Model answer: dedupe each micro-batch to the latest event per primary key by log position, then run a MERGE that deletes, updates or inserts based on the op, partition-pruned and compacted regularly, with ordering guaranteed by the version column.
