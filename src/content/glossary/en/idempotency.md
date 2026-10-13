---
term: "Idempotency"
aliases: ["idempotent", "idempotence"]
category: distributed
short: "An operation is idempotent if running it many times leaves the system in the same state as running it once."
related: ["exactly-once", "backfill", "upsert", "dead-letter-queue"]
---
## In plain words (Junior)
Think of an elevator call button. Pressing it five times does not bring five elevators; the result is the same as pressing it once. An idempotent data job is the same: if it crashes halfway and you run it again, you do not get duplicate or corrupted data. This matters because in data systems things fail and get retried all the time, so jobs must be safe to repeat.

## How it works (Mid)
A pipeline step is idempotent when `f(f(x)) = f(x)` in terms of its effect on the target. The common techniques are:

- Overwrite a deterministic slice (partition, date range) instead of appending.
- Write by a natural or deterministic key (`MERGE`/upsert, `INSERT ... ON CONFLICT`).
- Derive IDs from content (hash of business key) rather than random UUIDs or `now()`.
- Use a processed-offsets or batch-id table so a batch is applied once.

```
-- Not idempotent: a retry doubles the numbers
INSERT INTO daily_sales SELECT ... WHERE dt = '2024-05-01';

-- Idempotent: replace the whole partition (overwrite by key/date)
INSERT OVERWRITE TABLE daily_sales PARTITION (dt = '2024-05-01')
SELECT ... WHERE order_date = '2024-05-01';

-- Idempotent: keyed upsert
MERGE INTO sales t USING staging s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

The job must also be deterministic with respect to its inputs: reading "everything up to now" breaks idempotency, while reading "the logical date's window" keeps it.

## Senior angle & interview tips
Idempotency is what makes at-least-once delivery behave like exactly-once in effect: retries and duplicates become harmless. Typical traps: appending in a retried Airflow task, using `current_timestamp` in the output, non-idempotent side effects (emails, payments, API calls) and non-deterministic ordering that changes which duplicate wins. Idempotent producers in Kafka dedupe retries within one producer session only; end-to-end you still need idempotent sinks (keyed upserts, Iceberg `MERGE`, ClickHouse ReplacingMergeTree with caveats).

Interview question: "How do you make a daily pipeline safe to rerun?" Model answer: parameterize on the logical date, make the read window deterministic, and write via partition overwrite or keyed merge so that a rerun replaces rather than appends; add a run/batch id for side effects that cannot be overwritten.
