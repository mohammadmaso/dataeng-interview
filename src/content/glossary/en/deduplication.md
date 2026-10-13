---
term: "Deduplication"
aliases: ["dedup", "dedupe", "deduplicate", "duplicate records", "remove duplicates"]
category: quality
short: "Removing repeated records so each entity or event appears once, typically by key and by keeping the latest or first version."
related: ["idempotency", "upsert", "window-function", "bloom-filter"]
---
## In plain words (Junior)
If the same invoice is sent to you twice, you only want to pay it once. Deduplication is the cleanup that spots repeated copies of the same thing and keeps one. It matters because pipelines, retries and source systems often deliver the same record more than once.

## How it works (Mid)
Duplicates appear from at-least-once delivery, retries, overlapping incremental windows, replays/backfills, double-clicks and source bugs. Define what "same" means first: exact row equality, a natural/event id, or fuzzy matching (entity resolution) with similarity rules. For exact keys: `ROW_NUMBER()` per key keeping the latest, `DISTINCT`, `GROUP BY` with aggregation, or `MERGE` into a keyed target.

```
-- Keep the latest version per key (deterministic tiebreaker!)
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
           PARTITION BY event_id ORDER BY updated_at DESC, ingested_at DESC) AS rn
  FROM raw_events) WHERE rn = 1;

-- Spark streaming dedup with bounded state
df.withWatermark("event_time", "1 hour").dropDuplicates(["event_id", "event_time"])
```

Streaming dedup keeps seen keys in state, bounded by a watermark or TTL, so duplicates arriving after the window are missed. Probabilistic filters (Bloom filter) reduce memory but can wrongly drop a new event. In stores, keyed upserts or ReplacingMergeTree-like engines dedupe eventually at merge time.

## Senior angle & interview tips
Prefer making writes idempotent (dedupe at the sink by key) over cleaning later. Always add a deterministic tiebreaker in the `ORDER BY`, or reruns can pick different survivors. Know the cost: global dedup on huge data needs a shuffle by key; reduce it with partition-local dedup when duplicates are known to be near each other. Decide what to do with the discarded duplicates (audit table) and monitor duplicate rate as a quality signal: a spike means an upstream retry storm or replay. For entity resolution (same customer, different spellings), use blocking keys, similarity scoring and survivorship rules, which is much harder than key dedup.

Interview question: "How do you dedupe a Kafka stream into a table?" Model answer: carry a stable event id, make the sink an idempotent upsert on that id so replays and retries converge, and, if needed, a watermark-bounded streaming dedup operator to cut duplicates early with bounded state.
