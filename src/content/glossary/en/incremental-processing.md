---
term: "Incremental processing"
aliases: ["incremental load", "incremental model", "delta load", "high-water mark", "change detection"]
category: batch
short: "Processing only data that is new or changed since the last run instead of recomputing everything."
related: ["backfill", "upsert", "idempotency", "watermark"]
---
## In plain words (Junior)
Instead of recounting the whole warehouse stock every night, you just note what came in and went out today and adjust the total. Incremental processing handles only the new or changed data, which saves time and money as data grows.

## How it works (Mid)
Full reloads cost grows with total data; incremental runs cost grows with change volume. Techniques: high-water mark on a monotonic column (`updated_at`, auto-increment id), partition-based (reprocess the newest N partitions), CDC streams (apply inserts/updates/deletes), file/offset tracking for ingestion (new files in S3, Kafka offsets), and table-format incremental reads (Iceberg snapshot diffs).

```
-- dbt incremental model
{{ config(materialized='incremental', unique_key='order_id') }}
SELECT * FROM {{ source('app','orders') }}
{% if is_incremental() %}
WHERE updated_at >= (SELECT max(updated_at) - interval '3 hours' FROM {{ this }})  -- overlap!
{% endif %}
-- merge on unique_key => idempotent despite the overlap
```

To stay correct: use an overlap or lookback window for late-arriving data, merge/upsert by key so reprocessed rows replace rather than duplicate, handle deletes (soft-delete flag or CDC), and provide a full-refresh path to rebuild when logic changes.

## Senior angle & interview tips
Failure modes: `updated_at` not maintained by all writers (changes missed), clock skew and transaction commit order making a high-water mark skip rows committed late (use overlap or log-based CDC), deletes invisible to query-based extraction, non-idempotent appends, and state drift from small errors accumulating until a full refresh. Always reconcile counts/checksums between source and target periodically. Incremental aggregates need mergeable operations. Decide the unit of incrementality (row, partition, file) per source reliability.

Interview question: "Why subtract a few hours from the high-water mark?" Model answer: transactions commit out of order and data arrives late, so a strict greater-than max timestamp can miss rows; overlapping and merging by key keeps the load idempotent and complete.
