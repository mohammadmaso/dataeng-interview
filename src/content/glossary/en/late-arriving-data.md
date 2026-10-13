---
term: "Late-arriving data"
aliases: ["late data", "late events", "out-of-order events", "late-arriving facts", "late-arriving dimensions"]
category: streaming
short: "Records that arrive after the time period or window they belong to has already been processed."
related: ["watermark", "event-time", "backfill", "scd"]
---
## In plain words (Junior)
Imagine a postman delivering yesterday's letters today because of a delay. If you already closed yesterday's books, you must decide: reopen the books and add the letters, or record them as today's. Late data in pipelines is the same problem: events show up after you've already calculated the results for their time period.

## How it works (Mid)
Causes: mobile clients offline, network retries, upstream batch delays, clock skew, replays. Late means event time is older than the current processing position. Two common cases: late facts (events for a past day) and late dimensions (a fact references a customer not yet in the dimension).

```
-- Batch: re-process a trailing window each run instead of only "yesterday"
DELETE FROM daily_agg WHERE dt >= current_date - 3;
INSERT INTO daily_agg SELECT event_date, ... FROM events
WHERE event_date >= current_date - 3 GROUP BY 1;

-- Streaming (Flink): accept events up to 1 hour late, route the rest aside
.window(TumblingEventTimeWindows.of(Time.minutes(5)))
.allowedLateness(Time.hours(1))
.sideOutputLateData(lateTag)
```

Strategies: (1) lookback window, recompute the last N days each run; (2) event-time processing with watermarks plus allowed lateness, emitting updated results; (3) side output/DLQ for too-late data and periodic correction batches; (4) for dimensions, insert an inferred placeholder row (`Unknown`/stub) and update it when the real record arrives, with SCD2 correction if needed.

## Senior angle & interview tips
Decide acceptable lateness with the business: it sets both correctness and latency. Late updates mean consumers see changing numbers; downstream tables must handle retractions/upserts, and dashboards should show data freshness and "final" status. Partition by event date but ingest by arrival date (keep `ingested_at`) so late rows are findable and you can reprocess affected partitions only. Exactly-once is irrelevant if lateness silently drops rows: always count what was discarded.

Interview question: "How do you handle events arriving two days late?" Model answer: keep processing in event time, partition by event date, reprocess affected partitions from a lookback window idempotently, and for streaming use allowed lateness with updates plus a side output for later correction, while measuring and alerting on late-event volume.
