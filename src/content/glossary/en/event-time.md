---
term: "Event time vs processing time"
aliases: ["event time", "processing time", "ingestion time"]
category: streaming
short: "Event time is when something actually happened; processing time is when the system handled it. Correct analytics usually needs event time."
related: ["watermark", "late-arriving-data", "sessionization"]
---
## In plain words (Junior)
You buy a coffee at 11:59 pm but the card terminal uploads the sale at 12:03 am. Which day's sales does it belong to? The day you bought it (event time), not the day the bank heard about it (processing time). Streaming systems must tell these two clocks apart.

## How it works (Mid)
Three timestamps often coexist: event time (generated at the source), ingestion/arrival time (when the broker or lake received it) and processing time (when the job ran). Events arrive out of order and late due to network and batching, so grouping by processing time yields results that depend on system load and retries, not on reality.

```
{"user": 7, "action": "purchase",
 "event_time":  "2024-05-01T23:59:58Z",   // when it happened on the phone
 "ingest_time": "2024-05-02T00:03:10Z"}   // when Kafka/our system saw it

-- Which day does this purchase belong to? Event time: May 1.
-- Processing-time bucketing would wrongly put it on May 2.
```

Event-time processing assigns events to windows by their own timestamp and uses watermarks to know when a window is complete. Processing-time is simpler and lowest-latency but non-deterministic: rerunning the same data on another day yields different results, which breaks replays and backfills. Event-time results are reproducible.

## Senior angle & interview tips
Use event time for business metrics, processing time for operational ones (throughput, SLA monitoring of the pipeline itself). Pitfalls: untrusted client clocks (cap timestamps, fall back to server time when absurd), time zones and DST (store UTC), mixed timestamp sources across systems, and the cost of event time: state must be kept until the watermark passes, so more memory and latency. In Kafka, `CreateTime` vs `LogAppendTime` topic config matters. In batch, the same idea means partition by event date and keep an `ingested_at` column.

Interview question: "Why can't a streaming job just use the clock of the machine?" Model answer: because arrival order and delay vary, so results would be non-deterministic and change on replay; event time with watermarks gives reproducible, correct windows at the cost of buffering state and tolerating lateness.
