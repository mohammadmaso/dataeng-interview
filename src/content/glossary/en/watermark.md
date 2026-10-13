---
term: "Watermark"
aliases: ["watermarks"]
category: streaming
short: "A stream processor's estimate that no events older than time T are still expected, used to decide when event-time windows can close."
related: ["event-time", "late-arriving-data", "window-function", "checkpoint"]
---
## In plain words (Junior)
Imagine collecting exam papers at the end of a class. You cannot wait forever for a student who might still be writing, so you decide: "anything handed in within 10 minutes after the bell counts; after that I seal the pile." A watermark is that rule for streams. Events can arrive late and out of order, and the watermark tells the system how long it is willing to wait before declaring a time window finished.

## How it works (Mid)
Streams have two clocks: event time (when it happened) and processing time (when we saw it). A watermark `W(t)` is a heuristic statement: "all events with event time <= W are assumed to have arrived." Typically `W = max_event_time_seen - allowed_lateness`.

When the watermark passes the end of a window, the engine emits the result and can drop the window's state. Events arriving behind the watermark are "late": they are dropped, sent to a side output, or trigger a window update if allowed lateness is configured.

```
// Flink: tolerate events up to 10 s out of order
WatermarkStrategy
  .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
  .withTimestampAssigner((e, ts) -> e.eventTimeMs);

// Spark Structured Streaming
df.withWatermark("event_time", "10 minutes")
  .groupBy(window("event_time", "5 minutes")).count()
```

With several partitions or inputs, the operator watermark is the minimum across them, so one idle partition can stall everything unless idleness detection is on.

## Senior angle & interview tips
A watermark is a trade-off between latency and completeness: a small delay gives fast but incomplete results, a large delay gives complete results with higher latency and more state. It is a heuristic, not a guarantee, so late data policy is a product decision. Failure modes: stalled watermarks from idle Kafka partitions, skewed timestamps from a bad producer clock jumping the watermark far ahead (then everything is "late"), and unbounded state when watermarks do not advance. In Spark, watermarks also bound state for streaming aggregations and dedup.

Interview question: "What happens to a late event?" Model answer: if its window already closed relative to the watermark it is dropped or routed to a side output; I choose allowed lateness or a periodic batch correction depending on how exact the numbers must be.
