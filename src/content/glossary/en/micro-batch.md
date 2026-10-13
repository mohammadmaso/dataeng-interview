---
term: "Micro-batch processing"
aliases: ["micro-batching", "microbatch", "spark structured streaming trigger", "batch vs streaming"]
category: streaming
short: "Streaming implemented as a series of small batch jobs run every few seconds or minutes, trading latency for throughput and simplicity."
related: ["checkpoint", "backpressure", "exactly-once"]
---
## In plain words (Junior)
A bus that leaves every 5 minutes carries whoever has gathered at the stop, while a taxi leaves immediately for each passenger. Micro-batching collects data for a short moment and processes it together, which is efficient but not instant.

## How it works (Mid)
Each trigger reads the new offsets/files since the last batch, runs a normal batch plan on it, writes the output and commits progress in a checkpoint. Because every batch is deterministic and replayable from recorded offsets, exactly-once with idempotent or transactional sinks is straightforward. Latency is bounded below by the trigger interval plus job overhead (hundreds of ms to seconds, typically).

```
# Spark Structured Streaming: micro-batch every 30 s
(spark.readStream.format("kafka").option("subscribe", "events").load()
  .writeStream.format("iceberg")
  .trigger(processingTime="30 seconds")
  .option("checkpointLocation", "s3://b/ckpt/events")
  .start("db.events"))
# vs Flink: record-at-a-time (true streaming), latency in ms
```

Native streaming engines (Flink, Kafka Streams) process each record as it arrives with continuous operators and state, giving lower latency (ms). Spark offers micro-batch (default) and a continuous mode with limited support.

## Senior angle & interview tips
Trade-offs: micro-batch gives high throughput, batch-style optimizations, easy reuse of batch code and simpler fault recovery; costs are latency floors and per-batch overhead (scheduling, small files in sinks). Short triggers create small files; long triggers add latency, so pick based on the SLA and compact. State cleanup depends on watermarks. Event-time windows work the same as in record-at-a-time engines. Most "real-time" business needs are satisfied by seconds-to-minutes latency; true low-latency needs (fraud, alerting in ms) justify Flink-like engines.

Interview question: "Spark Structured Streaming vs Flink?" Model answer: Spark uses micro-batches over the batch engine, which suits throughput, unified batch/streaming code and moderate latency; Flink is a native streaming engine with finer state handling, event-time features and millisecond latency; choose by the latency SLA and team skills.
