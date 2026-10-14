---
term: "Stream processing"
aliases: ["streaming","stream processing engine"]
category: streaming
short: "Processing events continuously as they arrive instead of in periodic batches."
related: ["micro-batch","event-time","watermark","state-store"]
---
## In plain words (Junior)
Instead of waiting to collect a day of data and then crunching it, a stream processor handles each event within milliseconds or seconds of its arrival, like a cashier scanning items one by one rather than counting the whole cart at the end.

## How it works (Mid)
A streaming job reads from a log such as Kafka, applies transformations (filter, map, join, aggregate over windows), keeps state in a local store, and writes results to a sink. Engines: Flink (true record-at-a-time), Kafka Streams, Spark Structured Streaming (micro-batch by default). Correctness depends on event time, watermarks, checkpointing and idempotent or transactional sinks.

## Senior angle & interview tips
The real design questions are delivery semantics, state size and recovery time, handling late and out-of-order data, and reprocessing (replay from the log). Streaming is not always better: if the business only needs hourly data, batch is simpler and cheaper.

Interview question: "When would you not use streaming?" Answer: when latency requirements are loose, the team lacks operational maturity for stateful jobs, or results need heavy full-history recomputation.
