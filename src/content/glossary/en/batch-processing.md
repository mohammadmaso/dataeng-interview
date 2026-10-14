---
term: "Batch processing"
aliases: ["batch job","batch pipeline"]
category: batch
short: "Processing a bounded chunk of data at once, on a schedule."
related: ["stream-processing","etl-elt","backfill","dag"]
---
## In plain words (Junior)
Collect data for a while (an hour, a day), then process it all in one run, like doing laundry once a week instead of washing each shirt the moment you take it off.

## How it works (Mid)
A batch job reads a bounded input (files, a table partition), transforms it, and writes an output, typically triggered by an orchestrator. Engines: Spark, Hive, dbt on a warehouse, plain SQL. Benefits: high throughput, simple retries, easy reprocessing, cheap compute. Cost: latency equal to the schedule interval plus run time.

## Senior angle & interview tips
Make batches idempotent and partition-aligned (one run = one date partition, overwrite on rerun) so retries and backfills are safe. Watch for late data crossing batch boundaries. Many "real-time" needs are satisfied by micro-batches of a few minutes.

Interview question: "Batch vs streaming trade-off?" Answer: batch trades latency for simplicity, throughput and easy correctness; streaming trades complexity for freshness.
