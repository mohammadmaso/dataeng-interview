---
order: 96
tid: "spark-structured-streaming"
track: "8"
section: null
num: "8.12"
title: "Spark Structured Streaming"
type: "technology"
level: "advanced"
priority: "P1"
est_hours: 4
summary: "The micro-batch model, output modes (append, update, complete), watermarks and stateful operations, checkpoint location, foreachBatch for custom idempotent sinks."
covers: []
interview_qs: []
extras: []
prerequisites: ["spark-architecture", "stream-concepts"]
related: ["streaming-state", "stream-joins", "time-and-ordering", "spark-shuffle", "pattern-late-data", "kafka-exactly-once"]
---

## 🎯 TL;DR

> Structured Streaming treats a stream as a **table that keeps growing**. You write the same DataFrame or SQL you would use on a batch, and Spark runs it repeatedly on **micro-batches** of new data.

- The default model is a **micro-batch**: each trigger reads new input, updates the result, and writes to the sink.
- **Output mode** (`append`, `update`, `complete`) decides what is written each trigger.
- **Watermarks** tell Spark how late data may arrive, so it can drop old state and finalise windows.
- The **checkpoint location** stores offsets and state. It is what gives you restart and recovery. Without it, you do not have a production query.
- **foreachBatch** hands each micro-batch to your own code. Use it for sinks that have no streaming writer, but make the write idempotent yourself.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Think of a **mail room** that opens the mailbag every 10 seconds. It sorts whatever arrived since the last check, updates the running tally on the whiteboard, and writes the tally to the report. You do not handle each letter one by one; you handle a bag at a time.

A minimal streaming word count, reading from a socket for demo purposes:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("wordcount").getOrCreate()

lines = (spark.readStream.format("socket")
         .option("host", "localhost").option("port", 9999).load())

words = lines.select(F.explode(F.split("value", " ")).alias("word"))
counts = words.groupBy("word").count()

query = (counts.writeStream
         .outputMode("complete")
         .format("console")
         .option("checkpointLocation", "/tmp/chk/wordcount")
         .start())

query.awaitTermination()
```

Three things to learn from it:

- The code is almost the same as a batch job. You change `read` to `readStream` and `write` to `writeStream`.
- `complete` mode prints the whole table each trigger, because the count of every word can change.
- The checkpoint directory is required for most sinks. Delete it and the query starts from scratch.

> **⚠️ Watch out:** Streaming does not mean "instant". Each micro-batch has overhead. Latency is usually seconds, not milliseconds.

## 🔧 How it really works (Mid)

### The micro-batch loop

Each trigger runs these steps:

1. Ask the source for new data (for Kafka: the offset range from the last committed batch to the latest).
2. Write the offset range to the checkpoint **before** processing (the offset log).
3. Run the query on that range, reading and updating the state store if the query is stateful.
4. Write the result to the sink.
5. Commit the batch to the commit log in the checkpoint.

If the driver crashes between steps, Spark reads the offset log on restart and re-runs the same range with the same input. This is how Spark gets its guarantees: a **replayable source** (Kafka, files) plus a **sink that can ignore duplicates** gives exactly-once results.

### Triggers

| Trigger | Behaviour | Use when |
|---|---|---|
| Default (no trigger) | Next micro-batch as soon as the previous one finishes | Low latency, enough cluster capacity |
| `processingTime="10 seconds"` | Fixed interval | Predictable load and cost |
| `availableNow=True` | Processes all data available now, in several batches, then stops | Incremental batch-style jobs |
| Continuous | Experimental, very low latency, few operators supported | Check the docs for your version; rarely a default choice |

### Output modes

| Mode | What is written each trigger | Allowed for |
|---|---|---|
| `append` | Only new rows that will never change | Plain selects, and aggregations with watermarks |
| `update` | Rows that changed since the last trigger | Aggregations; sinks that support upserts |
| `complete` | The entire result table | Aggregations only; the result must fit in memory |

For a windowed aggregation with `append`, Spark waits until the watermark passes the end of a window before it writes the window. The window is then final and can be appended once. With `update`, the same window can be written several times as late data changes it.

### Watermarks and event time

Event time is the timestamp inside the record. Processing time is when Spark sees it. Records arrive late, so Spark needs a limit.

```python
from pyspark.sql import functions as F

clicks = (events
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"), "user_id")
    .count())
```

The watermark is roughly: `max(event_time seen so far) − 10 minutes`. A window is closed when the watermark passes its end. State older than the watermark is removed, and records later than the watermark are dropped.

A worked example with numbers:

```text
window = [10:00, 10:05)
watermark delay = 10 min
largest event_time seen = 10:16  → watermark = 10:06
→ window [10:00, 10:05) is closed; state is dropped
→ a record with event_time 10:03 that arrives now is discarded
```

> **💡 Analogy:** The watermark is the **cut-off time for late post**. Letters postmarked before the cut-off are accepted; anything after it goes to the lost-letter bin.

### Stateful operations

Aggregations, deduplication (`dropDuplicates`), stream-stream joins and `mapGroupsWithState` keep state across batches. The state lives in **state stores**, partitioned like a shuffle, and it is checkpointed. Without a watermark, the state for `groupBy` on an unbounded key grows forever. That is the most common production failure.

You can choose the state store implementation. The RocksDB-backed provider keeps large state off the JVM heap; check the docs for your version for the exact configuration key.

### foreachBatch

`foreachBatch` gives you each micro-batch as a normal batch DataFrame plus a `batchId`:

```python
def write_batch(batch_df, batch_id):
    # batch_df is a regular DataFrame; you can use any batch sink or MERGE
    (batch_df.write
        .mode("append")
        .parquet(f"/lake/clicks_counts/batch_{batch_id}"))  # the batch id names the output

query = (counts.writeStream
    .foreachBatch(write_batch)
    .outputMode("update")
    .option("checkpointLocation", "/checkpoints/clicks")
    .start())
```

By default this is **at-least-once**: if the job fails after the write but before the commit, the same `batch_id` runs again. To make it idempotent:

- write to a location keyed by `batch_id` and overwrite it, or
- use a `MERGE` into a table keyed by the business key (a late retry then updates the same row), or
- store the last committed `batch_id` in the same transaction as the data.

The example above names files by batch id, but a second run writes a second copy. A real pipeline would overwrite that path or use a `MERGE`.

### Reading Kafka

```python
events = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "clicks")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 2_000_000)   # backpressure: cap per batch
    .load()
    .selectExpr("CAST(value AS STRING) AS json"))
```

`maxOffsetsPerTrigger` is how you cap batch size. Without it, a backlog becomes one enormous micro-batch that may not fit in memory.

## 🧠 Senior depth & trade-offs

**The watermark is a latency/completeness trade-off.** A longer watermark accepts more late data but keeps more state and delays final results. A 1-hour delay on a 5-minute window keeps about 12 active windows per key. For high-cardinality keys this is the main memory cost. Choose the delay from the measured lateness distribution of your source, not from a guess.

**State size estimate.**

```text
keys per window          = 2,000,000 active users
windows kept at once     = 10-minute watermark / 5-minute windows → about 3
state rows               = 2,000,000 × 3 = 6,000,000
bytes per row (approx.)  = 100 B  → about 600 MB of state
```

Multiply by the number of state partitions and add headroom. Measure with the query progress metrics before you deploy.

**Shuffle partitions.** Stateful operations shuffle data. `spark.sql.shuffle.partitions` (default 200) sets the partition count for this shuffle. Too few partitions give huge state files per task; too many give many tiny files and overhead. For streaming, a fixed value sized for your peak throughput is common. See [[spark-shuffle]].

**Backpressure and sizing.** The source pulls up to `maxOffsetsPerTrigger` per batch. If processing time per batch is longer than the trigger interval, batches queue up and latency grows. Watch the batch duration against the trigger interval. Two rules of thumb:

- Keep processing time per batch comfortably below the trigger interval.
- Size the cluster for peak input, not average, or your lag builds at every peak.

**Schema and checkpoint compatibility.** The checkpoint records the query plan and state layout. Changing the query (adding a group key, changing an aggregation, changing the state operator) usually cannot restart from the old checkpoint. Plan migrations: start a new query with a new checkpoint, backfill if needed, then switch consumers.

**Skewed keys.** One hot key puts all its state and work on one task. Mitigate with a two-stage aggregation (salt the key, aggregate, then combine). See [[hot-key]].

**Exactly-once.** Spark's guarantee holds for the query's own state and for sinks that support it (the file sink with its metadata log, Delta, Iceberg). A custom `foreachBatch` sink is at-least-once unless you make it idempotent. Do not promise "exactly-once end to end" without naming the sink.

**Stream-stream joins** need watermarks on both inputs (for outer joins) and a time-range condition. Without them, state for the join can grow forever. See [[stream-joins]].

**Failure modes.**

- Unbounded state from missing watermarks, which ends in executor memory pressure or a slow checkpoint.
- Changing the query, then restarting from the old checkpoint, which fails with an incompatible state error.
- A slow sink (for example, a database under load) that makes each batch longer until the trigger interval is exceeded.
- Deleting the checkpoint "to fix" an error, which replays or skips data depending on the source's retention.

**What to monitor.** Input rate against processing rate, batch duration, `numInputRows` per batch, state rows and memory per operator, watermark value and its lag behind wall-clock time, and the offset lag of the source. Spark exposes these in `query.lastProgress` and through a `StreamingQueryListener`.

**When NOT to use it.** If you need per-record sub-second latency, use a record-at-a-time engine such as Flink. If the job is a daily load with no freshness requirement, a batch job is simpler and cheaper.

## 🧪 Try it

Run the Kafka example locally (Spark with the Kafka connector package for your Spark version). Then:

1. Produce 10 events with timestamps in the same 5-minute window, one with an `event_time` 30 minutes in the past.
2. Run the windowed count with `withWatermark("event_time", "10 minutes")` and `outputMode("append")`. Predict which rows are printed and when.
3. Stop the query and restart it with the **same** checkpoint. Confirm it does not reprocess the old batches.
4. Change the group key and restart with the same checkpoint. Read the error. Then restart with a new checkpoint.
5. Make the `foreachBatch` writer idempotent: write to a path keyed by batch id, then rerun the batch and confirm the output is unchanged.

```python
# Quick check: inspect the last batch's metrics
print(query.lastProgress)   # input rows, batch duration, watermark, state operators
```

> **🎤 Say this in the interview:** "I think of a streaming query as a batch query run on micro-batches with offsets and state checkpointed. I always set a watermark on stateful operations, because without one state grows without bound. For sinks I use a streaming-aware writer where I can, and with foreachBatch I make the write idempotent using the batch id or a MERGE. I size the cluster for peak input and watch batch duration against the trigger interval."

## 💬 Interview corner

**Q: What is the difference between append, update and complete?**
- **Junior:** Append adds only new rows. Update writes rows that changed. Complete writes the whole result each time.
- **Mid:** Append on an aggregation needs a watermark, so each window is written once when it is final. Complete needs the whole result in memory and only fits small aggregations.
- **Senior:** Pick the mode that the sink can handle. Append to a lake is simple and final. Update needs a sink that supports upserts. Complete is for small dashboards. Each choice changes the duplicate and correction behaviour downstream.

**Q: What does a watermark do?**
- **Mid:** It sets how late data may be. Spark drops state and rejects records older than the watermark, and it finalises windows once the watermark passes them.
- **Senior:** It is a trade-off between completeness and memory. I set it from measured lateness and report how many records get dropped, so the business knows what is missing.

**Q: How do you get exactly-once with Spark and Kafka?**
- **Mid:** Replayable offsets plus a checkpoint give replay on failure. The sink must not duplicate. The file sink and table formats such as Delta or Iceberg handle that; a plain foreachBatch needs an idempotent write.
- **Senior:** Name the sink first. The guarantee holds for the sink's commit protocol, and the rest of the pipeline is at-least-once. See [[kafka-exactly-once]] for the broader picture.

**Model answer (senior):** Structured Streaming runs a batch query on micro-batches, with offsets and state in the checkpoint. I use watermarks on every stateful operation to bound state, choose an output mode the sink supports, and make foreachBatch writes idempotent. I track batch duration against the trigger interval and state size, and I treat changes to the query as migrations that need a new checkpoint.

## ⚠️ Common mistakes

- **Stateful aggregation without a watermark.** State grows forever.
- **Assuming `complete` is the default for dashboards.** It rewrites the whole result each trigger.
- **Reusing an old checkpoint after changing the query.** Restart fails or state is misread.
- **Deleting the checkpoint to fix an error** without knowing what gets replayed or skipped.
- **Calling foreachBatch "exactly-once".** It is at-least-once unless the write is idempotent.
- **Ignoring `maxOffsetsPerTrigger`**, so a backlog becomes one enormous batch.
- **Confusing event time and processing time** in windows, which silently moves events into the wrong window after an outage.

## ✅ Check yourself (quiz)

1. Which output mode is valid for a streaming select with no aggregation and no watermark?
   - A) complete
   - B) update
   - C) append
   - D) None of them

   <details><summary>Answer</summary>

   **C** — Without aggregation, rows are final once they arrive, so append is the natural mode. Complete needs an aggregation, and update is for rows that change.

   </details>

2. What happens to a record whose event time is older than the current watermark?
   - A) It is written to the sink immediately
   - B) It is dropped from stateful processing, so it does not change finalised windows
   - C) It restarts the query
   - D) It is moved to the current time

   <details><summary>Answer</summary>

   **B** — Records behind the watermark are discarded for stateful operations, because their windows are considered closed and their state has been removed.

   </details>

3. **Scenario:** A query fails on restart with an error about incompatible state after you added a group key. What is the fix?
   - A) Increase executor memory
   - B) Start the query with a new checkpoint location, and backfill or migrate the output as needed
   - C) Switch to complete mode
   - D) Delete the source topic

   <details><summary>Answer</summary>

   **B** — The checkpoint stores the old state layout. A changed query cannot use it, so you start fresh and handle the output history separately.

   </details>

4. **Senior trade-off:** Why does a longer watermark increase cost?
   - A) It increases the number of partitions
   - B) It keeps state for windows longer, so more state is held in memory and checkpointed
   - C) It disables the checkpoint
   - D) It forces the continuous trigger

   <details><summary>Answer</summary>

   **B** — The state for each open window stays until the watermark passes it. A longer delay means more open windows and more state to store and checkpoint.

   </details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Micro-batch | Each trigger processes the new offset range as a small batch |
| Append mode | Writes rows that will not change; aggregations finalise after the watermark |
| Update mode | Writes rows changed since the last trigger |
| Complete mode | Writes the whole result table; aggregations only |
| Watermark | Lateness limit: `max(event_time) − delay`; old state is dropped |
| Checkpoint | Offset log, commit log and state; required for recovery |
| foreachBatch | Gives each micro-batch and its `batchId` to your code; at-least-once by default |
| `maxOffsetsPerTrigger` | Caps records per batch from a source like Kafka |
| `availableNow` | Processes available data in batches, then stops |

## 🔗 Go further

- [[streaming-state]] — how state is stored and recovered
- [[stream-joins]] — joins with watermarks on both sides
- [[pattern-late-data]] — handling late and out-of-order records end to end
- [[kafka-exactly-once]] — what exactly-once means across Kafka and sinks
- [[spark-shuffle]] — why stateful operators shuffle and how partitions affect them
- Spark Structured Streaming programming guide for your version (triggers, state store configuration and metrics)
