---
term: "Checkpoint"
aliases: ["checkpoints", "checkpointing", "savepoint", "snapshot barrier"]
category: streaming
short: "A consistent snapshot of a job's state and input positions, used to recover after failure without losing or double-counting data."
related: ["exactly-once", "wal", "backpressure"]
---
## In plain words (Junior)
Think of saving a video game. If the game crashes, you reload the last save instead of starting from level one. A checkpoint is a save point for a streaming job: it records how far the job has read and what it remembers, so after a crash it continues from there.

## How it works (Mid)
A checkpoint captures operator state plus source offsets at one logically consistent point. In Flink, a coordinator injects barriers into streams; each operator snapshots its state when the barrier passes (Chandy-Lamport style) and acknowledges, so the snapshot is consistent without stopping the job. State goes to durable storage (S3, HDFS), incrementally with RocksDB.

```
# Flink
execution.checkpointing.interval: 60s
execution.checkpointing.mode: EXACTLY_ONCE
state.checkpoints.dir: s3://bucket/ckpt
# Spark Structured Streaming
df.writeStream.option("checkpointLocation", "s3://bucket/ckpt/job1").start()
```

On failure, the job restarts, restores state from the last completed checkpoint, rewinds sources to the saved offsets, and replays. Combined with transactional or idempotent sinks, this yields exactly-once effects. Savepoints are user-triggered, portable checkpoints used for upgrades and migration. Spark stores offsets and state in the checkpoint directory per query.

## Senior angle & interview tips
Checkpointing trades overhead and latency for recovery time: shorter intervals mean less replay but more I/O and pressure; long intervals mean longer recovery and larger output replays for non-transactional sinks. Output only becomes visible at commit for two-phase-commit sinks, so end-to-end latency equals about the checkpoint interval. Failure modes: checkpoint timeouts under backpressure, huge state making checkpoints slow, incompatible state after code changes (operator uid matters), and checkpoint directories that cannot be reused across different logic. Retained checkpoints can serve as recovery after cancel.

Interview question: "How does Flink recover from a node crash?" Model answer: it restores operator state from the latest completed checkpoint, resets sources to the checkpointed offsets, and replays records since then; with idempotent or transactional sinks the output has exactly-once effect.
