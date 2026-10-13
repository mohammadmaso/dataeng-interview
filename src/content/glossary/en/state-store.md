---
term: "State store (stateful stream processing)"
aliases: ["state backend", "rocksdb state", "keyed state", "stateful processing", "stateful operator"]
category: streaming
short: "The local, fault-tolerant storage where a streaming operator keeps data between events, such as running counts, windows and join buffers."
related: ["checkpoint", "exactly-once", "watermark"]
---
## In plain words (Junior)
To answer "how many orders has this customer placed today?" a worker must remember the earlier orders. The place where the stream job keeps what it must remember is its state. The system saves this memory regularly, so if the machine dies, nothing is forgotten.

## How it works (Mid)
Stateless operators (map, filter) need no memory. Stateful ones (aggregations, windows, joins, deduplication, pattern matching) keep state partitioned by key, so each parallel task owns the state of its keys and updates it locally with low latency. State lives in memory or an embedded store (RocksDB on local disk) and is snapshotted to durable storage with checkpoints for recovery; on rescale, key groups are redistributed.

```
// Flink keyed state: count per user
public class Counter extends KeyedProcessFunction<String, Event, Out> {
  private ValueState<Long> count;
  public void open(Configuration c) {
    count = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Long.class));
  }
  public void processElement(Event e, Context ctx, Collector<Out> out) throws Exception {
    long n = (count.value() == null ? 0 : count.value()) + 1;
    count.update(n); out.collect(new Out(e.user, n));
  }
}
// state.backend: rocksdb (spills to local disk; incremental checkpoints to S3)
```

State must be bounded: use TTL, windows closed by watermarks, and compact representations. Kafka Streams keeps state in local RocksDB stores backed by changelog topics; Spark Structured Streaming stores per-batch state versions in HDFS/S3 checkpoint.

## Senior angle & interview tips
State size drives everything: checkpoint duration, recovery time, memory and disk, and rescaling cost. Unbounded state (dedup without TTL, joins without time bounds, sessions never closing) is the classic production failure. Hot keys produce skewed state. Schema evolution of state is hard: changing the state type or operator uid can invalidate savepoints, so plan serialization (Avro/Protobuf) and operator ids. Choose heap backend for small fast state, RocksDB for large state (slower, serialization per access). Queryable state or external lookups (Redis) are alternatives to local state, with latency and consistency costs.

Interview question: "A Flink job's checkpoints keep getting slower; why?" Model answer: probably growing state (no TTL, unbounded join/dedup) or backpressure delaying barriers; I'd bound state with TTL/watermarks, use incremental RocksDB checkpoints, check skew, and consider unaligned checkpoints.
