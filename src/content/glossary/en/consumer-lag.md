---
term: "Consumer lag"
aliases: ["lag", "kafka lag", "consumer group lag"]
category: streaming
short: "How far a consumer is behind the newest message in a partition, measured as log end offset minus committed offset."
related: ["consumer-group", "backpressure", "partition"]
---
## In plain words (Junior)
Think of a conveyor belt delivering boxes while a worker packs them. If the worker packs slower than boxes arrive, a pile of unpacked boxes grows. The size of that pile is the consumer lag. A small stable pile is normal; a pile that keeps growing means the worker cannot keep up.

## How it works (Mid)
For each partition, lag = log end offset (latest produced message) minus the consumer group's committed offset. Total lag is the sum across partitions. It can be measured in messages or converted into time ("we are 4 minutes behind").

```
kafka-consumer-groups.sh --bootstrap-server b:9092 \
  --describe --group billing

# GROUP   TOPIC   PART  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# billing orders  0     1200            1500            300
# billing orders  1     980             985             5
```

Common causes: slow processing (database writes, external calls), too few consumers or partitions, a rebalance storm, a hot partition, a poison message causing retries, or a producer burst. Fixes depend on cause: scale consumers up to the partition count, increase partitions (with ordering caveats), batch writes, optimize the handler, or fix the poison message with a DLQ.

## Senior angle & interview tips
Alert on lag trend and time-lag, not a fixed message count; a steady lag of 10k may be fine for a topic with 100k msg/s. Watch per-partition lag, since an average hides one stuck partition. Caveats: committed offset is not the same as processed offset (commit-before-process loses data, commit-after-process may duplicate), and lag can look zero while the consumer is dead if monitoring reads stale data. If retention is shorter than lag, data is deleted before it is read, which is data loss. Tools: Burrow, Kafka exporters, Cruise Control.

Interview question: "Lag is growing; what do you check?" Model answer: per-partition lag first (skew vs global), consumer count versus partitions, processing time per batch, rebalances and errors; then scale, batch, or isolate bad messages.
