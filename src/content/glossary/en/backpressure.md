---
term: "Backpressure"
aliases: ["back-pressure", "back pressure"]
category: streaming
short: "A feedback mechanism where a slow consumer makes upstream producers slow down instead of being overwhelmed."
related: ["consumer-lag", "checkpoint", "dead-letter-queue"]
---
## In plain words (Junior)
Picture a sink in a busy kitchen: if water drains slowly, you turn the tap down instead of letting it overflow. Backpressure is the same for data. When a downstream step can't keep up, it signals upstream to slow down, so nothing overflows or crashes.

## How it works (Mid)
Without flow control, a fast producer fills the buffers of a slow consumer until memory runs out or data is dropped. Strategies: block the producer (bounded queues, Flink network buffers with credit-based flow control), pull-based consumption (Kafka consumers fetch at their own pace, so the log itself absorbs the burst), rate limiting, load shedding (drop or sample), or buffering to durable storage.

```
source ──► map ──► keyBy/window ──► sink(slow DB)
                                      ▲ buffers fill
          ◄──── credit/queue-full signal propagates upstream ────
# Flink: watch backPressuredTimeMsPerSecond / busyTimeMsPerSecond per task
# Reactive Streams: subscriber calls request(n); producer sends at most n
```

In Flink, a bottleneck operator fills its input buffers, which propagates upstream task by task to the source, which then reads less from Kafka; lag grows in Kafka rather than memory in Flink. Spark Structured Streaming uses `maxOffsetsPerTrigger` to bound batch size.

## Senior angle & interview tips
Backpressure is a symptom, not a fix: find the slow operator (the one with high busy time whose upstream is backpressured), then fix cause: slow sink, skewed key, heavy state access, GC or too little parallelism. Dangers: checkpoint barriers get stuck behind backpressured data so checkpoints time out (unaligned checkpoints help), latency rises, and a long backpressure turns into consumer lag and possibly data loss when retention expires. Push-based systems without flow control (HTTP webhooks, UDP) need explicit queues or shedding.

Interview question: "What happens when a sink is slower than the source?" Model answer: with bounded buffers and credit-based flow control the slowness propagates upstream to the source, which reads less; the lag accumulates in the durable log instead of memory, and I'd then scale or fix the bottleneck operator.
