---
term: "Exactly-once semantics"
aliases: ["exactly-once", "exactly once", "EOS", "effectively-once"]
category: distributed
short: "Each record affects the final result exactly one time even though delivery may be retried; usually achieved by idempotence or transactions."
related: ["idempotency", "two-phase-commit", "checkpoint", "outbox-pattern"]
---
## In plain words (Junior)
Imagine sending a bank transfer by mail. The postman may lose the letter (so you resend), or deliver two copies (if you resend by mistake). Exactly-once means that no matter how many letters are sent or lost, the money moves exactly one time. Networks cannot guarantee a message is physically delivered once, so systems make the effect happen once.

## How it works (Mid)
Delivery guarantees: at-most-once (may lose), at-least-once (may duplicate), exactly-once (neither, in effect). True exactly-once delivery over an unreliable network is impossible; what systems provide is exactly-once processing effects, built from two ingredients:

1. Replayable sources (offsets) plus atomic state: the engine checkpoints source positions and state together (Flink checkpoints, Spark offsets + WAL).
2. Idempotent or transactional sinks: writes either dedupe by key or commit atomically with the offsets (Kafka transactions, two-phase commit sinks).

```
# Kafka producer + transactional consume-transform-produce
enable.idempotence=true
transactional.id=job-42

# consumer reading only committed data
isolation.level=read_committed
```

The guarantee holds inside the pipeline boundary; side effects outside it (HTTP calls, emails) need their own idempotency keys.

## Senior angle & interview tips
Kafka exactly-once covers read-process-write between Kafka topics via idempotent producers and transactions; once you write to an external system, you need a transactional or idempotent sink (e.g. Flink two-phase commit sink, Iceberg commit tied to checkpoint). Costs: higher latency (commits happen at checkpoint or transaction boundaries), `read_committed` waits for open transactions, and zombie producers need fencing. Many teams choose at-least-once plus idempotent sinks because it is simpler and just as correct.

Interview question: "Is exactly-once possible?" Model answer: not as network delivery, but as an effect: replay from offsets plus atomic commit of state and output, or idempotent writes; I would state precisely the boundary where the guarantee holds.
