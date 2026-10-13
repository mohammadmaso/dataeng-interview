---
order: 89
tid: "kafka-exactly-once"
track: "8"
section: null
num: "8.5"
title: "Kafka Exactly-Once & Transactions"
type: "concept"
level: "senior"
priority: "P0"
est_hours: 3
summary: "Idempotent producer + transactions + read_committed. Atomic read-process-write, and its limits when the sink is outside Kafka."
covers: ["transactional.id","zombie fencing","EOS in Kafka Streams and Flink"]
interview_qs: []
extras: []
prerequisites: ["kafka-producers", "kafka-consumers"]
related: ["delivery-semantics", "kafka-streams", "flink", "pattern-exactly-once", "distributed-transactions"]
---

## 🎯 TL;DR

> Kafka's **exactly-once** guarantee is for **read → process → write inside Kafka**. It combines three things: an **idempotent producer** (retries do not duplicate), **transactions** (several writes and the input offsets commit together or not at all), and **`read_committed`** consumers (they never see aborted writes).

- Idempotence removes duplicates caused by **retries** of one producer.
- Transactions make a **batch of writes plus offset commits** atomic.
- **Fencing** stops an old, "zombie" instance from committing after a new one has started.
- Once the output leaves Kafka (a database, an HTTP API, S3), Kafka cannot help. You need an idempotent sink or a two-phase commit.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A bank transfer has two legs: debit account A, credit account B. Either both happen or neither does, and nobody should see the money "in flight". A Kafka transaction does the same for a group of messages and the bookmark (offset) of what was read.

The problem first. A producer sends a message, the network times out, and the producer retries. The first send may have succeeded, so the message now exists twice. That is **at-least-once** delivery. Similarly, a consumer that crashes after writing output but before committing its offset will process the same input again.

```text
at-most-once  : may lose      (commit first, then process)
at-least-once : may duplicate (process first, then commit)   <- what plain Kafka gives you
exactly-once  : no loss, no duplicates of the effect (within Kafka)
```

Kafka gives you the third option **only** for the scope it controls. The rest of this lesson is about where that scope ends.

## 🔧 How it really works (Mid)

**Step 1: idempotent producer.** Set `enable.idempotence=true` (the default in recent clients). The broker gives each producer session a **producer ID**. Every record carries a **sequence number per partition**. If a retry arrives with a sequence number the broker already stored, it is dropped.

- Guarantees: no duplicates from retries, per partition, for one producer session.
- Does **not** guarantee anything across an application restart. A restarted process gets a new producer ID.

**Step 2: transactions.** Set a stable `transactional.id`. The producer then:

1. `initTransactions()`: registers the ID with the **transaction coordinator** and bumps the **epoch**.
2. `beginTransaction()`, then `send(...)` to any number of partitions.
3. `sendOffsetsToTransaction(...)`: adds the consumer's input offsets to the same transaction.
4. `commitTransaction()` or `abortTransaction()`.

On commit, the coordinator writes **control markers** into each partition. Consumers configured with `isolation.level=read_committed` skip records from aborted transactions and never read past the **last stable offset** (LSO), which is the first offset of a transaction that is still open.

**Step 3: fencing zombies.** Each `transactional.id` maps to one current epoch. When a new instance calls `initTransactions()` with the same ID, the epoch increases. An older instance, perhaps frozen by a long GC pause or a network partition, still believes it is the owner. Its next request is rejected with `ProducerFencedException`, so it cannot commit its stale output.

**Step 4: read-process-write.** The Java sketch below is the standard pattern. It is a skeleton, so `enrich()` is yours to write.

```java
props.put("transactional.id", "billing-enricher-1");   // stable per instance, not per run
props.put("enable.idempotence", "true");
// consumer side:
// enable.auto.commit=false, isolation.level=read_committed

producer.initTransactions();
while (true) {
    ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
    if (recs.isEmpty()) continue;
    producer.beginTransaction();
    try {
        for (ConsumerRecord<String, String> r : recs) {
            producer.send(new ProducerRecord<>("enriched", r.key(), enrich(r.value())));
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition tp : recs.partitions()) {
            List<ConsumerRecord<String, String>> list = recs.records(tp);
            offsets.put(tp, new OffsetAndMetadata(list.get(list.size() - 1).offset() + 1));
        }
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
        producer.commitTransaction();
    } catch (ProducerFencedException e) {
        producer.close();   // we are a zombie: do not abort, just stop
        throw e;
    } catch (Exception e) {
        producer.abortTransaction();
    }
}
```

The key line is `sendOffsetsToTransaction`. The output records and the input progress are either both visible after commit, or neither is. A crash before commit means the input is re-read and the output is discarded.

**Higher-level APIs do the same thing for you.**

- **Kafka Streams:** set `processing.guarantee=exactly_once_v2` (the older `exactly_once` setting is deprecated). Check the docs for your version.
- **Flink:** the Kafka sink with `DeliveryGuarantee.EXACTLY_ONCE` opens a transaction per checkpoint and commits it when the checkpoint completes. Checkpointing must be enabled, and downstream readers must use `read_committed`.

## 🧠 Senior depth & trade-offs

**The scope boundary is the whole story.** Exactly-once inside Kafka means the *Kafka side effects* are atomic. An external sink, like a relational database or an HTTP API, is not part of the transaction. A crash between "write to DB" and "commit Kafka transaction" can write the row twice on retry. Options, from cheapest to strongest:

1. **Idempotent sink:** make the write an upsert keyed by a business key (`INSERT ... ON CONFLICT DO UPDATE`), or include the Kafka `(topic, partition, offset)` in the row so a replay overwrites instead of adding. This covers most real pipelines.
2. **Transactional outbox:** the business write and the outgoing event are written in one database transaction; a relay publishes the outbox to Kafka. The boundary moves into the database.
3. **Two-phase commit sink:** Flink's `TwoPhaseCommitSinkFunction` pattern, with a sink that supports prepare and commit. It works, but it is expensive and fragile across systems.

**Costs you must price in.**

- **Latency:** `read_committed` consumers can only see data at transaction commit. In Flink this is roughly the checkpoint interval; in other designs it is the commit interval. Shorter intervals mean more transactions and more overhead.
- **Timeouts:** a producer's `transaction.timeout.ms` must be below the broker's maximum. A transaction left open (a stuck producer) blocks the LSO for `read_committed` readers, so lag looks normal for `read_uncommitted` but grows for committed readers.
- **Throughput:** each transaction adds coordinator round trips and markers. Batch records into fewer, larger transactions where latency allows.
- **Operational fragility:** `transactional.id` must be stable per logical instance. If two live instances share an ID, they fence each other in a loop. If IDs change on every restart, old transactions are never cleaned up.

**What you monitor.** Fenced-producer errors (should be rare and explained), aborted-transaction rate, time from transaction open to commit, LSO lag for committed readers, and the output-to-input ratio for each stage.

**When NOT to use Kafka transactions.** If the sink is idempotent and you accept at-least-once with dedup at the sink, transactions add latency and complexity for no gain. If your pipeline never writes back to Kafka, `read_committed` on the producer side does not exist; use idempotent writes at the destination instead.

## 🧪 Try it

Predict first. A Kafka Streams app reads `payments`, writes `payment-summaries` (in Kafka) and also calls a fraud API for each event. With `processing.guarantee=exactly_once_v2` enabled:

- Which parts are exactly-once?
- What happens to a payment that the fraud API accepted, but the app crashed before the Kafka commit?

<details><summary>Your guess?</summary>

The Kafka writes into `payment-summaries` and the consumer offsets are exactly-once: they commit atomically or are discarded. The fraud API call is outside the transaction. If the app crashes after the API call but before commit, it reads the same payment again and calls the API again. Exactly-once does not cover that side effect. Fix it with an idempotent fraud API keyed by payment ID, or move the call out of the hot path and make it a separate, deduplicated stage.

</details>

## 💬 Interview corner

**Q: Does Kafka give you exactly-once delivery?**

> **🎤 Say this in the interview:** "Kafka gives exactly-once *processing* inside Kafka: idempotent producers stop retry duplicates, transactions commit output and input offsets atomically, and `read_committed` consumers hide aborted data. It does not extend to external systems automatically. For a database sink I would use an idempotent upsert keyed by the business ID, or a transactional outbox, and I would say that the end-to-end guarantee is only as strong as the sink."

**Q: What is a zombie producer and how does Kafka handle it?**

> **🎤 Say this in the interview:** "It is an old instance that is still running after a new one has taken its place, for example after a long GC pause. Kafka assigns each `transactional.id` an epoch. When the new instance initializes, the epoch increases, and the zombie's requests fail with `ProducerFencedException`. It must stop writing, so its stale output never commits."

## ⚠️ Common mistakes

- **"Exactly-once means the message travelled once over the network."** It is about the *effect* in Kafka, after duplicates are removed. Retries still happen.
- **Reusing one `transactional.id` across different instances.** Instances fence each other, and throughput collapses into constant restarts.
- **Assuming the database write is covered.** It is not, unless the sink is idempotent or part of a two-phase protocol.
- **Forgetting `read_committed` downstream.** Readers with the default `read_uncommitted` will see aborted and in-flight records, which breaks the guarantee for them.
- **Setting `transaction.timeout.ms` above the broker maximum.** The producer fails at startup.
- **Turning on exactly-once everywhere "for safety".** It adds latency and operational risk. Use it where the output is Kafka and duplicates are costly.

## ✅ Check yourself (quiz)

1. What does the idempotent producer guarantee?
   - A) No message is lost across an application restart
   - B) Retries from one producer session do not create duplicate records in a partition
   - C) Consumers never process a message twice
   - D) Records are delivered in global order across partitions

<details><summary>Answer</summary>

**B** — the broker uses the producer ID and per-partition sequence numbers to drop duplicate retries. It says nothing about restarts (a new process gets a new producer ID), about consumers, or about ordering across partitions.

</details>

2. Which consumer setting makes the consumer ignore records from aborted transactions?
   - A) `enable.auto.commit=false`
   - B) `isolation.level=read_committed`
   - C) `auto.offset.reset=latest`
   - D) `group.instance.id` set

<details><summary>Answer</summary>

**B** — `read_committed` filters out aborted records and stops at the last stable offset. The other options control commits, the starting position and membership, not visibility of transactional data.

</details>

3. Two instances start with the same `transactional.id`. What happens?
   - A) Both write normally and the broker merges their output
   - B) The newer instance bumps the epoch and the older one is fenced
   - C) The broker rejects the second instance at startup permanently
   - D) Both are silently sharing one producer ID

<details><summary>Answer</summary>

**B** — initialization increases the epoch for that ID. The older instance's requests fail with `ProducerFencedException`. If the two instances keep starting and overlapping, they keep fencing each other, which is why IDs must be stable per logical instance and not shared.

</details>

4. *(Senior)* A pipeline reads from Kafka, enriches each record, and upserts into PostgreSQL. Kafka is set to exactly-once. Is the end-to-end result exactly-once? Justify in two sentences.

<details><summary>Answer</summary>

No. The Kafka output and offsets are atomic, but the PostgreSQL write is outside the transaction, so a crash between the DB write and the Kafka commit replays the record and writes it again. It becomes effectively exactly-once only if the upsert is keyed by a stable business key or the write is part of an outbox or two-phase protocol.

</details>

5. What is the main latency cost of `read_committed` consumers?

<details><summary>Answer</summary>

Records become visible only when their transaction commits, so visibility is delayed by the commit interval (for Flink, the checkpoint interval). A long-open transaction also holds back the last stable offset for those consumers, so they see lag even when the topic has plenty of data.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Kafka exactly-once scope | Read → process → write **inside Kafka**; external sinks need their own idempotency |
| Idempotent producer | Producer ID + per-partition sequence numbers; drops retry duplicates |
| `transactional.id` | Stable per logical instance; ties the producer to an epoch |
| `sendOffsetsToTransaction` | Puts input offsets in the same transaction as the output |
| Zombie fencing | Higher epoch wins; old instance gets `ProducerFencedException` |
| `read_committed` | Consumers skip aborted data and stop at the last stable offset |
| Kafka Streams setting | `processing.guarantee=exactly_once_v2` (check your version) |
| Sink strategy | Idempotent upsert, transactional outbox, or two-phase commit |

## 🔗 Go further

- **kafka-consumers** covers the consumer side: rebalancing, offset commits and lag, which this lesson depends on.
- **delivery-semantics** compares at-most-once, at-least-once and exactly-once across systems, including the sink question in more depth.
- **pattern-exactly-once** shows the same ideas as a system-design pattern with a full pipeline.
- **distributed-transactions** explains two-phase commit and why it is expensive.
- The glossary entries [[exactly-once]] and [[idempotency]] give short definitions you can reuse in interviews.
