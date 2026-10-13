---
order: 94
tid: "streaming-state"
track: "8"
section: null
num: "8.10"
title: "State Management in Streaming"
type: "concept"
level: "senior"
priority: "P0"
est_hours: 4
summary: "Stateful processing: state stores, RocksDB, state TTL, checkpoints and savepoints, rescaling state and evolving its schema."
covers: ["keyed vs operator state","incremental checkpoints","state size growth problems"]
interview_qs: []
extras: []
prerequisites: ["stream-concepts", "time-and-ordering"]
related: ["flink", "kafka-exactly-once", "stream-joins", "spark-structured-streaming", "storage-engines"]
---
## 🎯 TL;DR

> **State** is what a streaming job remembers between events: running counts, open windows, join buffers, deduplication keys. Without state you can only process each event in isolation.

- State is stored **per key** (for example per user or per card), so it can be split across parallel workers.
- State lives in a **state backend**: on the JVM heap (fast, limited by memory) or in RocksDB on local disk (large, slower per access).
- **Checkpoints** periodically snapshot all state, so a failed job restarts from a consistent point instead of from zero.
- Two problems dominate in production: state that **grows without bound**, and **checkpoints that take longer than the checkpoint interval**. TTL, retention and state-size monitoring are the fixes.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A shop cashier keeps a notebook with one page per customer: "Ali bought 3 items today". If the cashier forgets the notebook, every customer looks new again. In streaming, the notebook is **state**, and the power cut is a **crash**.

Imagine a stream of clicks. You want the number of clicks per user so far:

```text
event: user=ali   click
event: user=sara  click
event: user=ali   click   -> ali count is now 2 (this needs memory of the earlier click)
```

To produce `2`, the job must remember `ali -> 1` from the first event. That remembered number is the state. Three facts to hold on to:

1. State belongs to a **key** (here `user`). Ali's count does not affect Sara's.
2. State must **survive restarts**. If the job crashes and forgets `ali -> 2`, the next click gives `1` again, which is wrong.
3. State can **grow forever** if you never throw anything away. A count per user is small; a list of every event per user is not.

The rest of this lesson is about these three points: how state is partitioned, how it is saved, and how it is bounded.

## 🔧 How it really works (Mid)

### Keyed state and operator state

- **Keyed state** exists per key and is only visible when the event with that key is being processed. Flink exposes it as `ValueState`, `ListState`, `MapState`, `ReducingState` and `AggregatingState`. Kafka Streams uses key-value stores in the same way.
- **Operator state** belongs to a parallel instance, not a key. Sources use it for things like Kafka partition offsets. When parallelism changes it is redistributed as lists.

Keyed state is the one you use in business logic. Keys are hashed into **key groups**. Each parallel task owns a contiguous range of key groups, which is what makes rescaling possible.

### State backends

| Backend | Where state lives | Strength | Limit |
|---|---|---|---|
| Heap (HashMap) | JVM objects in memory | Fast reads and writes | Bounded by heap; large heaps cause long GC pauses |
| RocksDB (embedded) | Serialized in an LSM tree on local disk | State can be far larger than RAM | Serialization and disk I/O per access; slower per record |

With RocksDB, reads and writes go through the LSM tree described in [[lsm-tree]], and the state for one task sits on that task's local disk.

### Checkpoints and savepoints

A **checkpoint** is an automatic, periodic snapshot. The coordinator injects a barrier into the sources; each operator snapshots its state when the barrier arrives and forwards it. When every operator has acknowledged, the checkpoint is complete. This is the Chandy-Lamport family of algorithms discussed in [[flink]] and [[kafka-exactly-once]].

```java
env.enableCheckpointing(60_000);                              // every 60 s
env.getCheckpointConfig().setCheckpointTimeout(10 * 60_000);  // give up after 10 min
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
```

- **Full checkpoints** copy all state every time.
- **Incremental checkpoints** (RocksDB only) upload only the SST files that changed since the last checkpoint. Each checkpoint references the files it still needs, so the cost is proportional to change, not to total size.

A **savepoint** is a checkpoint you trigger on purpose, in a portable format. You take one before an upgrade, a schema change, or a rescale, then start the new job from it.

### State TTL

Without TTL, a key's state stays until the job ends. TTL says "forget this value after N time without a write".

```java
StateTtlConfig ttl = StateTtlConfig
    .newBuilder(Duration.ofHours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();

ValueStateDescriptor<Long> clicks = new ValueStateDescriptor<>("clicks", Types.LONG);
clicks.enableTimeToLive(ttl);
```

- `OnCreateAndWrite` refreshes the timer on writes only; `OnReadAndWrite` also refreshes on reads.
- `NeverReturnExpired` makes expired values invisible at once. Removal from storage is **best effort**: expired entries can stay on disk until they are cleaned up during reads, background cleanup, or RocksDB compaction. Do not assume that TTL immediately frees space.
- TTL applies per entry in list and map state, so each element expires independently.

> **⚠️ Watch out:** TTL is measured in processing time by default. It bounds memory, but it is not a substitute for event-time logic such as windows with allowed lateness. Check the docs for your version before relying on a specific cleanup strategy.

## 🧠 Senior depth & trade-offs

**State growth is the main production risk.** The state size is roughly `number of live keys × bytes per key`. Consider 10 million users, each with a small value of about 100 bytes plus overhead, so roughly 10M × 150 B ≈ 1.5 GB per operator before replicas and checkpoint files. That looks fine. The same job keyed by `(user, session_id)` with a list of events per session, and no TTL, grows with every session ever seen: 10M users × 20 sessions × 2 KB ≈ 400 GB, and it never shrinks. Growth like this is usually a **design bug** (an unbounded key or join buffer), not a tuning problem.

Typical sources of unbounded state:

- Joins that buffer both sides with no time bound (see [[stream-joins]]).
- Keys that never close, such as session IDs with no inactivity gap or a user ID space that keeps expanding.
- Deduplication sets with no retention window (see [[deduplication]]).
- Windows with `allowedLateness` far larger than the business needs.

**Checkpoint duration is a leading indicator.** When the checkpoint time approaches the checkpoint interval, checkpoints overlap or fail, recovery points get old, and at-least-once replay grows. Two causes to separate:

- **Alignment time**: with aligned barriers, fast channels wait for the slowest one. Under [[backpressure]], this wait grows. Unaligned checkpoints let barriers overtake buffered data, which shortens alignment but makes checkpoints larger and adds some overhead.
- **Upload time**: full checkpoints of a large state are dominated by the network and object-store upload. Incremental RocksDB checkpoints help, but RocksDB compaction can rewrite many SST files at once, so the increment is not always small.

**Rescaling and max parallelism.** A savepoint restores into a new parallelism by reassigning key-group ranges. The number of key groups (max parallelism) is fixed when the state is first written. Choosing it too small limits future scale-out; changing it later is a state-migration problem. Set it deliberately, with headroom, at the start.

**Schema evolution.** State is serialized with a `TypeSerializer`. Flink supports some compatible changes (for example adding a field in certain Avro or POJO layouts, and in newer versions enabling TTL on existing state). Renames, type changes and incompatible serializers usually require a state migration job that reads old state and writes new state. Test the restore path from a savepoint before a release, not during an incident.

**When not to use keyed state.** If you only need the latest value per key for serving, a key-value store or table is simpler to query. If the state is huge and rarely updated, a periodic batch recompute may be cheaper. State is powerful, but every byte you keep is a byte you must checkpoint, restore and monitor.

**What to monitor:** total state size per operator, checkpoint duration and alignment time, checkpoint failure count, RocksDB or heap usage, state access latency, and the age of the last successful checkpoint.

## 🧪 Try it

Work on paper or in a notebook, then check the answers.

1. **Size estimate.** A job keeps one `ValueState<Long>` per `card_id`. There are 5 million active cards. Each key costs about 40 bytes of value and overhead, and checkpoints keep 3 completed versions. Estimate the live state size and the disk footprint with 3 retained checkpoints, ignoring incremental sharing.
2. **TTL choice.** A fraud job keeps "last seen merchant" per card for duplicate detection. Business rules say a repeated charge within 2 hours is a duplicate. What TTL would you set, and why is 2 hours not enough as a window for "customers who return within a month"?
3. **Checkpoint diagnosis.** A job has a 60 s checkpoint interval. Checkpoints take 45 s on average, then start failing with timeouts after the state doubles. List two causes you would check first, in order.

<details><summary>Show solutions</summary>

**1.** Live state: 5,000,000 × 40 B = 200,000,000 B ≈ 200 MB. With 3 retained full checkpoints: 3 × 200 MB ≈ 600 MB on the checkpoint store, plus the working copy on the task managers. With incremental checkpoints the stored total is smaller, but the 200 MB estimate is the right order of magnitude to plan capacity.

**2.** A TTL of 2 hours (with `OnCreateAndWrite`, so the timer refreshes on each new charge) keeps exactly the information needed for the duplicate rule. "Returns within a month" needs 30 days of state per card, which is 360× more time and therefore up to 360× more entries for keys that stay active. Such a rule belongs in a batch job or a separate store, not in a long-lived streaming timer.

**3.** First check whether the checkpoint is slow to align (backpressure, skewed key groups, a hot key). Second check upload time (state size growth, RocksDB compaction creating large increments, object-store throughput). Then decide between reducing per-key state, tuning parallelism, or switching to unaligned checkpoints if alignment dominates.

</details>

## 💬 Interview corner

**Question: "How does Flink keep state consistent after a failure?"** Mid answer: "It takes periodic checkpoints of the state and restarts from the last one." Senior answer: "Checkpoints are barrier-aligned snapshots, so each operator's state corresponds to the same position in the input. On restart, sources rewind to the offsets in the checkpoint and state is restored. Combined with transactional sinks, that gives exactly-once results, not just exactly-once state."

**Question: "A stateful job's checkpoints keep timing out. What do you do?"** Mid: "Increase the timeout and the parallelism." Senior: "I separate alignment from upload time in the checkpoint metrics first. Alignment points to backpressure or skew; upload points to state size or compaction churn. Then I check which keys have unbounded state and whether TTL or a redesign is the real fix."

> **🎤 Say this in the interview:** "I think about state in three questions: which key owns it, where it lives, and when it is forgotten. I'd set TTL and monitor state size before the job goes to production, because growing state is what causes checkpoint failures months later."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Assuming TTL frees disk space immediately. Expired values are hidden at once but removed later, so disk usage can stay high for a while.

> **⚠️ Watch out:** Changing the key of a stateful operator without a migration. State is partitioned by the old key, so the new job will not find its state.

- Using a join with no time bound on either side, then wondering why state keeps growing.
- Setting the checkpoint interval shorter than the checkpoint duration, so checkpoints queue up.
- Ignoring max parallelism when first deploying, then being unable to scale out cleanly.
- Storing large objects (full event payloads) in state when only a small summary is needed.
- Treating a savepoint as a backup you never test restoring from.

## ✅ Check yourself (quiz)

1. Where is keyed state partitioned?
   - A) On the job manager
   - B) By key, so each parallel task owns a range of key groups
   - C) Once per operator, shared by all tasks
   - D) In the Kafka topic only

<details><summary>Answer</summary>

**B** — keyed state is partitioned by key into key groups, and each parallel task owns a contiguous range. This is why state can be redistributed when parallelism changes.

</details>

2. You have 2 TB of state and a fast SSD on each task manager. Which backend fits best?
   - A) Heap backend, because it is always fastest
   - B) RocksDB backend, because state can exceed memory and it supports incremental checkpoints
   - C) Operator state, because it stores everything in one list
   - D) No backend; state must stay in Kafka

<details><summary>Answer</summary>

**B** — RocksDB keeps state on local disk, so it scales beyond RAM and supports incremental checkpoints. The heap backend is faster per access but is limited by heap size and GC pauses.

</details>

3. A TTL is set to 24 hours with `OnCreateAndWrite`. A key is read every hour but never written. When does its state expire, and why?

   - A) Never, because reads are allowed
   - B) About 24 hours after the last write, because reads do not refresh the timer under `OnCreateAndWrite`
   - C) After 1 hour, because the most recent read counts
   - D) Immediately, because TTL is ignored for reads

<details><summary>Answer</summary>

**B** — under `OnCreateAndWrite`, only creation and writes refresh the TTL. Use `OnReadAndWrite` if reads should count as activity.

</details>

4. **(Senior)** A job keyed by `(user_id, session_id)` keeps all events per session with no TTL. Over a month, checkpoint duration grows from 20 s to 4 min. What is the root cause, and what would you change first?

   - A) The checkpoint interval is too long
   - B) Unbounded state growth from keys that never close; add a session timeout or TTL and review retention
   - C) RocksDB cannot store lists
   - D) The fix is more task managers with the same state

<details><summary>Answer</summary>

**B** — state grows with every session ever seen, so checkpoint size and upload time grow with it. Closing sessions by inactivity gap and bounding retention address the cause; adding machines only moves the problem.

</details>

5. **(Senior)** Why does `max parallelism` matter for state, even if you never plan to scale beyond your current parallelism?

   - A) It sets the number of Kafka partitions
   - B) It fixes the number of key groups in the state, which limits how far the job can later scale out
   - C) It controls the checkpoint interval
   - D) It only affects batch jobs

<details><summary>Answer</summary>

**B** — the key-group count is set when state is written. Choosing it too low caps future scale-out, and changing it later means migrating state, so set it deliberately with headroom.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What is state in streaming? | Information a job keeps between events, such as counts, windows or join buffers |
| Keyed vs operator state | Keyed: per key, used in business logic. Operator: per parallel instance, for example source offsets |
| Heap vs RocksDB backend | Heap is fast but bounded by memory; RocksDB is on disk, larger than RAM, slower per access |
| Checkpoint vs savepoint | Checkpoint is automatic recovery; savepoint is user-triggered and portable for upgrades and rescaling |
| Incremental checkpoint | Uploads only changed SST files (RocksDB); cost follows change, not total size |
| TTL with `OnCreateAndWrite` | Timer refreshes on create and write, not on read |
| Does TTL free disk immediately? | No; expired entries are hidden at once and removed later (best effort) |
| Main cause of state growth | Unbounded keys or joins without time bounds, and retention set longer than the business needs |

## 🔗 Go further

- **stream-concepts** and **time-and-ordering** explain event time and windows, which decide what state is needed.
- **stream-joins** shows which join types need bounded buffers.
- **kafka-exactly-once** explains how checkpoints and transactional sinks combine into end-to-end guarantees.
- **spark-structured-streaming** covers the state store model in Spark, to compare with Flink.
- Read the state and checkpointing pages of the Flink docs for your exact version before tuning anything in production.
