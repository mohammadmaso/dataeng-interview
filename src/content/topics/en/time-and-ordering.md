---
order: 53
tid: "time-and-ordering"
track: "5"
section: null
num: "5.7"
title: "Time, Clocks & Ordering"
type: "concept"
level: "advanced"
priority: "P1"
est_hours: 2
summary: "Physical vs logical clocks (Lamport, vector clocks), clock skew, and why event ordering is guaranteed only within a partition."
covers: ["event time vs ingestion time vs processing time","NTP","hybrid logical clocks"]
interview_qs: []
extras: []
prerequisites: ["distributed-fundamentals"]
related: ["consistency-models", "kafka-architecture", "kafka-producers", "consensus", "kafka-consumers"]
---
## 🎯 TL;DR

> Clocks on different machines disagree, and messages take variable time. So "happened before" must be defined by **causality** (what could have influenced what), not by the timestamp printed on an event. Distributed systems get reliable ordering from **logical clocks**, from **a single log**, or from **bounded clock uncertainty**, and each has a scope.

- **Physical clocks** (wall clocks synced by NTP) drift and skew. They can even jump backwards. Never use them alone to order events across machines.
- **Lamport clocks** give a counter that respects causality, but a smaller number does not prove that one event came first.
- **Vector clocks** detect whether two events are causally related or **concurrent**.
- **Hybrid logical clocks (HLC)** keep a timestamp close to physical time while still respecting causality.
- In data systems, the reliable order is usually **the order inside one partition or log** (a Kafka offset, a database log position). Across partitions there is no global order.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Three friends write notes on paper, each with a wristwatch that is a few seconds off. A note stamped 10:00:05 may have been written before one stamped 10:00:03, because the watches disagree. To order the notes truly, you need to know who wrote what after reading what. "I wrote this after I read your note" is more reliable than any watch.

Here is a concrete failure. Two servers record events for the same user:

```text
server A clock: 10:00:05.200  -> event "order placed"
server B clock: 10:00:04.900  -> event "payment captured"

Sorted by timestamp:  payment captured, then order placed
Real order:           order placed, then payment captured
```

Sorting by timestamp put the payment before the order. The clocks were a few hundred milliseconds apart, which is common without careful sync. A pipeline that trusts those timestamps will build a wrong story.

What do we use instead? Either a **sequence number** assigned by a single source (for example a database log position), or a clock that counts causal steps, not seconds.

## 🔧 How it really works (Mid)

### Two kinds of time on one machine

- **Wall-clock time** (time of day) is set by NTP, a protocol that syncs machines to time servers. Typical accuracy on a good local network is milliseconds, and it can be worse on busy or badly configured machines. The clock can step backwards when NTP corrects it.
- **Monotonic time** never goes backwards, but has no meaning across machines. Use it to measure durations on one machine.

Timestamps from wall clocks are good for "roughly when" and for human dashboards. They are weak for "which came first".

### Lamport clocks

Each process keeps a counter `L`:

1. Before each local event, `L = L + 1`.
2. When sending a message, attach the current `L`.
3. When receiving a message with value `m`, set `L = max(L, m) + 1`.

Worked example with three processes:

```text
P1: a (L=1) ---- send m (L=2) ------------------+
P2: c (L=1)                                    |
P2: receive m -> L = max(1, 2)+1 = 3   <-------+   event r
P2: d (L=4)
P3: e (L=1)
```

Guarantee: if `a` happened before `b` (causally), then `L(a) < L(b)`. The converse is false: `L(e) = 1` and `L(a) = 1` say nothing about whether `a` and `e` are related. Lamport numbers give a consistent total order (ties broken by process id), but that order can place concurrent events in an order that never happened.

### Vector clocks

Each process keeps a vector with one counter per process. Each event increments its own entry. Messages carry the whole vector, and the receiver takes the element-wise maximum before incrementing its own entry.

```text
P1: a -> [1,0,0]     send to P2
P2: b -> [1,1,0]     (received [1,0,0], then increments P2)
P3: d -> [0,0,1]     no contact with P1 or P2

Compare a=[1,0,0] with b=[1,1,0]: a <= b element-wise -> a happened before b
Compare a=[1,0,0] with d=[0,0,1]: neither <= the other -> concurrent
```

Vector clocks answer "are these concurrent?", which Lamport clocks cannot. The cost is a vector whose size grows with the number of processes, which is why they are used in small groups, such as replicated stores that track version history (Dynamo-style systems), and not for millions of clients.

### Hybrid logical clocks

An HLC timestamp is a pair: a physical component close to the local wall clock, and a logical counter that breaks ties and keeps causality when physical clocks are behind. An HLC value is a readable time and still orders causally related events. Databases such as CockroachDB use HLC-style timestamps for multi-version concurrency control.

### Bounded uncertainty: TrueTime

Google Spanner takes a different route. Its TrueTime API returns an interval `[earliest, latest]` that bounds the true time, using GPS and atomic clocks. A transaction waits out the uncertainty (commit wait) before it becomes visible, so later transactions get later timestamps. This buys external consistency, but it needs special hardware and infrastructure most teams do not have.

### Event time, ingestion time and processing time

For streaming data, three times matter:

| Time | Meaning | Example |
|---|---|---|
| Event time | When it happened at the source | the tap on the turnstile |
| Ingestion time | When the broker or gateway received it | Kafka `LogAppendTime` |
| Processing time | When your job handles it | the moment the window closes |

Windows should use event time. Watermarks (see [[watermark]]) tell the engine how far event time has progressed, and [[late-arriving-data]] is what arrives after that point. Processing time gives results that depend on how busy the cluster is, which is hard to reproduce.

### Ordering in Kafka

Kafka orders records **inside one partition**. The offset is the position in that partition's log, and every consumer of that partition sees the same order. Records with the same key go to the same partition (by the default partitioner), so per-key order is preserved for that partition. There is no order between partitions. A producer with idempotence enabled keeps order across retries within a partition (check your client's settings for the number of in-flight requests). See [[kafka-architecture]] and [[kafka-producers]].

## 🧠 Senior depth & trade-offs

- **Do not order by device time.** Mobile and IoT clients have unreliable clocks, sometimes wrong by hours. Store the device time as an event time, store the server receipt time as ingestion time, and flag events whose device time is far from receipt time. Decide explicitly which one drives your windows.
- **Use the log as the source of truth.** For CDC, the database's own log position (LSN in PostgreSQL, binlog position in MySQL) is the real commit order. Timestamps in rows may not match it, especially for long transactions that commit later than they started. See [[cdc]].
- **Cross-partition questions.** "Which event happened first across two partitions?" has no answer from Kafka alone. You need a causal key (the same entity in the same partition), a sequence number from the producer, or a store that assigns versions.
- **Clock skew bugs.** Symptoms: rows that appear to change "before" they were created, negative durations, dedup windows that miss duplicates, and last-write-wins conflicts where the wrong write wins. Measure NTP offset on every host, and alert on it.
- **Last-write-wins is a clock decision.** LWW resolves conflicts by timestamp. It silently discards writes when clocks disagree. Use it only when losing an update is acceptable, or use version vectors and let the application merge.
- **Late data and watermarks.** A watermark that is too aggressive drops legitimate late events. A watermark that is too loose keeps state forever. Choose the allowed lateness from the measured delay distribution, not from a guess.
- **What to monitor.** Clock offset per host, event-time lag (now minus max event time processed), watermark lag, the share of late events, and out-of-order rates per key.

## 🧪 Try it

1. Two events, `a` and `b`, have Lamport timestamps 4 and 7. Can you conclude that `a` happened before `b`? What can you conclude about two events with timestamps 4 and 4 on different processes?
2. Given vector timestamps `x=[2,1,0]` and `y=[2,3,0]`, which event happened first, or are they concurrent?
3. A stream aggregates "orders per 5-minute window" by ingestion time. Mobile clients buffer events for up to 20 minutes when offline. What goes wrong, and what would you change?

<details><summary>Show solutions</summary>

**1.** No. Lamport timestamps are necessary but not sufficient for "happened before": `a` may be causally unrelated to `b`. A smaller timestamp does not prove causality, and equal timestamps on different processes say nothing about order.

**2.** `x` happened before `y`, because every element of `x` is less than or equal to the matching element of `y` (2<=2, 1<=3, 0<=0) and `x` differs from `y`. Concurrency would need neither vector to be element-wise less than or equal to the other.

**3.** The buffered events arrive late, after their 5-minute windows have already been emitted, so counts are wrong or missing. Ingestion time puts them in the window they arrived in, not the window they happened in. Change the windowing to event time, set a watermark with an allowed lateness that matches the measured offline delay (or accept a correction window), and publish updated results for late windows. Also validate device time against receipt time to catch clock problems.

</details>

## 💬 Interview corner

**Question: "How do you order events across machines?"**

- Mid: "I do not trust wall-clock timestamps alone. I use a sequence from one source, like a log offset or a database commit position, or a logical clock if I need causality."
- Senior: "Ordering is per scope. Within a partition or a single log, the offset is the order. Across partitions I need a causal key or versions. For event-time analytics, I'd use event time with watermarks and an explicit allowed lateness, and monitor clock offset on every host, because skew silently breaks dedup windows and last-write-wins."

**Question: "What is the difference between Lamport and vector clocks?"** Lamport gives a counter that respects causality in one direction. Vector clocks can also say that two events are concurrent.

> **🎤 Say this in the interview:** "Timestamps from different machines are estimates, not an order. I establish order from a single log, from causality, or from bounded uncertainty. For streaming, I pick event time explicitly and design for late data."

## ⚠️ Common mistakes

> **⚠️ Watch out:** "Sort by `created_at` across all services and you have the true order." Clock skew and late commits break this.

> **⚠️ Watch out:** "Kafka guarantees order for the topic." It guarantees order inside a partition only.

- Using processing time for windows, so results change with cluster load.
- Using `time.time()` to measure durations. Use the monotonic clock.
- Assuming a Lamport timestamp shows causality in both directions.
- Forgetting that device clocks on phones and IoT sensors can be far off.
- Treating ingestion time as event time, which moves late data into the wrong window.

## ✅ Check yourself (quiz)

1. Which clock is best for measuring how long a function takes on one machine?
   - A) NTP-synced wall clock
   - B) Monotonic clock
   - C) A Lamport counter
   - D) Device time from the client

<details><summary>Answer</summary>

**B** — the monotonic clock never goes backwards, so it is safe for durations. Wall clocks can step when NTP corrects them.

</details>

2. In a Lamport clock system, what is guaranteed?
   - A) If `L(a) < L(b)`, then `a` happened before `b`
   - B) If `a` happened before `b`, then `L(a) < L(b)`
   - C) All events have distinct timestamps across processes
   - D) Timestamps match physical time within 1 ms

<details><summary>Answer</summary>

**B** — the causal order implies a smaller Lamport value. The reverse does not hold, so a smaller number does not prove causality.

</details>

3. Kafka has a topic with 6 partitions. A consumer reads two events for the same order key in one partition. Which statement is true?
   - A) Order across the whole topic is guaranteed
   - B) The events come in the offset order of that partition
   - C) Order is random because timestamps are not used
   - D) Each consumer group reorders them by event time

<details><summary>Answer</summary>

**B** — the order inside one partition is the offset order, and the same key always goes to the same partition with the default partitioner. There is no order across partitions.

</details>

4. **(Senior)** A pipeline builds a "latest status per order" table by taking the row with the largest `updated_at` from a stream of CDC events. Occasionally an old status overwrites a new one. What is the likely cause and the fix?
   - A) Kafka is dropping messages
   - B) Timestamps from different application servers are skewed, or a long transaction committed later with an old `updated_at`; order by the database log position (LSN or binlog offset), or use a version column that the database increments
   - C) The primary key is missing
   - D) The fix is to add more partitions

<details><summary>Answer</summary>

**B** — the wall-clock field is not the commit order. The log position of the source database is the real order of commits, so use it, or a monotonically increasing version assigned at write time.

</details>

5. **(Senior)** Why does Spanner's TrueTime need commit wait, and what does a team without such hardware do instead?
   - A) To hide network latency from clients
   - B) The API reports an uncertainty interval, and the transaction waits until its timestamp is certainly in the past, so later transactions get later timestamps; without that hardware, teams use a single log position, a version column, or a database that gives a hybrid or serializable order
   - C) To run backups
   - D) Commit wait is unnecessary when clocks are NTP-synced

<details><summary>Answer</summary>

**B** — the clock is uncertain by a bounded amount. Waiting out that uncertainty ensures that a later transaction cannot receive an earlier timestamp. Teams without this infrastructure rely on log positions, versions or a database that handles ordering for them.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Wall clock vs monotonic clock | Wall clock is for time of day and can jump; monotonic only measures durations on one machine |
| Lamport clock guarantee | Causal order implies a smaller timestamp; a smaller timestamp does not prove causality |
| What do vector clocks add? | They detect concurrent events, not just order |
| HLC | Physical-time-like timestamp plus a logical counter; keeps causality |
| Event time vs ingestion time | When it happened at the source vs when the system received it |
| Kafka ordering scope | Within one partition, by offset; not across partitions |
| Why not trust device time? | Phone and IoT clocks can be wrong by a lot |
| TrueTime idea | Bounded uncertainty interval plus commit wait |

## 🔗 Go further

- **[[consistency-models]]** shows which orderings each consistency level guarantees.
- **[[consensus]]** explains how a replicated log gives one agreed order.
- **[[kafka-architecture]]** and **[[kafka-producers]]** explain partitions, keys and offsets in practice.
- **[[watermark]]** and **[[late-arriving-data]]** cover event time in streaming.
- **[[cdc]]** explains why the database log is the best source of commit order.
