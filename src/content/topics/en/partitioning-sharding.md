---
order: 42
tid: "partitioning-sharding"
track: "4"
section: null
num: "4.5"
title: "Partitioning & Sharding"
type: "concept"
level: "advanced"
priority: "P0"
est_hours: 4
summary: "Splitting data across nodes: range, hash, consistent hashing, and time-based partitioning. Hot spots and hot keys, and rebalancing."
covers: ["secondary indexes on partitioned data (local vs global)","request routing","partition vs bucket in Hive/Spark"]
interview_qs: []
extras: []
prerequisites: ["distributed-fundamentals", "replication"]
related: ["consistency-models", "cap-pacelc", "nosql-modeling", "pattern-hot-keys", "physical-modeling-olap", "mpp-architecture", "spark-shuffle"]
---
## 🎯 TL;DR

> **Partitioning** splits one large dataset into smaller pieces. **Sharding** is partitioning across machines, where each machine owns some of the pieces. The goal: each piece is small enough to store and fast enough to query, and you can add machines without rewriting everything.

- **Range** partitioning keeps neighbours together. It is good for range scans and risky for keys that always grow (timestamps).
- **Hash** partitioning spreads keys evenly. It is good for point lookups and bad for range scans.
- **Time-based** partitioning is the default in warehouses and lakes. It makes "the last 7 days" cheap.
- The real enemies are **skew** (one partition is much bigger or busier than the rest) and **rebalancing** (moving data when the number of machines changes).

## 🌱 Start simple (Junior)

> **💡 Analogy:** A library with a million books. You can shelve them alphabetically by author (range), by a code that spreads authors evenly across ten rooms (hash), or by publication year (time). Each shelving scheme makes some questions fast and others slow.

Suppose a table of users has 8 rows and you have 4 servers. Use `id mod 4` as a stand-in for a real hash function:

```text
Hash partitioning (id mod 4):
server 0: ids 4, 8
server 1: ids 1, 5
server 2: ids 2, 6
server 3: ids 3, 7

Range partitioning (by id):
server A: ids 1-2
server B: ids 3-4
server C: ids 5-6
server D: ids 7-8
```

Now ask two questions:

- "Give me user 6." Both layouts send you to exactly one server. Partitioning helps point lookups.
- "Give me users 3 to 4." Range partitioning reads only server B. Hash partitioning spreads these ids over two servers, and with a larger range it would touch all of them.

Partitioning makes the engine **skip** the pieces it does not need, and lets machines work in parallel on the rest.

## 🔧 How it really works (Mid)

**Terms.** People use "partition" and "shard" loosely. In Postgres or a warehouse, a partition is often a piece of one table on one machine. A shard is usually a piece of the data on a separate machine, reached through routing. In Hive and Spark, a partition is a directory per value (for example `dt=2026-01-01`).

**The three classic strategies:**

| Strategy | How a row picks its partition | Good for | Bad for |
|---|---|---|---|
| Range | Key falls in `[lo, hi)` | Range scans, time windows, ordered data | Monotonic keys (all new writes hit the last range), uneven ranges |
| Hash | `hash(key) mod N` or a ring position | Even spread, point lookups | Range scans (results scatter), changing N |
| List / directory | Key equals a value (country, tenant) | Clear business boundaries, per-tenant isolation | Very uneven sizes (one big tenant) |

Declarative range partitioning in PostgreSQL (version 10 and later) looks like this:

```sql
CREATE TABLE events (id bigint, ts timestamptz, payload text)
  PARTITION BY RANGE (ts);

CREATE TABLE events_2026_01 PARTITION OF events
  FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

EXPLAIN SELECT * FROM events
WHERE ts >= '2026-01-10' AND ts < '2026-01-11';
-- The plan scans only events_2026_01 (partition pruning).
```

**Why `mod N` hurts on resize.** With `hash mod N`, changing N moves almost everything. Going from 4 to 5 servers keeps a key only when `h mod 4 = h mod 5`. That holds for 4 of every 20 residues, so about **80% of keys move**. Copying 80% of a cluster just to add one node is not acceptable.

**Consistent hashing** fixes this. Hash both keys and nodes onto a ring; each key belongs to the next node clockwise. Adding a node takes over only the arc before it, roughly `1/N` of the keys (about 20% when going from 4 to 5 nodes). Systems such as Cassandra and Dynamo-style stores add **virtual nodes** (many ring positions per machine) so the load evens out and a new machine takes a little from everyone. See [[consistent-hashing]].

**Rebalancing strategies:**

- **Fixed number of partitions.** Create many more partitions than machines (for example 1,000 for 10 machines). Moving a whole partition to a new machine is cheap, and only partition-to-machine assignments change.
- **Dynamic splitting.** Range stores (HBase, Bigtable-style) split a partition in two when it grows past a threshold and merge small ones.
- **Automatic vs manual.** Automatic rebalancing can start heavy data movement during a traffic spike. Many teams keep a human in the loop for large moves.

**Secondary indexes on partitioned data** come in two forms:

- **Local index** (document-partitioned): each partition indexes only its own rows. Writes touch one partition. A query by a non-key column must **scatter-gather** to every partition. Cassandra's built-in secondary indexes work this way.
- **Global index** (term-partitioned): the index itself is partitioned by the indexed value. Reads go to one index partition, but a write may update index partitions on other machines, so it is slower or asynchronous. DynamoDB global secondary indexes are asynchronous in this sense.

**Request routing** answers "which machine has this key?". The client can hold the partition map, a routing tier can forward requests, or any node can forward to the owner. A metadata service (ZooKeeper, etcd or the database's own catalogue) keeps the map current.

**Partition vs bucket in Hive and Spark.** A Hive or Spark **partition** is a directory per value, so `WHERE dt = '2026-01-10'` lists one directory. A **bucket** is a fixed number of files per partition, chosen by `hash(column) mod B`. Buckets make sampling and bucketed joins possible (two tables bucketed the same way on the join key can join without a shuffle). Partitions prune by value; buckets organise rows inside a partition.

## 🧠 Senior depth & trade-offs

- **Measure skew, do not guess.** Use the ratio `max partition size / average partition size` and the same ratio for request rate. Example: 10 partitions hold 100 GB; one holds 40 GB. The average is 10 GB, so the skew factor is 4x, and that one node is the bottleneck even though the cluster looks balanced.
- **Hot keys defeat hashing.** Hashing spreads *distinct* keys. One celebrity user, one big tenant or one "today" key still lands on one partition. Remedies: split the key (`key + random suffix 0..9`, then merge on read), add a salt, cache the hot key, or give the big tenant its own partition. See [[hot-key]] and [[salting]].
- **The time-partition trap.** Partitioning by `event_date` is excellent for pruning, but all new writes go to today's partition. Make sure the write rate of one day fits one partition's throughput, or sub-partition the current day by hour or by a hash bucket.
- **Partition count is a trade-off.** Too few partitions cap parallelism and create large, slow files. Too many create small-file and metadata overhead (the Hive metastore and object listing grow with the count). Size partitions to your recovery and compaction budget, and check the docs for your engine rather than trusting a fixed number.
- **Cross-shard operations are expensive.** Joins across shards need data movement, and transactions across shards need a protocol such as two-phase commit. Unique constraints that are not on the partition key are hard to enforce. Design so that the common query stays on one shard: co-locate related rows by a shared key (for example `tenant_id` in every table).
- **What to monitor.** Partition size distribution, per-partition QPS and latency percentiles, the number of partitions per query (a scatter-gather is a warning sign), and rebalancing progress.
- **When not to shard.** If one well-tuned machine (or a read replica set) handles the load and the data, sharding adds routing, operations and consistency problems for no gain. Shard when you have to, not when you can imagine needing to.

## 🧪 Try it

Use PostgreSQL 10 or later.

1. **Pruning.** Create a range-partitioned `events` table with two monthly partitions (January and February 2026). Run `EXPLAIN` on a query for one day in February. Which partition appears in the plan?
2. **Skew.** Table `orders` has partition key `customer_id`. One customer has 60% of the rows. Explain why hashing the key does not fix this, and propose two fixes.
3. **Rebalancing.** Write a short Python script that hashes 10,000 keys (for example with `hashlib.md5`), computes the fraction whose partition changes between 4 and 5 buckets with `hash % n`, and compares it with consistent hashing with 4 and 5 nodes (you can estimate it as about 1/5 of keys moving).

<details><summary>Show solutions</summary>

**1.** Only the February partition, for example `events_2026_02`. The planner prunes the others because the predicate is a constant range on the partition key.

**2.** Hashing spreads distinct keys. All rows of one customer share the same key, so they all land in the same partition. Fixes: split that customer's rows across several partitions with a suffix (`customer_id, bucket = hash(order_id) % 8`) and aggregate across them, or isolate the hot customer in a separate partition or table.

**3.**
```python
import hashlib

def h(key: str) -> int:
    return int.from_bytes(hashlib.md5(key.encode()).digest()[:8], "big")

keys = [f"user-{i}" for i in range(10_000)]
moved = sum(1 for k in keys if h(k) % 4 != h(k) % 5)
print(round(moved / len(keys), 2))  # about 0.8
```
Consistent hashing with 4 to 5 nodes moves roughly 1/5 (about 0.2) of the keys, because only the new node's arc changes owner.

</details>

## 💬 Interview corner

**Question: "How would you shard a user table with 2 billion rows?"** Mid answer: "Hash the user id across N shards, with a routing layer, and use consistent hashing so that adding shards moves only a fraction of the data."

Senior answer: "I'd start from the access pattern. If almost every query is by user id, I shard by user id with consistent hashing and many virtual partitions, so resharding moves whole partitions. Email lookups need a global index or a separate lookup table keyed by email. I'd measure skew and plan hot users separately, and accept that cross-shard joins and unique constraints on other columns are expensive."

**Question: "Partition or bucket in Spark?"** A partition is a directory per value and enables pruning. A bucket is a hash-based file split inside a partition and enables bucketed joins and sampling.

> **🎤 Say this in the interview:** "I choose the partition key from the most common filter, and I check its distribution before I commit. Hashing gives even spread but kills range scans, range partitioning gives scans but risks hot spots, and time partitioning gives cheap pruning but concentrates writes. Rebalancing with `mod N` moves almost everything, so I use consistent hashing or a fixed number of partitions."

## ⚠️ Common mistakes

> **⚠️ Watch out:** A partition key that does not appear in your `WHERE` clauses gives you no pruning. You pay the cost of partitioning and still scan everything.

> **⚠️ Watch out:** Partitioning by a column with very few values (for example `country` with 3 values) creates a few huge partitions. Partition by a column with a sensible number of values and check the sizes.

- Choosing a monotonic key (an auto-increment id or a timestamp) for range partitioning without thinking about where new writes go.
- Using `hash mod N` and then being surprised that adding a node moves 80% of the data.
- Assuming hashing solves hot keys. It does not; one key is still one partition.
- Expecting a global unique constraint or a cross-shard join to be as cheap as a local one.
- Creating thousands of tiny daily partitions and then wondering why the metastore and the file listing are slow.
- Confusing Spark `repartition` (an in-memory shuffle) with `partitionBy` on write (directories on storage).

## ✅ Check yourself (quiz)

1. Which strategy is most likely to create a hot partition when the partition key is an ever-increasing timestamp?
   - A) Hash partitioning
   - B) Range partitioning
   - C) List partitioning
   - D) Consistent hashing with virtual nodes

<details><summary>Answer</summary>

**B** — with range partitioning, every new row falls in the newest range, so one partition takes all the writes. Hashing spreads the same keys across partitions.

</details>

2. You shard with `hash(key) mod 4` and then grow to 5 nodes. Roughly what fraction of keys move?
   - A) 5%
   - B) 20%
   - C) 50%
   - D) 80%

<details><summary>Answer</summary>

**D** — a key stays only when `h mod 4 = h mod 5`, which holds for 4 of every 20 residues. About 80% of keys move. Consistent hashing would move about 20%.

</details>

3. A query filters by `email`, but the table is partitioned by `user_id` with local secondary indexes. What happens?
   - A) It reads one partition
   - B) It fails, because the index is local
   - C) It scatter-gathers to every partition
   - D) It is always faster than a global index

<details><summary>Answer</summary>

**C** — a local index covers only its own partition, so the query must ask every partition and merge the results. A global index on `email` would route the read to one index partition, at the cost of slower or asynchronous writes.

</details>

4. **(Senior)** Your daily partitions are fine for pruning, but the ingestion job for "today" is slow and the partition is huge. Which change addresses the root cause with the least damage?

   - A) Remove partitioning and use one table
   - B) Sub-partition the current day by hour or by a hash bucket, keeping the daily layout for pruning
   - C) Increase the number of daily partitions to a thousand
   - D) Partition the table by `customer_id` instead

<details><summary>Answer</summary>

**B** — the issue is the write concentration on one partition. Sub-partitioning the newest data spreads writes and keeps pruning by day. Option A loses pruning, option C adds metadata overhead, and option D trades the time-pruning problem for a skew problem.

</details>

5. **(Senior)** Why is a cross-shard transaction a trade-off and not a free feature of sharding?

   - A) Shards cannot store transactions
   - B) It needs a coordination protocol across machines, adding latency and failure modes such as blocked participants
   - C) Transactions are only allowed on one machine by law
   - D) It requires an index on every column

<details><summary>Answer</summary>

**B** — a transaction across shards needs a protocol such as two-phase commit, which adds round trips and can block when a participant or the coordinator fails. Designing so that most transactions stay on one shard avoids most of this cost.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Partitioning vs sharding | Partitioning splits data; sharding is partitioning across machines, each owning some pieces |
| Range vs hash partitioning | Range keeps neighbours together (good scans, hot spots on monotonic keys); hash spreads evenly (good lookups, bad scans) |
| Why `hash mod N` is painful | Going from N to N+1 nodes moves N/(N+1) of keys; 4 to 5 nodes moves about 80% |
| Consistent hashing | Keys and nodes on a ring; adding a node moves about 1/N of the keys |
| Local vs global secondary index | Local: write one partition, read all (scatter-gather). Global: read one index partition, write may touch many |
| Partition pruning | The engine skips partitions that cannot match the predicate on the partition key |
| Hot key | One key with disproportionate traffic; hashing does not fix it, salting or splitting does |
| Hive partition vs bucket | Partition is a directory per value (pruning); bucket is a hash-based file split inside a partition (bucketed joins) |

## 🔗 Go further

- **[[replication]]** shows how each partition is copied for fault tolerance; sharding and replication are usually used together.
- **[[distributed-fundamentals]]** and **[[cap-pacelc]]** explain what consistency you give up when a request spans shards.
- **[[pattern-hot-keys]]** goes deeper into skew remedies and monitoring.
- **[[nosql-modeling]]** shows how to pick a partition key from the query, the key decision in a wide-column store.
