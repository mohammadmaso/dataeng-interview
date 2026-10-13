---
order: 65
tid: "nosql-modeling"
track: "6"
section: null
num: "6.10"
title: "Modeling for NoSQL & Serving Stores"
type: "concept"
level: "advanced"
priority: "P2"
est_hours: 2
summary: "Query-first modeling for Cassandra, HBase and Redis; row-key design to avoid hot spots."
covers: []
interview_qs: []
extras: []
prerequisites: ["nosql-families", "partitioning-sharding"]
related: ["modeling-levels", "physical-modeling-olap", "pattern-hot-keys", "serving-layer", "storage-engines"]
---
## 🎯 TL;DR

> In NoSQL stores you model **the queries first**, then the tables. There are no joins, and the access path is decided by the **key**: the partition key (Cassandra) or the row key (HBase). Pick the key so that each query reads a small, contiguous range on one node, and so that writes spread across the cluster.

- **Cassandra:** one table per query. The partition key picks the node; clustering columns sort rows inside the partition.
- **HBase:** one sorted index, the row key. Rows are stored in key order, so a monotonically increasing key sends all writes to one region (a hot spot).
- **Redis:** the key is the address. Choose data structures (hash, sorted set, list) that match the read pattern and fit in memory.
- Denormalize on purpose. Duplicating data across tables is normal, and the cost is paid at write time.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A phone book sorted by last name. You can find "Masoudi" in seconds, but "find everyone who lives on Valiasr Street" means reading the whole book. A NoSQL store is a phone book with one sorting order. If you want a second order, you print a second book. Storage is cheap; scanning the whole book is not.

In SQL you start with entities and join them later. In a wide-column store like Cassandra you start with a question: "show the last 100 sensor readings for sensor `s-42` on a given day". The answer decides the table:

```sql
-- Cassandra (CQL): one table shaped by one question
CREATE TABLE readings_by_sensor_day (
  sensor_id text,
  day       date,
  ts        timestamp,
  value     double,
  PRIMARY KEY ((sensor_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

SELECT ts, value
FROM readings_by_sensor_day
WHERE sensor_id = 's-42' AND day = '2026-10-12'
LIMIT 100;
```

Read the primary key carefully:

- The **partition key** `(sensor_id, day)` is in double brackets. All rows with the same sensor and day live together on one set of replicas.
- The **clustering column** `ts` sorts rows inside that partition, newest first here.
- The query names the full partition key, so the database goes straight to one place and reads a contiguous slice.

> **⚠️ Watch out:** If you ask a different question, such as "all readings with value > 90 across all sensors", this table cannot answer it efficiently. You build a second table for that query, or you use a different system. That is normal in NoSQL.

## 🔧 How it really works (Mid)

### Cassandra: partitions are the unit of everything

Cassandra hashes the partition key to a token, and the token decides which nodes own the data (consistent hashing; see the consistent-hashing glossary entry). The rules that follow:

- **Equality on the full partition key** is fast. A query without it becomes a cluster-wide scan, which is slow and usually rejected by default.
- **Clustering columns** give sorted range queries inside a partition, such as "last 24 hours" with a `ts` range.
- **Partition size matters.** A partition that grows without bound (one sensor forever) gets slow to read and to repair. The usual fix is **time bucketing**: add the day or the hour to the partition key, as in the example above. Size guidance changes between versions, so check the current docs, but the principle holds: keep partitions bounded.
- **Low-cardinality partition keys create hot spots.** A partition key like `country = 'IR'` puts most of the traffic on the few replicas that own it. Add a high-cardinality part (user id, bucket number) or split by time.

A second query needs a second table. Duplication is the price:

```sql
-- Same events, another question: "which sensors had an alert today?"
CREATE TABLE alerts_by_day (
  day        date,
  sensor_id  text,
  ts         timestamp,
  severity   text,
  PRIMARY KEY ((day), severity, sensor_id, ts)
);
```

Notice the bad side of `alerts_by_day`: every alert of the day lands in the same partition. If the number of alerts is large, that partition is hot. You would bucket it further, for example by `(day, hour)`, and read several partitions for a day.

> **🎤 Say this in the interview:** "I start from the query. The partition key must be known in the query and must spread the data. The clustering key gives the sort order I need for range reads. If a second query needs different keys, I add a table and write to both."

### HBase: one sorted index, the row key

HBase (on top of HDFS, modeled after Bigtable) has one index: the **row key**. Rows are stored sorted by row key, and each region (a contiguous key range) is served by one RegionServer. Column families group columns that are stored together; keep the number of families small, because each one has its own store files.

This creates the main HBase rule: **the row key design decides both the read pattern and the write distribution.**

- A **range scan** over a row key prefix is cheap, because the rows are adjacent. `device-42#...` scans one device.
- A **monotonically increasing key** (a timestamp at the front, an auto-increment id) sends all new writes to the last region. That single server becomes the bottleneck. This is the classic hot spot.

Three standard fixes, often combined:

```python
import zlib

SALT_BUCKETS = 16
MAX_TS_MS = 9_999_999_999_999  # fixed width, 13 digits

def row_key(device_id: str, event_ts_ms: int) -> bytes:
    # 1) reversed timestamp: the newest rows of a device sort first
    reversed_ts = MAX_TS_MS - event_ts_ms
    # 2) salt: spread devices over buckets; crc32 is deterministic across processes
    salt = zlib.crc32(device_id.encode()) % SALT_BUCKETS
    # 3) fixed-width fields keep the lexicographic order correct
    return f"{salt:02d}|{device_id}|{reversed_ts:013d}".encode()
```

- **Salting** (the prefix bucket) spreads writes across regions. It is derived from the device id, so all rows of one device share a salt and a device scan is still one contiguous range.
- **Reversed timestamps** make "latest N events of a device" a short scan from the start of the device's range.
- **Fixed-width numbers** matter. `"9"` sorts after `"10"` as text. Pad every number to the same width, or the order is wrong.

> **⚠️ Watch out:** Salting trades write spread for read cost. To scan *all* devices in time order, you now run one scan per bucket and merge the results. Decide which query is more important before you salt.

### Redis: the key is the address

Redis keeps data in memory and is addressed by key. Modeling is about choosing a **data structure** that matches the read:

```text
user:42:profile          HASH   name, country, plan       -> HGETALL for the profile page
leaderboard:2026-10      ZSET   member = user id, score   -> top 10 with ZREVRANGE
session:9f1c...          STRING with TTL                  -> expires automatically
```

For a leaderboard: `ZADD leaderboard:2026-10 1500 user:42` writes the score, and `ZREVRANGE leaderboard:2026-10 0 9 WITHSCORES` reads the top ten. The sorted set does the ranking, so the application does not sort millions of rows. Memory is the limit: every key lives in RAM, so the size of each structure and the TTL policy are part of the model.

## 🧠 Senior depth & trade-offs

- **Write-time cost for read-time speed.** Each query table is a copy you must keep consistent. Decide who writes all copies (one service, an outbox, or a stream pipeline), and what happens when one write fails. Idempotent writes help: the same event written twice must produce the same row.
- **Partition sizing is capacity planning.** Estimate rows per partition from the bucket size and the event rate. For example, 200 events per second per sensor is 200 × 86,400 = 17.28 million events per day for one sensor. An hourly bucket still holds 200 × 3,600 = 720,000 events, which may be too large for one partition depending on row size, so a finer bucket or a different key may be needed. Show the arithmetic in the design review.
- **Deletes and tombstones.** In Cassandra a delete writes a tombstone, and reads must skip tombstones until compaction removes them. A queue-like table, where rows are inserted and deleted constantly, becomes slow. Prefer time-to-live (TTL) on the data, or a model where rows are not repeatedly deleted.
- **Consistency is a per-query choice.** Cassandra lets you choose the consistency level per read and write; HBase is strongly consistent per row; Redis replication is asynchronous by default. Model the important read path first, then choose the level it needs (see the consistency-models topic).
- **Hot keys are not only a partition problem.** One very popular key (a celebrity profile, a global counter) can overload one node even with a good partition design. The fixes are caching, splitting the counter into shards, or a read replica (see the pattern-hot-keys topic).
- **Monitor the model, not only the cluster.** Track partition size distribution, tombstone counts, region load in HBase, and memory per Redis key pattern. A key design that was fine in the first month can fail at a larger scale.
- **When NOT to use NoSQL for this:** if the questions change every week and need joins across many entities, a relational database or a warehouse is cheaper to change. NoSQL pays off when the access patterns are known, stable and high volume.

## 🧪 Try it

A ride-hailing app stores GPS pings from 200,000 active drivers, each sending one ping every 4 seconds. Two queries matter: (a) "the last 50 pings of driver `d-123`" for the trip replay, and (b) "all drivers inside a geohash cell right now" for dispatch.

1. Write the Cassandra table for query (a). Choose the partition key and the clustering key.
2. Estimate the number of pings per day for the whole fleet, and the rows per driver per day.
3. Why is `geohash` a bad partition key on its own for query (b)? Propose a better design.
4. In HBase, what row key would you use for a trip-replay scan, and how do you avoid a hot region for new pings?

<details><summary>Your guess?</summary>

1. `CREATE TABLE pings_by_driver_day (driver_id text, day date, ts timestamp, lat double, lon double, PRIMARY KEY ((driver_id, day), ts)) WITH CLUSTERING ORDER BY (ts DESC);` with a query like `WHERE driver_id = 'd-123' AND day = '2026-10-12' LIMIT 50`.
2. One ping every 4 seconds is 86,400 / 4 = 21,600 pings per driver per day. For 200,000 drivers, that is 200,000 × 21,600 = 4.32 billion pings per day.
3. A popular geohash cell (a city centre) becomes a hot partition, because many drivers are in it at the same time. For dispatch, use a short-lived table or an in-memory store keyed by cell and bucketed by time, or split busy cells into finer cells, and keep the current position in a store built for latest-state lookups.
4. Use `driver_id` followed by a reversed timestamp, with a salt prefix derived from the driver id, as in the Python example above. The salt spreads new writes across regions, and the reversed timestamp makes "last 50 pings" a short scan.

</details>

## 💬 Interview corner

**Question: "How would you model this for Cassandra?"** Mid answer: "I'd write down the queries, then make one table per query with the lookup value as the partition key and the sort order as clustering columns." Senior answer: "I'd also check partition size and growth. I'd bucket by time when a partition could grow without bound, and I'd state how each write reaches all the tables, with idempotent retries. I'd add a monitoring alert for partition size and tombstone counts."

**Question: "Why does my HBase cluster have one busy RegionServer?"** Mid: "The row key is probably monotonic, like a timestamp, so every new write goes to the last region." Senior: "That's the first thing I'd check, and then I'd verify with the region load metrics. The fix is in the key: salt the prefix, reverse the timestamp for recent-first scans, and pre-split the table so the regions exist before the load arrives."

**Question: "When would you choose Redis over a database for modeling?"** Mid: "For low-latency reads on a known key, like a session or a counter." Senior: "When the data fits in memory, the access is by key or by a simple ranking structure, and I can accept async replication or can rebuild the data from a source of truth. I'd keep the source of truth elsewhere."

> **🎤 Say this in the interview:** "In a NoSQL store the query decides the key. I make the partition key match the query, bucket partitions so they stay bounded, and spread writes so no single node or region gets all of them. If two queries need different keys, I accept a second copy of the data and make the writes reliable."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Using a timestamp at the start of an HBase row key or a Cassandra partition key without bucketing. All new writes go to one place.

> **⚠️ Watch out:** Designing a Cassandra table around an entity and then adding `ALLOW FILTERING` to answer a new query. That scans the cluster. Build a table for the query instead.

- Modeling NoSQL like a normalized relational schema with foreign keys and expecting joins.
- Using a low-cardinality column (status, country) as the only partition key.
- Letting one partition grow forever instead of bucketing by time.
- Using a numeric row-key component without fixed width, so the lexicographic order is wrong.
- Forgetting that every extra table is a write you must keep consistent.
- Using a Cassandra table as a queue, with constant inserts and deletes, which fills the store with tombstones.

## ✅ Check yourself (quiz)

1. In Cassandra, what does the partition key decide?
   - A) The sort order of rows inside the partition
   - B) Which replicas own the rows, and whether a query can be answered efficiently
   - C) The column types of the table
   - D) The number of columns allowed in the table

<details><summary>Answer</summary>

**B** — the partition key is hashed to decide the owning replicas. A query that includes the full partition key reads one place; clustering columns decide the order inside it.

</details>

2. In HBase, which row key pattern creates a hot spot for new writes?

   - A) `device_id#reversed_ts`
   - B) `salt|device_id|ts`
   - C) `timestamp` at the front of the key (monotonically increasing)
   - D) `user_id` followed by `event_type`

<details><summary>Answer</summary>

**C** — monotonically increasing keys always land in the last region, so one RegionServer takes all new writes. Salting or reversing the timestamp spreads them.

</details>

3. A Cassandra table has `PRIMARY KEY ((country), user_id)`. Most traffic is from one country. What is the risk?

   - A) None; the clustering key balances the load
   - B) A hot partition on a few replicas, because the low-cardinality partition key concentrates writes
   - C) The table cannot store `user_id`
   - D) Cassandra will reject the insert

<details><summary>Answer</summary>

**B** — `country` has few values, so one partition gets most of the data and traffic. Add a higher-cardinality part to the partition key or bucket the partition.

</details>

4. **(Senior)** A team wants to answer a new query on an existing Cassandra table by adding `ALLOW FILTERING`. Why is this a bad idea, and what is the right move?

   - A) It is a syntax error in every version
   - B) It forces a scan across partitions, which is slow and load-heavy at scale; create a new table keyed for the query and write to both tables
   - C) It makes the query faster by sorting in memory
   - D) It only affects the clustering key

<details><summary>Answer</summary>

**B** — filtering without the partition key reads many partitions on many nodes. The correct design is a query table with the right partition key, with the cost paid at write time, and writes kept consistent.

</details>

5. **(Senior)** Your partition is `(sensor_id)` and a sensor sends 200 events per second forever. Why will this fail, and what is a principled fix?

   - A) Sensors cannot send that many events
   - B) The partition grows without bound, which slows reads, repairs and compaction; add a time bucket such as the day to the partition key, and size the bucket with arithmetic from the event rate
   - C) Use Redis instead for all sensors
   - D) Store the events as a single row

<details><summary>Answer</summary>

**B** — unbounded partitions are the classic Cassandra failure. Bucketing by time bounds the size, and the bucket length should come from the rate: 200 events per second is 17,280,000 per day, so a daily bucket is still very large and an hourly or finer bucket is safer.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Cassandra partition key | Hashed to choose the replicas; include it in every efficient query |
| Clustering column | Sorts rows inside one partition; enables range queries |
| Unbounded partition | Grows forever and slows reads and repairs; fix with time bucketing |
| HBase row key | The only index; rows are stored sorted by it, in regions |
| HBase hot spot cause | Monotonically increasing row key sends all writes to the last region |
| Salting | A prefix bucket derived from the entity id to spread writes across regions |
| Reversed timestamp | Makes the newest rows of an entity sort first |
| Redis sorted set (ZSET) | Ranked structure in memory, for leaderboards and top-N reads |

## 🔗 Go further

- **nosql-families** compares key-value, document, wide-column and graph stores, which decides which modeling rules apply.
- **partitioning-sharding** explains how keys map to nodes, the foundation of every rule in this lesson.
- **physical-modeling-olap** shows the same query-first idea for columnar analytic engines.
- **pattern-hot-keys** covers the detection and mitigation of hot keys in more depth.
- **storage-engines** explains LSM trees, memtables and compaction, which explain tombstones and write behavior.
