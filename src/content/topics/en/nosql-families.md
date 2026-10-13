---
order: 45
tid: "nosql-families"
track: "4"
section: null
num: "4.8"
title: "NoSQL Families"
type: "concept"
level: "intermediate"
priority: "P1"
est_hours: 4
summary: "NoSQL families and when to use each: key-value (Redis), document (MongoDB), wide-column (Cassandra, HBase, ScyllaDB), graph (Neo4j), search (Elasticsearch), time-series. Query-first modeling."
covers: ["Cassandra partition key & clustering key","HBase row key design","Redis data structures","Elasticsearch inverted index"]
interview_qs: []
extras: []
prerequisites: ["storage-engines","partitioning-sharding"]
related: ["nosql-modeling","cap-pacelc","consistency-models","time-series-db","sql-indexing","realtime-olap"]
---
## 🎯 TL;DR

> **NoSQL** is not one thing. It is a set of data models, each built for a different access pattern. The rule that matters in interviews is **query-first modeling**: decide the questions you must answer fast, then shape the data so each question reads from one place.

- **Key-value** (Redis): get and set by key, very fast, often in memory.
- **Document** (MongoDB): a whole nested record per key, queried by fields inside it.
- **Wide-column** (Cassandra, HBase, Bigtable, ScyllaDB): rows grouped by a partition key and sorted by a clustering key. Built for huge write volumes and known access paths.
- **Graph** (Neo4j): nodes and edges, for traversals across many hops.
- **Search** (Elasticsearch, OpenSearch): an inverted index for text relevance, filters and aggregations.
- **Time-series**: optimized for timestamped points, retention and downsampling.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Think of different kinds of storage in a shop. A **coat check** gives you one ticket and returns one coat (key-value). A **personal file folder** holds everything about one customer in one place (document). A **warehouse shelf sorted by aisle and then by date** lets you find all of a customer's orders in one aisle (wide-column). A **search index at the back of a book** lets you find every page that mentions a word (search).

A relational database starts from the tables and then asks you to join them. NoSQL usually starts from the **question**. If the app always asks "show the last 20 messages in this chat", you design a storage layout that answers exactly that question with one read.

Here is the same idea in code. The user's profile fits in one document:

```json
{
  "_id": "user:42",
  "name": "Ali",
  "city": "Tehran",
  "devices": [
    { "os": "android", "last_seen": "2026-10-01T09:00:00Z" }
  ]
}
```

A Redis key does one thing very fast:

```text
SET session:9f3a "user:42"  EX 3600     # expires after one hour
GET session:9f3a                       # returns "user:42"
```

The junior takeaway: **pick the store by the question you need to answer quickly, not by the brand.**

## 🔧 How it really works (Mid)

### Family by family

| Family | Examples | Data unit | Strong at | Weak at |
|---|---|---|---|---|
| Key-value | Redis, DynamoDB | value per key | point reads and writes, caching, sessions | querying by anything but the key |
| Document | MongoDB | nested document | one-record reads with nested data | joins and cross-document transactions at scale |
| Wide-column | Cassandra, HBase, Bigtable, ScyllaDB | row in a partition, sorted by clustering keys | write-heavy, known access paths, very large tables | ad hoc queries, joins |
| Graph | Neo4j | node and edge | multi-hop traversals | bulk analytics, huge writes |
| Search | Elasticsearch, OpenSearch | document with inverted index | full-text, relevance, faceted filters | source of truth, strong consistency |
| Time-series | InfluxDB, TimescaleDB (a PostgreSQL extension), others | timestamped points | ingestion and time-window queries | general relational work |

### Wide-column: Cassandra partition and clustering keys

In Cassandra, the **partition key** decides which nodes store a row (the token is a hash of the partition key). The **clustering columns** order the rows inside a partition on disk. Each partition should stay bounded in size.

```sql
-- CQL (Cassandra Query Language)
CREATE TABLE messages_by_chat (
  chat_id    uuid,
  sent_at    timestamp,
  msg_id     uuid,
  sender_id  bigint,
  body       text,
  PRIMARY KEY ((chat_id), sent_at, msg_id)
) WITH CLUSTERING ORDER BY (sent_at DESC, msg_id DESC);

-- Last 20 messages in one chat: one partition, read in order
SELECT sender_id, body, sent_at
FROM messages_by_chat
WHERE chat_id = 5b2e8c1a-0000-4000-8000-000000000001
LIMIT 20;
```

- `(chat_id)` is the partition key. All messages of one chat live together, so the query touches one partition.
- `sent_at, msg_id` are clustering columns, so messages are already sorted newest first on disk.
- There are no joins and no arbitrary `WHERE` clauses. You create one table per query (**denormalize** and duplicate on write).

A partition that grows without limit becomes a **wide partition**. Reads get slow, compaction gets heavy, and repair struggles. The common fix is to add a time bucket to the partition key, for example `((chat_id, day), sent_at, msg_id)`. Keep partitions to a size you can measure in tens of megabytes, not gigabytes. Check the docs for the exact guidance for your version.

Cassandra's storage is an LSM design (see [[storage-engines]]). Deletes create tombstones, which readers must skip until compaction removes them.

### Wide-column: HBase row keys

HBase (and Bigtable, which uses the same ideas) keeps rows **sorted by row key** and splits the table into **regions**, each a contiguous range of row keys. The row key is the only index. Column families are storage units, and column qualifiers can be created freely per row.

The main design issue is **hotspotting**. If the row key starts with a monotonically increasing value such as a timestamp, every new write lands in the last region, and one server takes all the load.

| Row-key design | Result |
|---|---|
| `2026-10-13T09:00:01_device42` | all new writes go to the newest region: hotspot |
| `device42#2026-10-13T09:00:01` | writes spread across devices, but a scan over all devices for one time range is hard |
| `salt(0-7)#device42#timestamp` | writes spread across 8 buckets, and reads fan out to all 8 |
| `device42#(MAX - epoch_ms)` | newest rows of one device come first, and the device is the prefix |

Pick the key from the main read pattern: "all events of one device, newest first" points to the last row. HBase stores data in HFiles on HDFS, with a MemStore in memory and a WAL for durability, so it is also an LSM design.

### Key-value: Redis data structures

Redis is an in-memory store whose values are **data structures**, not just strings.

- **Strings:** counters (`INCR`), cached values, flags.
- **Hashes:** a record with fields, such as a user profile.
- **Lists:** queues and recent-item feeds (`LPUSH`, `LTRIM`).
- **Sets:** unique members and set operations (`SADD`, `SINTER`).
- **Sorted sets (ZSET):** leaderboards and ranges by score (`ZINCRBY`, `ZRANGE ... WITHSCORES`).
- **Streams:** append-only logs with consumer groups.
- **HyperLogLog:** approximate distinct counts in a small fixed memory footprint. See [[probabilistic-ds]].
- **Bitmaps:** compact per-user flags.

```text
ZINCRBY leaderboard:week42 25 user:7      # add 25 points
ZREVRANGE leaderboard:week42 0 9 WITHSCORES   # top 10
```

Redis keeps data in memory. Persistence uses RDB snapshots and/or an append-only file (AOF). Core command execution has historically run on a single thread, so one slow command blocks others. Keep big keys and long scans out of the hot path.

### Search: the inverted index

An **inverted index** maps each term to the list of documents that contain it (a postings list). A query for `"kafka" AND "lag"` intersects two postings lists instead of scanning every document.

```text
"kafka"  -> [doc 3, doc 17, doc 90]
"lag"    -> [doc 17, doc 44, doc 90]
"kafka AND lag" -> [doc 17, doc 90]
```

Analyzers decide the terms: tokenization, lowercasing, stemming, and stop words. Elasticsearch is built on Lucene, whose segments are immutable and merged in the background, similar to an LSM design. New documents become searchable after a **refresh**, so search is near-real-time, not immediate.

Search engines are great for filtering and aggregating over indexed fields, but they are usually a **derived** store. Keep the source of truth elsewhere and rebuild the index from it.

### Document: MongoDB

MongoDB stores BSON documents, which can nest arrays and objects. The core modeling choice is **embed vs reference**:

- **Embed** when the child data is read with the parent, and its size is bounded (for example a user's last 10 addresses).
- **Reference** when children are large, unbounded, or shared (for example all orders of a user, or a product used in many orders).

A document has a size limit of 16 MB, so an unbounded array inside a document is a design bug. Sharding uses a **shard key**, and a poor shard key (monotonic, or low cardinality) creates hot shards, the same problem as HBase row keys.

## 🧠 Senior depth & trade-offs

- **Pick the access pattern first.** Write down the top 5 to 10 queries with their rates and latency targets. Then choose the family and the key design. A NoSQL store with a schema designed for different queries becomes a full scan in disguise.
- **Consistency is tunable, not free.** Cassandra uses replication factor and per-query consistency levels (for example `QUORUM` reads and writes). Overlapping quorums give strong reads in the normal case, but they still have failure and repair edge cases. Read [[consistency-models]] and [[cap-pacelc]] before promising guarantees.
- **Scale numbers matter.** Estimate partition sizes, rows per partition and request rate per partition. Cassandra and HBase handle many nodes well, but one hot partition or region caps the throughput of that key range.
- **Operational cost.** Compaction, repair, tombstone handling and region splits each need monitoring. A Cassandra cluster with unnoticed tombstone growth is a slow incident waiting to happen.
- **Denormalization has a write cost.** Every query table is a copy. Updating a user's name may mean updating many tables. Decide with the business which copies must be exact and which can be eventually consistent.
- **Search as a derived index.** Feed Elasticsearch from a CDC stream or from batch loads, and handle reindexing as a planned operation. See [[cdc]].
- **Multi-model with PostgreSQL.** JSONB and extensions cover many document or time-series needs without a new system. The choice of a separate store should follow a measured gap, not a trend. See [[postgresql]].
- **When not to use NoSQL.** When you need ad hoc queries across many entities, joins, and strong transactions over several records, a relational database or a warehouse is usually the better choice. Query flexibility and transaction support are the price of many NoSQL designs.

## 🧪 Try it

1. **Key design.** An IoT platform stores sensor readings, and the main query is "last 100 readings for one sensor". Propose a Cassandra primary key and explain the partition size for 1 reading per second over 30 days.
2. **Hotspot.** Your HBase table uses `yyyyMMddHHmmss#deviceId` as the row key and writes peak at 50,000 rows per second. What goes wrong, and give one fix that keeps "one device, time range" reads efficient.
3. **Redis choice.** You need the top 10 players of the week, updated on every game event. Which Redis structure fits, and what command gives the answer?
4. **Search or table?** Users search product titles with typos and filters by price. Which store fits, and what is the source of truth?

<details><summary>Show solutions</summary>

**1.** `PRIMARY KEY ((sensor_id, day), reading_time)` with `CLUSTERING ORDER BY (reading_time DESC)`, where `day` is the date. One sensor per day is 86,400 readings per day, so the partition holds one day of data, about 86,400 rows. For 30 days, a partition covers one day, not 30 days. The query "last 100 readings" reads today's partition, and goes to the previous day only if today has fewer than 100 readings. Without the day bucket, one sensor would grow to 2.6 million rows (30 × 86,400) in one partition.

**2.** The key starts with a time value that increases, so all new writes go to the last region and one server gets the load. A fix: prefix with a salt bucket (for example `hash(deviceId) % 16`) or put the device first. With the device first, the query "one device, time range" is a single prefix scan. The salted version needs one scan per bucket.

**3.** A sorted set (ZSET). Each event runs `ZINCRBY leaderboard:week42 <points> <player>`, and the answer is `ZREVRANGE leaderboard:week42 0 9 WITHSCORES`.

**4.** A search engine such as Elasticsearch handles typo tolerance, relevance and faceted filters. The source of truth stays in the product database, and the search index is rebuilt or updated from it through CDC.

</details>

## 💬 Interview corner

**Question: "How would you choose between Cassandra and MongoDB?"** Mid answer: "Cassandra is for very high write volume with known access paths, and it scales across nodes without a primary. MongoDB suits flexible documents with rich queries on fields." Senior answer: "I'd start from the queries and the write rate. For a multi-datacenter write-heavy event store with known keys, I'd lean to Cassandra and design the partition keys with bounded partitions. If the app needs secondary indexes, aggregations on nested fields and transactions across a few documents, MongoDB is a better fit. I'd check the operations team's skills before deciding."

**Question: "Design a key for a time-series table in HBase."** Mid: "Put the entity first so one entity's rows are contiguous, then a reversed timestamp so the newest rows come first." Senior: "I'd check the write hotspot and the read pattern together. A salted prefix spreads writes, but every read becomes a fan-out over the salt buckets. I'd choose the bucket count from the peak write rate and the region count."

**Question: "What is query-first modeling?"** Mid: "You list the queries first, and then design each table or document so the query reads one partition or one key." Senior: "It means denormalizing on write, accepting duplicated data, and deciding which copies can be eventually consistent. The cost shows up in the update path, so I'd design both paths together."

> **🎤 Say this in the interview:** "I pick a NoSQL family from the access pattern, not from the brand. In Cassandra I model one table per query, with a partition key that bounds partition size and clustering columns for order. I watch hotspots, tombstones and compaction, and I keep derived stores like search indexes rebuildable from the source of truth."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Modeling NoSQL like a relational schema, then asking for joins and ad hoc filters. The query falls back to a full scan or a fan-out to every node.

> **⚠️ Watch out:** A monotonically increasing key (timestamp, auto-increment ID) as the partition or row-key prefix. All writes go to one place.

- Using `ALLOW FILTERING` in Cassandra to make a query work. It scans many partitions, and it is a sign the table is wrong for the query.
- Letting a Cassandra partition grow without a time bucket, so it becomes a wide partition.
- Embedding an unbounded array in a MongoDB document until it hits the 16 MB limit.
- Treating Redis as a durable primary store without thinking about persistence settings and failover.
- Using a search index as the source of truth.
- Choosing a family because it is popular, without estimating write rate, partition size and consistency needs.

## ✅ Check yourself (quiz)

1. In Cassandra, which key decides which nodes store a row?
   - A) The clustering key
   - B) The partition key
   - C) The primary key's last column
   - D) The table name

<details><summary>Answer</summary>

**B** — the partition key is hashed to a token, which determines the replica nodes. Clustering columns only order rows inside the partition.

</details>

2. Why does a monotonically increasing row key cause problems in HBase?
   - A) It cannot be stored as bytes
   - B) New writes all go to the last region, so one server handles most of the load
   - C) HBase does not allow sorted keys
   - D) Column families must be sorted by time

<details><summary>Answer</summary>

**B** — HBase keeps rows sorted by key, so new keys at the end of the range all go to the last region. This is a hotspot.

</details>

3. Which Redis data structure is best for a live leaderboard that needs the top N by score?
   - A) String
   - B) Hash
   - C) Sorted set
   - D) Stream

<details><summary>Answer</summary>

**C** — a sorted set keeps members ordered by score, and `ZREVRANGE` returns the top N directly. A stream is an append-only log and does not rank members.

</details>

4. **(Senior)** A Cassandra table is slow to read and compaction is heavy. The team finds that one partition holds many years of events for a busy device. What is the root cause and the fix?
   - A) Too few nodes; add nodes
   - B) The partition is unbounded; add a time bucket to the partition key so each partition has a bounded size
   - C) The clustering order is wrong; switch to ascending order only
   - D) Use a larger batch size for writes

<details><summary>Answer</summary>

**B** — an unbounded partition makes reads, compaction and repair expensive. Adding a time bucket (for example by day or month) to the partition key bounds the size, and queries read only the buckets they need.

</details>

5. **(Senior)** Why should a search index not be the system of record, even though it stores all the data?
   - A) Search indexes cannot store text
   - B) The index is a derived, near-real-time structure with refresh and rebuild semantics, so it should be rebuildable from a source of truth
   - C) Search engines delete data after 24 hours
   - D) Inverted indexes do not support filters

<details><summary>Answer</summary>

**B** — a search index is built from source data, refreshed in near real time, and can be rebuilt. Keeping the source of truth elsewhere makes recovery, schema change and reindexing safer.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Query-first modeling | Decide the queries first, then shape the storage so each query reads one place |
| Cassandra partition key role | Hashes to a token and decides which nodes store the row |
| Cassandra clustering column role | Orders rows inside one partition on disk |
| HBase row key rule | Rows are sorted by key, so the key design controls hotspots and scans |
| Wide partition | A partition that grows without a bound, causing slow reads and heavy compaction |
| Redis sorted set use | Ranked members by score, with range queries such as top N |
| Inverted index | Maps each term to the documents that contain it |
| Embed vs reference (MongoDB) | Embed bounded child data read together; reference large or unbounded data |

## 🔗 Go further

- [[nosql-modeling]] goes deeper into designing tables and documents for each family.
- [[storage-engines]] explains the LSM design behind Cassandra and HBase.
- [[partitioning-sharding]] covers how partition keys spread data across nodes.
- [[cap-pacelc]] and [[consistency-models]] explain the trade-offs behind tunable consistency.
- [[time-series-db]] covers time-series stores and their retention models.
- [[realtime-olap]] covers how search and analytical stores complement each other in a serving layer.
- Check the Cassandra, HBase and Redis documentation for your version before you rely on specific limits or defaults.
