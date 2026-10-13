---
order: 25
tid: "sql-query-optimization"
track: "2"
section: null
num: "2.10"
title: "Query Execution & Optimization"
type: "concept"
level: "advanced"
priority: "P0"
est_hours: 6
summary: "Reading EXPLAIN / EXPLAIN ANALYZE, join algorithms (nested loop, hash, merge), table statistics and cardinality estimation, predicate pushdown and partition pruning. Senior question: \"this query is slow — walk me through how you debug it.\""
covers: ["cost-based optimizer","sargable predicates","function-on-column anti-pattern","join order","spilling to disk","materialized views"]
interview_qs: []
extras: []
prerequisites: ["sql-joins", "sql-indexing"]
related: ["sql-indexing", "sql-window-functions", "spark-joins-skew", "spark-aqe", "postgresql", "columnar-storage", "sql-dialects"]
---

## 🎯 TL;DR

> SQL says **what** you want. The **optimizer** decides **how** to get it: which indexes to use, which join algorithm to run, in which order to join tables, and where to filter. `EXPLAIN` shows that decision.

- The optimizer is **cost-based**: it estimates the cost of many plans from table **statistics** and picks the cheapest.
- Most slow queries come from **wrong estimates** (the optimizer thinks 100 rows, the real number is 2 million) or from a **plan shape** that cannot use an index.
- Debug in order: get the actual plan, find the node where time or row counts go wrong, check statistics, check sargability, then change the query, the index or the data layout.
- Three join algorithms (nested loop, hash, merge) have different costs. Knowing which one the plan uses tells you what it needs: an index, memory, or sorted input.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A map app. You type "from home to the airport". The app does not drive every road to find the answer. It uses a map of traffic (statistics), guesses how long each route takes (cost), and shows you one route (the plan). If the traffic data is old, it sends you down a closed road.

When you run a query, the database does four steps:

1. **Parse** the SQL and check that tables and columns exist.
2. **Rewrite** it (for example, expand views).
3. **Plan**: generate candidate plans, estimate each one's cost, and pick the cheapest.
4. **Execute** the chosen plan and return rows.

You can see step 3 with `EXPLAIN`. It shows the plan without running the query. `EXPLAIN ANALYZE` runs the query and adds real timings and row counts next to the estimates.

```sql
CREATE TABLE customers (id int PRIMARY KEY, city text, segment text);
CREATE TABLE orders (id bigint PRIMARY KEY, customer_id int, created_at timestamp, amount numeric);
CREATE INDEX ON orders (created_at);

EXPLAIN ANALYZE
SELECT c.city, SUM(o.amount)
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.created_at >= '2026-01-01' AND o.created_at < '2026-02-01'
GROUP BY c.city;
```

Read the plan from the **inside out**: the most indented lines run first and feed the lines above them.

## 🔧 How it really works (Mid)

### Statistics and cardinality

The optimizer does not look at the data when it plans. It uses **statistics**: row counts, number of distinct values, the most common values, and histograms per column. From these it estimates **cardinality**, the number of rows at each step of the plan. A bad estimate at one step propagates to every step above it.

```text
Hash Join  (cost=... rows=48000) (actual time=... rows=212000 loops=1)
  -> Index Scan using orders_created_at_idx on orders o
       Index Cond: (created_at >= '2026-01-01' AND created_at < '2026-02-01')
       (rows=48000) (actual rows=212000)
```

This is a simplified plan. The estimate (48,000) is 4 times lower than the real count (212,000). The planner may then pick a nested loop that is fine for 48,000 rows and terrible for 212,000. The fix is usually `ANALYZE` on the table (refresh statistics), or extended statistics when two columns are correlated, such as `city` and `zip_code`.

### Join algorithms

| Algorithm | Works best when | Needs | Rough cost |
|---|---|---|---|
| **Nested loop** | outer side is small; inner side has an index on the join key | index on the inner key | outer rows × one index lookup each |
| **Hash join** | equality join; one side fits in memory | memory for the hash table | build on the smaller side, probe with the other: about n + m if it fits |
| **Merge join** | both inputs are already sorted on the join key | sorted input (an index can provide it) | about n + m after sorting |

Hash and merge joins need an **equality** join condition. Nested loop works for any condition, such as `<` or `BETWEEN`.

Hash join has a memory limit. When the hash table exceeds `work_mem` (PostgreSQL, per operation), the join splits into **batches** and writes them to disk. `EXPLAIN ANALYZE` shows this as `Batches: 8` or higher. The same happens to sorts: `Sort Method: external merge Disk: ...kB`. That is **spilling to disk**, and it is often the hidden reason a query is ten times slower than expected.

### Sargable predicates

A predicate is **sargable** (search-argumentable) when the engine can use an index or statistics on it. The common anti-pattern is putting a function or arithmetic on the column:

```sql
-- Not sargable: the index on created_at cannot be used directly
WHERE DATE(o.created_at) = '2026-01-15'

-- Sargable: a half-open range on the raw column
WHERE o.created_at >= '2026-01-15' AND o.created_at < '2026-01-16'
```

The same rule applies to `WHERE amount * 1.09 > 100`. Move the arithmetic to the constant side: `WHERE amount > 100 / 1.09`.

### Predicate pushdown and partition pruning

**Predicate pushdown** means applying a filter as early as possible. In a plan, the filter sits at the scan, not on top of a join with a million rows. In Parquet or ORC files, the engine can skip whole row groups using min/max statistics stored in the file footer. In a view or subquery, the optimizer pushes the outer `WHERE` inside when that is safe. It does not push through `LIMIT`, window functions or volatile functions.

**Partition pruning** means skipping whole partitions. If `orders` is partitioned by `created_at` by month, a query with `WHERE created_at >= '2026-01-01' AND created_at < '2026-02-01'` reads one partition. In `EXPLAIN`, you see only that partition in the scan. Wrapping the partition key in a function often prevents pruning in the same way it prevents index use.

### Materialized views

A **materialized view** stores the result of a query on disk, so readers do not recompute it. In PostgreSQL, `REFRESH MATERIALIZED VIEW` recomputes it and blocks readers. `REFRESH ... CONCURRENTLY` lets readers continue but requires a unique index on the view. The trade-off is **freshness** against **compute**: the view is only as current as its last refresh.

## 🧠 Senior depth & trade-offs

**A debugging walkthrough for "this query is slow":**

1. **Get the actual plan** on realistic data (`EXPLAIN (ANALYZE, BUFFERS)` in PostgreSQL). Do not trust the estimate-only plan.
2. **Find the node where time or rows diverge.** Compare `rows=` (estimate) with `actual rows=`. A factor of 10 or more is the first suspect.
3. **Check statistics.** When were the tables last analyzed? Large bulk loads often leave statistics stale until `ANALYZE` runs.
4. **Check sargability and join keys.** Functions on columns, implicit type casts (comparing `int` to `text`), and join keys with different types all force bad plans.
5. **Check spilling.** `Batches` above 1 or `external merge Disk` means memory is too small for that operation.
6. **Only then change something:** rewrite the query, add a targeted index (see [[sql-indexing]]), change the partitioning, or pre-aggregate into a table.

**Engines differ.** Spark uses adaptive query execution (AQE) to change join strategy and partition counts at run time, and it can broadcast a small side instead of shuffling (see [[shuffle]]). A skewed join key still makes one task slow, so the root cause is [[data-skew]], not the join algorithm. Trino and BigQuery and Snowflake give query profiles with per-operator timings. ClickHouse uses sparse indexes and parts instead of B-trees. Check the docs for your engine and version before quoting a specific setting.

**Join order.** For many joins, the order matters a lot. Modern optimizers search many orders, but the search is limited for very large join counts, and the estimates are the input. Give the optimizer good statistics and avoid unnecessary joins before you try to hint the plan.

**Plan hints.** Some engines let you force a plan (for example, through extensions or `pg_hint_plan`). Treat this as a last resort: a hint that works today can be wrong after the data grows.

**Monitoring.** Track slow queries from the server log or the query-stats view, plus plan changes over time. A plan that flips after a data change is a common cause of "it was fast yesterday".

**When not to optimize the query.** If the same aggregate runs every minute for every dashboard, a materialized table or view refreshed hourly is often better than making each run faster.

## 🧪 Try it

Use PostgreSQL 13 or later with the schema above. Load about 1 million orders and 100,000 customers with `generate_series`, then run `ANALYZE` on both tables.

1. Run the `EXPLAIN ANALYZE` query from the Junior section. Name the join algorithm and say which side is the build side.
2. Change the filter to `WHERE DATE(o.created_at) = '2026-01-15'`. Compare the scan node before and after your rewrite to a half-open range.
3. Run `SET work_mem = '64kB';` and then `EXPLAIN ANALYZE` a query that sorts all orders by `amount`. Find the `external merge` line and the disk size.

<details><summary>Show solutions</summary>

**1.** For a range that matches many rows, the plan usually hashes the smaller side (`customers`, or the filtered `orders` after the index scan) and probes it. With an index on `customers(id)` and very few matching orders, a nested loop can win. Your plan may differ; the point is to name the operator and the build side.

**2.** The function version usually shows a sequential scan on `orders` (or a filter on a scan with a row count near the full table). The range version shows an `Index Scan` or `Bitmap Index Scan` on `orders_created_at_idx`. The result rows are identical.

**3.** With `work_mem` at 64 kB, the sort does not fit in memory. The plan shows `Sort Method: external merge  Disk: ...kB`. Reset with `RESET work_mem;`. Exact sizes depend on the data.

</details>

## 💬 Interview corner

**Question: "This query is slow. Walk me through how you debug it."** Mid answer: "I'd run `EXPLAIN ANALYZE`, look for sequential scans on big tables, and check whether indexes are used." Senior answer: "I'd start with the actual plan on realistic data, not the estimate. I'd find the first node where estimated and actual rows diverge by a big factor, because every node above it inherits the wrong estimate. I'd check when the statistics were last refreshed, check for functions on filter columns and type mismatches on join keys, and check for spills to disk. Only after that I'd add an index or change the query, and I'd verify the fix on the plan and on production-sized data."

**Question: "When would the optimizer choose a nested loop over a hash join?"** When the outer input is small and the inner side has an index on the join key, so each outer row costs one fast lookup. A hash join wins when both sides are large and the hash table fits in memory.

**Question: "Why does `WHERE DATE(col) = ...` make my query slow?"** The function hides the column from the index and from the statistics. The optimizer then estimates the selectivity badly and scans the table. A half-open range on the raw column fixes both problems.

> **🎤 Say this in the interview:** "I debug from the actual plan, not from intuition. I look for the first node where the row estimate is wrong, because that is where the plan goes off track. Then I fix the cause: statistics, sargability, join keys, memory, or the data layout."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Reading `EXPLAIN` without `ANALYZE` and assuming the timings are real. Plain `EXPLAIN` shows the planner's estimate only.

> **⚠️ Watch out:** Adding an index before checking the plan. If the cause is a wrong estimate or a function on the column, a new index may not be used at all.

- Ignoring a large gap between estimated and actual rows. It is usually the root cause.
- Running a query on a dev database with 1,000 rows and assuming production will behave the same.
- Wrapping filter columns in functions or casts, which blocks index use and pruning.
- Forgetting that a join with a non-equality condition cannot use hash or merge join.
- Treating `work_mem` as a global setting. It applies per operation, so many parallel sorts can use many times the value.
- Forcing a join order or plan hint without re-checking it after data growth.

## ✅ Check yourself (quiz)

1. In an `EXPLAIN ANALYZE` plan, a node shows `rows=500` (estimate) and `actual rows=900000`. What is the most likely first thing to check?
   - A) The network latency to the client
   - B) Whether the table statistics are stale, and whether a function or correlated columns break the estimate
   - C) The number of CPU cores
   - D) The column order of the `SELECT` list

<details><summary>Answer</summary>

**B** — a big gap between estimate and actual rows usually means the statistics are stale or the planner cannot estimate a predicate (for example, a function on the column or correlated columns). A, C and D do not change row estimates.

</details>

2. A join condition is `a.ts BETWEEN b.start_ts AND b.end_ts`. Which join strategy is the natural fit?
   - A) Hash join on `a.ts`
   - B) Nested loop, with an index on the bounds of `b` when available
   - C) Hash join on `b.start_ts`
   - D) Merge join on `a.ts = b.start_ts`

<details><summary>Answer</summary>

**B** — hash and merge joins match rows by equality on a key, and a range condition has no equality key. A nested loop can evaluate any condition, and an index on the range bounds makes each outer row a fast lookup. A, C and D all rely on an equality key that this condition does not provide.

</details>

3. Which predicate is sargable for an index on `created_at`?
   - A) `WHERE EXTRACT(YEAR FROM created_at) = 2026`
   - B) `WHERE created_at >= '2026-01-01' AND created_at < '2027-01-01'`
   - C) `WHERE created_at::text LIKE '2026%'`
   - D) `WHERE created_at + interval '1 day' > now()`

<details><summary>Answer</summary>

**B** — the range is on the raw column. A, C and D all put a function, cast or arithmetic on the column, which hides it from the index.

</details>

4. (Senior) A nightly job runs a 40-second query, and `EXPLAIN ANALYZE` shows `Batches: 16` on a hash join. What is the most likely cause and the first fix to try?
   - A) The index is missing; add a B-tree on every join key
   - B) The hash table does not fit in memory and spills to disk; increase memory for that operation or reduce the build side (filter first, or pre-aggregate)
   - C) The statistics are too new; drop `ANALYZE`
   - D) The query uses too many columns

<details><summary>Answer</summary>

**B** — `Batches` above 1 means the hash table spilled. The fix is more memory for that operation, or a smaller build side. A is not the cause of spilling, C reverses the right direction, and D is not related.

</details>

5. (Senior, trade-off) A dashboard runs the same aggregate over 2 billion rows every few minutes. Which is the best change?
   - A) Add a hint that forces a nested loop
   - B) Build a materialized table or view refreshed on the schedule the dashboard needs, and point the dashboard at it
   - C) Disable statistics collection
   - D) Remove the `GROUP BY`

<details><summary>Answer</summary>

**B** — repeating the same heavy aggregate is wasted compute. A precomputed result refreshed at the freshness level the dashboard needs is the usual fix. A is fragile, C makes plans worse, and D changes the result.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What is a cost-based optimizer? | It estimates the cost of candidate plans from statistics and picks the cheapest |
| What does `EXPLAIN ANALYZE` add to `EXPLAIN`? | It runs the query and shows actual time and row counts next to the estimates |
| What is cardinality? | The estimated or actual number of rows at a plan step |
| When is a nested loop a good join? | Small outer input and an index on the inner join key |
| What does a hash join need? | An equality condition and memory for the hash table; it spills to batches when memory runs out |
| What makes a predicate sargable? | The raw column is compared with constants, with no function, cast or arithmetic on it |
| What is partition pruning? | Skipping partitions that cannot match the filter, based on the partition key |
| What is spilling to disk? | An operator (sort, hash) that exceeds its memory limit and writes temporary files |

## 🔗 Go further

- Prerequisites: [[sql-joins]] (join semantics) and [[sql-indexing]] (what an index can and cannot do).
- Related: [[sql-window-functions]] (window sorts are a common cost), [[spark-aqe]] and [[spark-joins-skew]] (the same ideas inside Spark), [[columnar-storage]] (why scans in warehouses behave differently).
- Read your engine's plan documentation for your exact version: PostgreSQL "Using EXPLAIN", Trino "EXPLAIN ANALYZE", or your warehouse's query profile.
