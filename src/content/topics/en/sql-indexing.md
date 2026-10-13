---
order: 26
tid: "sql-indexing"
track: "2"
section: null
num: "2.11"
title: "Indexing"
type: "concept"
level: "intermediate"
priority: "P0"
est_hours: 3
summary: "B-tree structure, composite indexes and column order, covering indexes, PostgreSQL index types (GIN, GiST, BRIN), and the write cost of indexes."
covers: ["clustered vs non-clustered","index-only scan","partial indexes","when indexes don't help in OLAP (sparse indexes instead)"]
interview_qs: []
extras: []
prerequisites: ["sql-fundamentals"]
related: ["sql-query-optimization", "sql-joins", "storage-engines", "wal-mvcc", "columnar-storage"]
---

## 🎯 TL;DR

> An **index** is a separate, sorted structure that lets the database find rows without reading the whole table. It makes reads faster and writes slower, and it costs disk space.

- The default index in most OLTP databases is a **B-tree**: a balanced tree with sorted keys, so lookups take a few page reads even for millions of rows.
- A **composite index** on `(a, b)` helps queries that filter on `a`, or on `a` and `b`. It does not help a query that filters only on `b`.
- A **covering index** contains every column a query needs, so the database can answer from the index alone.
- Every index is updated on every insert, and on updates to its columns. More indexes means slower writes.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A book index at the back of a textbook. To find "window function", you read the index (sorted, short) and jump to page 212. Without it, you read all 600 pages. The index is a second, smaller book that points into the first one, and you must update it every time you add a page.

Use this table for the whole lesson:

```sql
CREATE TABLE orders (
  id          bigint PRIMARY KEY,
  customer_id int NOT NULL,
  status      text NOT NULL,        -- 'open', 'paid', 'shipped'
  created_at  timestamp NOT NULL,
  amount      numeric(10,2) NOT NULL
);
```

Without an index on `customer_id`, this query reads the whole table (a **sequential scan**):

```sql
SELECT id, amount FROM orders WHERE customer_id = 42;
```

Add an index and the database can jump straight to the rows:

```sql
CREATE INDEX idx_orders_customer ON orders (customer_id);
```

Think of the cost. The index is extra data that must stay in sync with the table. A query that does not filter or sort on `customer_id` gets no benefit, and every insert still pays to update the index.

## 🔧 How it really works (Mid)

### B-tree

A B-tree is a balanced tree of **pages** (blocks, usually 8 KB in PostgreSQL). Each internal page holds sorted keys and pointers to child pages. Leaf pages hold the keys and a pointer to the table row (a **TID** in PostgreSQL, a row locator in other engines).

For a table with 10 million rows and about 400 keys per page, the tree depth is about 3: `log400(10,000,000) ≈ 2.9`. A lookup reads about 3 index pages plus 1 table page. A full scan reads all table pages.

The B-tree is sorted, so it also serves:

- equality: `WHERE customer_id = 42`
- ranges: `WHERE created_at >= '2026-01-01' AND created_at < '2026-02-01'`
- sorting: `ORDER BY created_at` (no extra sort step if the index order matches)
- prefix matches: `WHERE name LIKE 'Ali%'`, but **not** `LIKE '%Ali'`

### Composite indexes and column order

```sql
CREATE INDEX idx_orders_cust_created ON orders (customer_id, created_at);
```

The index is sorted by `customer_id` first, then by `created_at` inside each customer. This gives the **leftmost-prefix rule**:

| Query | Uses the index well? | Why |
|---|---|---|
| `WHERE customer_id = 42` | yes | prefix `customer_id` |
| `WHERE customer_id = 42 AND created_at >= '2026-01-01'` | yes | both columns, range on the second |
| `WHERE created_at >= '2026-01-01'` | no (usually) | the first column is missing, so matching rows are spread across the whole tree |
| `WHERE customer_id = 42 ORDER BY created_at` | yes | rows are already in order |

Put the **equality** columns first and the **range** column last. An index on `(created_at, customer_id)` would scan a date range and check `customer_id` on each row, which is much less selective.

### Covering indexes and index-only scans

```sql
-- Query needs only customer_id and amount for open orders
CREATE INDEX idx_orders_cover ON orders (customer_id) INCLUDE (amount);

SELECT customer_id, amount FROM orders WHERE customer_id = 42;
```

`INCLUDE` (PostgreSQL 11+) stores extra columns in the leaf pages without making them part of the search key. If every column the query needs is in the index, PostgreSQL can do an **index-only scan** and skip the table. It can only skip the heap for rows that are marked visible in the **visibility map**, which `VACUUM` maintains. On a table with many recent updates, the planner may still visit the table for some rows.

### Partial indexes

```sql
CREATE INDEX idx_orders_open ON orders (customer_id) WHERE status = 'open';
```

The index holds only the rows that match the `WHERE`. It is small, cheap to write, and used only when the query contains a condition the planner can prove implies `status = 'open'`. A query with `WHERE status = 'open'` uses it; a query with no status filter does not.

### PostgreSQL index types

| Type | Good for | Typical example |
|---|---|---|
| B-tree (default) | equality, ranges, sorting | `id`, `created_at` |
| GIN | many values per row: arrays, `jsonb`, full-text search | `WHERE tags @> '{vip}'` |
| GiST | geometry, ranges, nearest-neighbour, exclusion constraints | `WHERE period && '[2026-01-01,2026-02-01)'` |
| BRIN | very large tables where values follow physical order | time-ordered log tables |

**BRIN** (Block Range INdex) stores the min and max value for each group of table pages. It is tiny, but only useful when the column is correlated with the physical row order, such as an append-only `event_time`.

## 🧠 Senior depth & trade-offs

- **Write amplification.** An insert updates the heap and **every** index. An update that changes an indexed column creates a new index entry. PostgreSQL's HOT (heap-only tuple) updates skip index changes when no indexed column changes and the page has room. Five indexes on a hot table can make writes several times slower than with one.
- **Selectivity matters.** If a predicate matches 40% of rows, the planner usually prefers a sequential scan: random index lookups for that many rows cost more than reading the table in order. Indexes help most when the predicate is selective.
- **Clustered vs non-clustered.** In InnoDB (MySQL), the table *is* the primary-key B-tree (clustered). A secondary index stores the primary key, so a lookup through a secondary index costs two tree walks. In PostgreSQL, the heap is unordered and all indexes are non-clustered. `CLUSTER` reorders the table once; it is not maintained afterwards.
- **OLAP is different.** Warehouses and columnar engines (ClickHouse, StarRocks, Redshift, BigQuery, Snowflake) rely on sorted storage, zone maps (min/max per block) and partition pruning, not on many B-trees. ClickHouse uses a **sparse primary index**: one entry per block of rows, sorted by the table's `ORDER BY` key. Check the docs for your engine before designing indexes; the mechanisms differ.
- **Monitoring.** Watch for unused indexes (`pg_stat_user_indexes.idx_scan` near zero for a long time) and for index bloat after heavy updates. Each unused index is pure write cost.
- **When not to index.** Small tables (a full scan of a few pages is cheaper), columns with very low cardinality on their own (a boolean with 50/50 values), and write-heavy staging tables that are only read once.

## 🧪 Try it

Use PostgreSQL 13 or later. Create the `orders` table above and insert one million rows with `generate_series`.

1. Run `EXPLAIN SELECT id, amount FROM orders WHERE customer_id = 42;` before and after `CREATE INDEX idx_orders_customer ON orders (customer_id);`. Describe the scan type in both plans.
2. Create `(customer_id, created_at)`. Which of these queries use it: (a) `WHERE created_at > now() - interval '7 days'`, (b) `WHERE customer_id = 42 AND created_at > now() - interval '7 days'`?
3. Create a partial index for `status = 'open'` and explain why a query with `WHERE status = 'paid'` does not use it.

<details><summary>Show solutions</summary>

**1.** Before: `Seq Scan on orders`. After: `Index Scan using idx_orders_customer` (or `Bitmap Heap Scan` if many rows match). The number of rows read drops from one million to the matching customer's rows.

**2.** (a) does not use `idx_..._cust_created` efficiently: the first column is missing. (b) uses it well: an equality on the first column plus a range on the second. Build a separate index on `(created_at)` if queries like (a) matter.

**3.** The partial index only covers rows with `status = 'open'`. A query for `'paid'` needs rows the index does not contain, so the planner cannot use it.

</details>

## 💬 Interview corner

**Question: "Why is my query slow even though the column has an index?"** Mid answer: "Check whether the query can use the index: function on the column, wrong leftmost prefix, or low selectivity. Then run `EXPLAIN ANALYZE`." Senior answer: "I'd look at the plan first. If it reads the table, I'd check predicate shape (sargability), composite column order, statistics freshness, and whether the planner's row estimate is far from the actual count. I'd then weigh the index against the write cost on that table."

**Question: "Should I index every column used in a WHERE clause?"** No. Each index slows writes and uses storage. Index the access paths your important queries use, verify with plans, and drop unused indexes.

> **🎤 Say this in the interview:** "An index is a sorted copy of some columns that lets the database skip rows. I pick the column order from the query: equality first, range last. I add `INCLUDE` columns when I want index-only scans, and I always count the write cost on the table."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Assuming a composite index `(a, b)` helps a query that filters only on `b`. It usually does not, because the B-tree is sorted by `a` first.

> **⚠️ Watch out:** Indexing a column and then wrapping it in a function. `WHERE DATE(created_at) = '2026-01-01'` cannot use an index on `created_at`. Rewrite as a range: `WHERE created_at >= '2026-01-01' AND created_at < '2026-01-02'`.

- Creating an index for every column "just in case". Each index is paid for on every write.
- Putting the range column first in a composite index, which makes the later columns almost useless for filtering.
- Forgetting that `LIKE '%text'` (leading wildcard) cannot use a B-tree index. Use a trigram (`pg_trgm`) GIN index if you need it.
- Expecting an index-only scan without a recent `VACUUM`. The visibility map must be up to date.
- Believing an index is always used. The planner decides by cost; low selectivity or small tables can make a sequential scan faster.

## ✅ Check yourself (quiz)

1. A table has `INDEX (country, city)`. Which query benefits most?
   - A) `WHERE city = 'Tehran'`
   - B) `WHERE country = 'IR' AND city = 'Tehran'`
   - C) `WHERE city LIKE 'Teh%'`
   - D) `WHERE id = 7`

<details><summary>Answer</summary>

**B** — both columns match the index order (equality on the leading column, equality on the second). A cannot use the leading column, so the index is not helpful. C is a prefix on `city`, which is not the leading column. D uses the primary key, not this index.

</details>

2. You need `SELECT customer_id, amount FROM orders WHERE customer_id = ?` to avoid touching the table. What is the right index?
   - A) `(amount)`
   - B) `(customer_id) INCLUDE (amount)`
   - C) `(customer_id, amount, id)` in every order
   - D) A GIN index on `amount`

<details><summary>Answer</summary>

**B** — the search key is `customer_id`, and `amount` is stored in the leaf for an index-only scan. A does not filter by `customer_id`. D is for multi-valued data, not this query.

</details>

3. A query `WHERE DATE(created_at) = '2026-01-01'` is slow with an index on `created_at`. What is the fix?
   - A) Add `DESC` to the index
   - B) Rewrite as a half-open range on `created_at`
   - C) Increase `work_mem`
   - D) Replace the B-tree with a hash index

<details><summary>Answer</summary>

**B** — the function on the column hides it from the index. A half-open range (`>= day` and `< next day`) keeps the index usable. A does not change this, and C and D do not address the function.

</details>

4. (Senior) A `orders` table gets 50,000 inserts per second and has 6 indexes, and 3 of them show zero scans for 90 days. What is your recommendation and why?
   - A) Add two more indexes to speed up writes
   - B) Drop the unused indexes after confirming the stats window covers a full business cycle, and check that no constraint needs them
   - C) Convert the table to a partial index
   - D) Switch the table to BRIN for all columns

<details><summary>Answer</summary>

**B** — unused indexes cost write time and storage with no read benefit. Confirm over a full cycle (monthly jobs can look unused for weeks), and keep indexes that back primary keys or unique constraints. A adds cost. C and D do not reduce the write load.

</details>

5. Why does `CREATE INDEX` on a huge production table need care?
   - A) Indexes cannot be created on populated tables
   - B) A plain `CREATE INDEX` blocks writes on the table while it builds; PostgreSQL offers `CREATE INDEX CONCURRENTLY`, which takes longer and cannot run inside a transaction block
   - C) Indexes are always rebuilt on read
   - D) The index is stored only in memory

<details><summary>Answer</summary>

**B** — a regular build takes a lock that blocks writes. `CONCURRENTLY` avoids that at the cost of more work and extra steps if it fails. A and C are false, and D is wrong because indexes are on disk.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What is a B-tree index? | A balanced, sorted tree of pages that finds keys in a few reads; the default in PostgreSQL and most OLTP engines |
| Leftmost-prefix rule | A composite index `(a, b)` is useful for filters on `a`, or on `a` and `b`; not for `b` alone |
| Covering index | An index that holds every column a query needs, so the table is not read (index-only scan) |
| What does `INCLUDE` do? | Stores extra columns in leaf pages without making them part of the search key (PostgreSQL 11+) |
| Partial index | An index built on a subset of rows, defined by a `WHERE` clause |
| BRIN index | Stores min/max per block range; tiny, good for large tables correlated with physical order |
| Why do indexes slow writes? | Each insert, and each update to an indexed column, must also update every index |
| Why does `DATE(col) = ...` not use an index? | The function hides the column; rewrite as a range on the raw column |

## 🔗 Go further

- Next: [[sql-query-optimization]] for reading plans and deciding when the planner ignores your index.
- Related: [[sql-joins]] (join keys need indexes), [[storage-engines]] (B-tree vs LSM), [[columnar-storage]] (how warehouses skip data without B-trees).
- PostgreSQL documentation: "Indexes" and "Index Types" chapters. Check the version you run; `INCLUDE` and HOT behaviour depend on it.
