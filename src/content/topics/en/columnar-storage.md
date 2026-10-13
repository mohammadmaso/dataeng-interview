---
order: 39
tid: "columnar-storage"
track: "4"
section: null
num: "4.2"
title: "Row vs Columnar Storage & Vectorized Execution"
type: "concept"
level: "intermediate"
priority: "P0"
est_hours: 3
summary: "Why columnar storage is faster for analytics: read only needed columns, better compression, vectorized (SIMD) execution. The foundation of ClickHouse, StarRocks and Parquet."
covers: ["late materialization","zone maps / min-max indexes","sparse primary index","column encodings"]
interview_qs: []
extras: []
prerequisites: ["oltp-vs-olap","sql-indexing"]
related: ["data-formats","clickhouse","starrocks-doris","compression","physical-modeling-olap","storage-engines"]
---
## 🎯 TL;DR

> A **row store** keeps all columns of a row together. A **column store** keeps each column together. Analytics reads a few columns from many rows, so column layout reads less data, compresses better (similar values sit side by side) and lets the CPU process values in batches (**vectorized execution**). The price: reading or writing one full row touches many places.

- Projection: read only the columns the query names.
- Encoding and compression: a column of one type with repeated or sorted values shrinks a lot.
- Skipping: per-block min/max statistics let the engine ignore blocks that cannot match a filter.
- Execution: operators work on arrays of values, not one row at a time.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Index cards vs drawers. A row store is one card per customer, with name, city, age and salary written on it. A column store is one drawer per field: all names in one drawer, all cities in another. To find "everything about customer 42", the card is easier. To find "average salary in Tehran", you open only two drawers and skip the rest.

Here is the same three-row table in both layouts:

```text
row store (one record after another):
(1, 'Ali',  'Tehran', 31, 9000)
(2, 'Sara', 'Shiraz', 27, 12000)
(3, 'Reza', 'Tehran', 45, 10000)

column store (one array per column):
id:     [1, 2, 3]
name:   ['Ali', 'Sara', 'Reza']
city:   ['Tehran', 'Shiraz', 'Tehran']
age:    [31, 27, 45]
salary: [9000, 12000, 10000]
```

Query: `SELECT city, AVG(salary) FROM people GROUP BY city;`. The row store reads all five fields of every row. The column store reads only `city` and `salary`.

The idea of processing a whole array at once can be seen in a few lines of Python:

```python
import numpy as np

salary = np.array([9000, 12000, 10000], dtype=np.int64)
print(salary[salary > 9500].sum())  # one pass over the array: 12000 + 10000 = 22000
```

The filter and the sum each run as one tight loop over the array, not as a function call per row.

## 🔧 How it really works (Mid)

**1. Column pruning (projection).** Each column is stored separately, so a query that names three columns of sixty reads roughly three columns of data. Parquet files and ClickHouse table parts both lay data out this way.

**2. Encodings and compression.** Values of one column share a type and often a narrow range. Common encodings:

- **Dictionary encoding**: repeated strings like `'Tehran'` become small integer codes plus one dictionary.
- **Run-length encoding (RLE)**: `[7, 7, 7, 7, 8]` becomes `(7, ×4), (8, ×1)`. Sorted columns compress very well.
- **Delta encoding**: increasing values like timestamps are stored as small differences.
- A general-purpose codec (LZ4, ZSTD) can run on top of these.

**3. Skipping with statistics.** Each block of a column stores its min and max. A filter `WHERE event_date = '2026-03-01'` skips every block whose range excludes that date. If the data is sorted by `event_date`, most blocks are skipped. This is why the sort key matters.

**4. Sparse primary index.** ClickHouse keeps one index entry per group of rows (a *granule*, 8192 rows by default; check the docs for your version), not one per row. The index is small enough to stay in memory, and it finds the start of the right granule, then the engine scans from there.

**5. Late materialization.** The engine first reads only the filter columns, computes the matching row positions, and then fetches the other columns for those rows only. This is cheap when the filter is selective. When most rows match, the extra fetches become many random reads.

```mermaid
flowchart LR
    Q["WHERE city = 'Tehran'"] --> F["Read city column only"]
    F --> P["Matching row positions"]
    P --> S["Fetch salary for those rows"]
    S --> A["AVG(salary)"]
```

**6. Vectorized execution.** Classic row-at-a-time engines call a `next()` function per row, which costs CPU in calls and branches. Vectorized engines process batches of values in tight loops over arrays. That lets the CPU use SIMD instructions and keeps data in cache. The X100 research prototype (later Vectorwise) made this approach popular.

**Numbers.** A table has 1 billion rows and 50 columns, with an average row size of 400 bytes. A query averages 3 columns of about 8 bytes each. A row store reads about 1,000,000,000 × 400 B = 400 GB. A column store reads 1,000,000,000 × 24 B = 24 GB raw. At an assumed 5:1 compression that is about 4.8 GB. The ratio depends on the data; the order of magnitude is the point.

## 🧠 Senior depth & trade-offs

- **Small inserts create many parts.** Columnar engines write immutable parts (ClickHouse MergeTree) or immutable files (Parquet on object storage). One row per insert creates thousands of tiny parts that background merges must clean up. Batch inserts into thousands of rows, or buffer through a queue. Watch the part count.
- **Updates and deletes are expensive.** Rewriting a column block to change one value is costly. Engines offer merge-time deduplication, delete markers or lakehouse table formats such as Iceberg and Delta. The exact behavior depends on the engine and version, so check its docs before you promise anything.
- **Point lookups are weak.** A sparse index only helps for filters on a prefix of the sort key. Choose the sort key from the most frequent filters, and check that the engine uses it (`EXPLAIN` or the query profile shows granules or blocks read).
- **Parquet internals.** A Parquet file has row groups, and each row group has one column chunk per column, split into pages. Min/max statistics and dictionary pages live in the footer and pages, which is what [[predicate-pushdown]] and column pruning use. See [[parquet]] and [[data-formats]].
- **Vectorization needs batches.** Very small batches waste the per-batch overhead. Very large batches may not fit in cache. Engines tune this internally; your job is to avoid tiny queries on huge columnar tables.
- **Wide tables and joins.** Columnar stores favor wide, denormalized tables, but a wide table duplicates data and makes updates harder. This is a trade-off, not a rule. See [[physical-modeling-olap]].
- **When not to use it.** Frequent single-row updates, many point lookups by key, or small tables where the overhead of parts and metadata dominates. Use a row store (an OLTP database) for those.
- **Engines in practice.** ClickHouse uses MergeTree parts and sparse indexes. StarRocks and Doris use columnar segments with zone maps and short-key indexes. Trino and Spark read Parquet with column pruning and pushdown, and Spark has a vectorized Parquet reader. Their details differ; verify them in the docs for your version.

## 🧪 Try it

Use DuckDB (in-process, columnar, no server needed). Run the statements in a DuckDB shell or with Python's `duckdb` package.

```sql
CREATE TABLE t AS
SELECT range AS id,
       range % 1000 AS city_id,
       random() * 100 AS amount
FROM range(10000000);

-- Query A: needs one column
EXPLAIN ANALYZE SELECT city_id, SUM(amount) FROM t GROUP BY city_id;

-- Query B: needs every column
EXPLAIN ANALYZE SELECT * FROM t WHERE id > 9999990;

-- Export to Parquet and inspect the per-row-group min/max statistics
COPY t TO 'out.parquet' (FORMAT parquet);
SELECT * FROM parquet_metadata('out.parquet') LIMIT 5;
```

1. Predict which of queries A and B reads more data, and why. Then compare the `EXPLAIN ANALYZE` output.
2. Rewrite `SELECT *` as `SELECT id, city_id, amount` and compare the scanned data.
3. Predict how much the filter on `id` would skip if the table were sorted by `city_id` instead of `id`. Explain your answer in one sentence.

<details><summary>Show what to look for</summary>

Query A scans only two columns and aggregates in batches. Query B needs every column, but its filter on `id` can skip row groups whose max `id` is below the range, so the scan is short. For question 3, sorting by `id` makes the min/max ranges of row groups disjoint and the filter skips most of them. Sorting by `city_id` makes every row group contain almost every `id` range, so skipping on `id` does almost nothing.

</details>

## 💬 Interview corner

**Q: Why are column stores faster for analytics?**

> **🎤 Say this in the interview:** A query usually reads a few columns from many rows. A column store reads only those columns, compresses them well because values are alike, skips blocks using min/max statistics, and runs vectorized loops over arrays. The trade-off is that single-row writes and full-row reads are slower, so it is the wrong layout for OLTP.

- **Junior answer:** says the database "reads only the needed columns" and gives an example.
- **Mid answer:** explains encodings (dictionary, RLE, delta), zone maps and vectorized execution.
- **Senior answer:** adds sort-key choice, small-insert part explosion, update costs, late materialization for selective filters, and how Parquet footers drive pruning.

**Follow-up: "Why does ClickHouse suggest batch inserts?"** Each insert creates a new part on disk. Thousands of tiny parts mean many files to read per query and heavy background merging.

## ⚠️ Common mistakes

- Choosing a sort key by column name order instead of by the most frequent filter.
- Inserting one row at a time into a columnar engine and wondering why merges fall behind.
- Using `SELECT *` on a wide columnar table. The engine can only skip I/O for columns you do not name.
- Expecting a column store to be fast at point updates of single rows.
- Assuming compression ratios from a blog post. Measure on your own data.
- Forgetting that late materialization is not always a win. It hurts when most rows match.

## ✅ Check yourself (quiz)

1. A table has 60 columns. Which query benefits most from a column layout?
   - A) `SELECT * FROM products WHERE id = 42`
   - B) `SELECT AVG(price) FROM products`
   - C) `INSERT` one row into `products`
   - D) `UPDATE products SET name = 'x' WHERE id = 42`

<details><summary>Answer</summary>

**B** — the query reads one column across many rows, which is the case columnar layout is built for. The other three touch whole rows or single keys.

</details>

2. Why does a column store usually compress better than a row store on the same data?
   - A) Columns are encrypted before compression
   - B) A column holds values of one type that are often alike, so dictionary and run-length encodings work well
   - C) Rows contain many duplicate values that columns remove
   - D) Column stores drop all NULL values

<details><summary>Answer</summary>

**B** — similar values of one type are what encodings like dictionary, RLE and delta exploit. Compression does not depend on encryption or on dropping NULLs.

</details>

3. A table is sorted by `event_date`. Which mechanism lets the engine skip most of the data for `WHERE event_date = '2026-03-01'`?
   - A) Vectorized execution
   - B) Per-block min/max statistics (zone maps) on the sorted column
   - C) Using a row store instead
   - D) Late materialization

<details><summary>Answer</summary>

**B** — sorted data gives narrow min/max ranges per block, so most blocks cannot contain the date and are skipped. Vectorized execution and late materialization do not skip blocks.

</details>

4. **(Senior)** A ClickHouse table receives one row per insert, about 1,000 times a second. Queries slow down and background merges fall behind. What is the cause and the fix?
   - A) Too many columns; remove half of them
   - B) Each insert creates a small immutable part, so the part count explodes; batch inserts into thousands of rows or buffer them through a queue
   - C) Compression is too strong; disable it
   - D) `SELECT *` is used; replace it with a view

<details><summary>Answer</summary>

**B** — immutable parts multiply faster than merges can combine them. Batching at the source or buffering in a queue (for example Kafka) fixes the root cause.

</details>

5. **(Senior)** When can late materialization make a query slower?
   - A) When the filter is very selective and only a few rows survive
   - B) When the filter matches most rows, so fetching the other columns becomes many random reads
   - C) When the query has a `GROUP BY`
   - D) Never; it is always faster

<details><summary>Answer</summary>

**B** — with a non-selective filter, the second phase fetches almost every row's other columns at random positions, which costs more than reading the columns sequentially. Late materialization is a bet on selectivity.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Row store vs column store | Row store keeps a row together; column store keeps a column together |
| Why column layout helps analytics | Reads only the needed columns and skips blocks by statistics |
| Dictionary encoding | Repeated values become small integer codes plus one dictionary |
| Run-length encoding | Repeated runs are stored as (value, count) pairs |
| Zone map | Per-block min and max, used to skip blocks that cannot match a filter |
| Sparse primary index | One index entry per group of rows, small enough to live in memory |
| Vectorized execution | Operators process batches of values in tight loops, not one row per call |
| Late materialization | Read filter columns first, then fetch the rest only for matching rows |

## 🔗 Go further

- [[oltp-vs-olap]] explains the workloads that make this layout the right choice.
- [[data-formats]] and [[parquet]] show the same ideas in a file format.
- [[compression]] covers the codecs that run on top of encodings.
- [[clickhouse]] and [[starrocks-doris]] show how two engines apply these ideas.
- [[physical-modeling-olap]] covers sort keys and table design for columnar and MPP engines.
