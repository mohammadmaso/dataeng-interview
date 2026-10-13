---
order: 33
tid: "python-dataframes"
track: "3"
section: null
num: "3.4"
title: "DataFrames: pandas, Polars & DuckDB"
type: "technology"
level: "intermediate"
priority: "P1"
est_hours: 4
summary: "Single-machine DataFrame work. pandas is most common; Polars and DuckDB are faster and columnar. When do you no longer need Spark?"
covers: ["vectorization","groupby/merge","memory dtypes","lazy evaluation in Polars","DuckDB over Parquet","Apache Arrow"]
interview_qs: []
extras: []
prerequisites: ["python-core", "columnar-storage"]
related: ["python-concurrency", "python-large-files", "python-production", "data-formats", "pyspark-api", "spark-architecture", "other-batch-engines"]
---

## 🎯 TL;DR

> A **DataFrame** is a table in memory with named columns. **pandas** is the default tool, **Polars** and **DuckDB** are newer engines that are faster on one machine because they work on **columns**, run in **parallel**, and (Polars, DuckDB) can **optimize the whole query** before running it.

- Use **vectorized** operations (whole-column expressions). Avoid Python loops over rows.
- Watch **memory dtypes**: a column of repeated strings as `object` can take 10× the memory of a `category`.
- **Lazy** engines build a plan first and run it once; eager engines run each step immediately.
- **DuckDB** lets you write SQL directly over Parquet files on disk without loading them first.
- Single-machine tools are often enough for tens of GB. Spark is for data that does not fit one machine, or for cluster-wide shuffles.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A DataFrame is a spreadsheet with typed columns. Doing `amount * 1.09` on a whole column is like filling a formula down 1 million rows in one click. A Python `for` loop over rows is like typing each formula by hand.

```python
import pandas as pd

orders = pd.DataFrame({
    "customer": ["ali", "sara", "ali", "reza"],
    "amount":   [10.0, 5.5, 7.0, 3.0],
    "status":   ["paid", "paid", "refunded", "paid"],
})

paid = orders[orders["status"] == "paid"]           # filter: one vectorized comparison
per_customer = (
    paid.groupby("customer", as_index=False)        # group + aggregate
        .agg(total=("amount", "sum"), orders=("amount", "size"))
)
print(per_customer)
#   customer  total  orders
# 0      ali   10.0       1
# 1     reza    3.0       1
# 2     sara    5.5       1
```

Notice what is missing: no loop. The filter, the group and the sum are expressed as operations on whole columns, and pandas runs them in optimized compiled code.

## 🔧 How it really works (Mid)

### Columnar memory and vectorization

Each column is stored as one contiguous block of values. A sum over `amount` reads one block, so the CPU's caches and SIMD instructions work efficiently. A row-by-row Python loop does the opposite: it creates a Python object for every value and dispatches on each one, which is typically 10–100× slower.

```python
import numpy as np, pandas as pd

df = pd.DataFrame({"x": np.random.rand(1_000_000)})
df["y_slow"] = [v * 2 for v in df["x"]]   # Python loop: creates a Python float per row
df["y_fast"] = df["x"] * 2                # vectorized: one compiled operation
```

Rule of thumb: if you wrote `for _, row in df.iterrows()`, look for an expression, `np.where`, `.map` with a vectorized function, or a SQL/Polars expression instead.

### Memory and dtypes

pandas stores text in `object` columns by default. Each value is a separate Python string object with its own overhead. Repeated values (status codes, country names) waste memory.

```python
import pandas as pd

df = pd.DataFrame({"status": ["paid"] * 1_000_000})
print(df.memory_usage(deep=True).sum() / 1e6)      # tens of MB of Python strings

df["status"] = df["status"].astype("category")     # stores small integer codes + a lookup table
print(df.memory_usage(deep=True).sum() / 1e6)      # about 1 MB
```

Other useful choices: `int32` instead of `int64` when the range allows, `float32` for non-financial measures, and the Arrow-backed string type (`pd.ArrowDtype(pa.string())` or `dtype="string[pyarrow]"`) for less overhead. Choose the dtype when you load data (`read_csv(..., dtype=...)`) and not after the fact, because the peak memory during loading is what breaks jobs.

### groupby and merge

`groupby` splits rows by key, applies an aggregate and combines the results. `merge` is a SQL-style join:

```python
customers = pd.DataFrame({"customer": ["ali", "sara"], "city": ["Tehran", "Shiraz"]})
enriched = orders.merge(customers, on="customer", how="left")   # reza gets NaN for city
```

Joins and groupbys build hash tables on the key. Duplicated keys on one side multiply rows (a many-to-many join), which is the most common silent bug: always check row counts before and after a merge, or pass `validate="many_to_one"`.

### Lazy evaluation with Polars

Polars has two APIs: eager `DataFrame` and lazy `LazyFrame`. Lazy means nothing runs until `.collect()`. The optimizer can then push filters down to the file reader and read only the columns you use.

```python
import polars as pl

orders.to_parquet("orders.parquet", index=False)  # needs pyarrow; creates a sample file

result = (
    pl.scan_parquet("orders.parquet")             # lazy: reads metadata only
      .filter(pl.col("status") == "paid")         # pushed down into the Parquet reader
      .group_by("customer")
      .agg(pl.col("amount").sum().alias("total"), pl.len().alias("orders"))
      .sort("customer")
      .collect()                                  # the whole plan runs here
)
print(result)
```

`lf.explain()` prints the optimized plan. If you see `FILTER` and a short column list close to the scan, the optimizer did its job. This is the same idea as Spark's Catalyst optimizer and as predicate pushdown in Parquet readers.

### DuckDB over Parquet

DuckDB is an in-process analytical database: a library, not a server. It reads Parquet, CSV and Arrow directly, and runs SQL with a vectorized, parallel engine.

```python
import duckdb

result = duckdb.sql("""
    SELECT customer, SUM(amount) AS total, COUNT(*) AS orders
    FROM 'orders.parquet'
    WHERE status = 'paid'
    GROUP BY customer
    ORDER BY customer
""").df()                                          # .pl() returns Polars, .arrow() returns Arrow

print(result)
```

The string `'orders.parquet'` in `FROM` is a file reference. DuckDB reads only the columns and row groups it needs, which is why it is excellent for ad-hoc analysis over files that are too big for pandas.

### Apache Arrow: the shared memory format

Arrow defines an in-memory columnar layout that many tools share. pandas (with the Arrow backend), Polars, DuckDB and Spark's Python bridge can exchange data without copying or re-parsing it, often as zero-copy. Practically: use Arrow tables as the interchange object between tools, and Parquet as the file format on disk ([[columnar-storage]], [[parquet]]).

```python
import duckdb, polars as pl

arrow_tbl = duckdb.sql("SELECT * FROM 'orders.parquet'").arrow()
df_polars = pl.from_arrow(arrow_tbl)
```

## 🧠 Senior depth & trade-offs

- **Choosing the tool.**
  - **pandas**: the ecosystem default, huge API surface, single-threaded for most operations, and memory-hungry. Good up to a few GB comfortably; plan on several times the data size in RAM.
  - **Polars**: multi-threaded, lazy optimizer, strict types, and a smaller but growing API. Good for ETL on one machine when the data is larger than pandas can handle.
  - **DuckDB**: best when your logic is naturally SQL and the data is in files (Parquet, CSV) or in Arrow. Runs in a notebook, a container or a Python job with no server.
  - Rule of thumb, to verify on your own workload: one machine with enough RAM and local disk handles tens of GB well with DuckDB or Polars. Spark becomes necessary when the data does not fit on one node, when you need a distributed shuffle, or when many jobs must share a cluster with fault tolerance.
- **Spill and out-of-core.** Polars streaming and DuckDB can spill to disk when memory is short; pandas generally cannot. Check the docs of your version for the exact option names before you promise a job will run on a laptop.
- **Schema drift.** A pandas `read_csv` infers types from the first chunk. One later row with `"N/A"` makes an integer column `object`, and a join then silently fails to match. Declare schemas (dtypes or a Polars schema) at the boundary, and validate ([[data-quality]]).
- **Determinism and row order.** `groupby` in pandas sorts keys by default; Polars `group_by` does not guarantee order unless you `sort`. Never rely on implicit order in a test or a report.
- **Nulls and NaN.** In pandas `NaN` is a float, so an integer column with a missing value becomes float. Polars and Arrow use a separate validity bitmap, so nulls are typed correctly. Check this when you move data between engines.
- **Monitoring.** For a batch job on one machine, track peak RSS, wall time per step, row counts in and out of every join, and the number of rows rejected by validation. A join that doubles the row count is a bug you can see only if you count.
- **When not to use a DataFrame at all.** If the transformation is a few SQL joins into a warehouse table, leave it in the warehouse. Moving data out to a laptop for a DataFrame job adds a copy, a failure point and a schedule problem.

## 🧪 Try it

Create `orders.parquet` as in the Polars example and a `customers.csv` with `customer,city` for `ali`, `sara` and `reza`.

1. Write the same "paid total per customer" query in **pandas**, **Polars** (lazy) and **DuckDB** SQL. Confirm all three give the same numbers.
2. Load `orders.parquet` with `pl.scan_parquet(...).select(["customer"]).collect()`. Explain what the optimizer avoided reading.
3. Join `orders` with `customers` and add a row to `customers` with a duplicated key `ali`. Predict the row count of the result before running it, then add `validate="many_to_one"` to the pandas merge and observe the error.

<details><summary>Show solutions</summary>

**1.** The three versions produce the same rows (`ali 10.0`, `reza 3.0`, `sara 5.5`). The pandas version is in the lesson body; Polars is the lazy chain above; DuckDB is the SQL above. They should match exactly, which is the point: the choice is about performance, memory and ergonomics, not results.

**2.** The `select` lets the Parquet reader skip every other column. `explain()` shows a projection on `customer` only. The file is still scanned, but far less data is decoded and held in memory.

**3.** Before the change, `orders` has 4 rows and `customers` has 2 keys (`ali`, `sara`), plus `reza` without a match in a left join. With a second `ali`, the join produces one row per matching customer row, so `ali`'s two orders now match two customer rows and produce 4 rows instead of 2. Total result: 6 rows. `validate="many_to_one"` raises `MergeError` because the right side is not unique on `customer`, which is exactly the protection you want.

</details>

## 💬 Interview corner

**Question: "pandas is slow on my 20 GB file. What do you do?"**
Mid answer: "Read in chunks with `chunksize`, use category dtypes, or switch to Polars."
Senior answer: "First I check whether the work is filtering and aggregating, because then the file should be read by a columnar engine that can push predicates and projections down. I'd use DuckDB or Polars lazy over Parquet, measure peak memory, and only move to Spark if the data does not fit on one machine or needs a cluster shuffle. I'd also check whether the data should be in Parquet at all."

**Question: "When do you no longer need Spark?"**
Senior answer: "When the data and the working set fit comfortably on one machine, the logic is column-oriented or SQL, and there is no need for distributed fault tolerance. Spark's cost is cluster management, serialization and shuffles, and for small data those costs are pure overhead. I'd still keep Spark if the same code must run on petabyte-scale data later."

> **🎤 Say this in the interview:** "I pick the tool by data size and by where the data lives. pandas for small, Polars or DuckDB for larger single-machine work, and Spark when the data or the shuffle needs a cluster. Whatever I choose, I validate row counts at joins and fix dtypes at load."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Looping over rows with `iterrows()` or `apply(axis=1)` for simple arithmetic. It is slow and it hides bugs. Use column expressions.

> **⚠️ Watch out:** A merge on a non-unique key silently multiplies rows. Always validate the key uniqueness on the side you expect to be unique.

- Loading a big CSV with default dtypes and then wondering why memory is 5× the file size.
- Using `object` for low-cardinality strings instead of `category`.
- Assuming Polars `group_by` output is sorted. It is not unless you sort.
- Forgetting that pandas `NaN` converts integer columns to float, which breaks joins on IDs.
- Calling `.collect()` early in a Polars chain, which defeats the optimizer.
- Choosing Spark for a 500 MB file out of habit.

## ✅ Check yourself (quiz)

1. What is the main reason vectorized column operations are faster than a Python `for` loop over rows?
   - A) They use more threads
   - B) They run in compiled code over contiguous columns, avoiding per-row Python object work
   - C) They skip the NaN check
   - D) They write results to disk

<details><summary>Answer</summary>

**B** — a column operation runs in compiled code over a contiguous block, while a Python loop creates and dispatches Python objects for every row.

</details>

2. In Polars, what does `pl.scan_parquet(...).filter(...).collect()` do differently from `pl.read_parquet(...).filter(...)`?
   - A) Nothing; they are identical
   - B) The lazy version builds a plan and can push the filter and column selection into the reader before running
   - C) The lazy version always uses less memory and never reads the file
   - D) The lazy version writes a temporary Parquet file

<details><summary>Answer</summary>

**B** — `scan_parquet` is lazy. The optimizer can push the filter down and read only the needed columns. Eager `read_parquet` loads the whole file first.

</details>

3. Why can a column of 1 million repeated status strings use much more memory in pandas than after `astype("category")`?
   - A) `category` compresses with gzip
   - B) `object` stores a separate Python string object per value, while `category` stores small integer codes plus one lookup table
   - C) `category` removes the null values
   - D) pandas cannot store strings at all without `category`

<details><summary>Answer</summary>

**B** — the object column keeps one Python string per row. The category column stores compact codes that point into a small table of distinct values.

</details>

4. **(Senior)** A pandas `merge` on `customer_id` produces 3× the expected rows after a dimension table was refreshed. What is the likely cause and the guard you add?

   - A) The merge is `how="left"`, which always triples rows
   - B) The dimension now has duplicate keys, so each fact row matches several dimension rows; check uniqueness and use `validate="many_to_one"` (or dedupe the dimension first)
   - C) pandas merges are non-deterministic; use `sort=True`
   - D) The fix is `groupby` after the merge

<details><summary>Answer</summary>

**B** — a many-to-many join multiplies rows. Enforce the cardinality with `validate=` and fix the dimension so that its key is unique (for example, with a latest-version filter).

</details>

5. **(Senior)** When would you choose DuckDB over Spark for a nightly job of 30 GB of Parquet files on one 64 GB machine, and what would make you change your mind?

   - A) Never; Spark is always faster
   - B) DuckDB fits well when the data fits the machine, the logic is SQL/columnar, and no cluster is needed; switch to Spark if the data grows beyond one node, the job needs distributed shuffles or fault tolerance across many nodes, or the team must share one cluster platform
   - C) DuckDB only works with CSV
   - D) DuckDB cannot read Parquet

<details><summary>Answer</summary>

**B** — the decision depends on whether the data and work fit one node and whether you need distributed execution. Measure on your own data; do not trust a generic speed claim.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Why are vectorized operations faster than row loops? | They run compiled code over contiguous columns, with no per-row Python objects |
| `object` vs `category` for repeated strings | `object` stores one Python string per row; `category` stores small codes plus one lookup table |
| What does `scan_parquet` return? | A lazy plan (LazyFrame) that runs only on `.collect()` |
| What is predicate pushdown? | Moving a filter into the file reader so less data is read |
| What does `validate="many_to_one"` protect against? | Duplicate keys on the right side of a merge that multiply rows |
| What is Apache Arrow? | A shared in-memory columnar format that tools exchange with little or no copying |
| What does DuckDB do? | Runs SQL in-process, directly over Parquet, CSV and Arrow |
| When is Spark necessary? | When the data or shuffle does not fit one machine, or when a cluster is required |

## 🔗 Go further

- **python-concurrency** explains why pandas and NumPy operations are fast in C while Python loops are not, and how the GIL affects parallel work.
- **python-large-files** covers chunked reading and streaming, the other path for data that does not fit in memory.
- **columnar-storage** and **data-formats** explain why Parquet and column layout make scans cheap.
- **pyspark-api** and **spark-architecture** show what changes when the same logic has to run distributed.
- **other-batch-engines** compares Spark with other batch engines for the cases where a single machine is not enough.
