---
order: 34
tid: "pyspark-api"
track: "3"
section: null
num: "3.5"
title: "PySpark API"
type: "technology"
level: "intermediate"
priority: "P0"
est_hours: 5
summary: "Writing jobs with the Python DataFrame API: transformations vs actions, UDFs and their cost, Pandas UDFs, windows in PySpark, and testing jobs."
covers: ["Python ↔ JVM serialization overhead","Arrow optimization","explode/struct/array ops"]
interview_qs: ["Rewrite this UDF with built-in functions","Why is my Python UDF slow?"]
extras: []
prerequisites: ["python-core", "spark-architecture"]
related: ["spark-catalyst", "spark-shuffle", "spark-aqe", "python-dataframes", "spark-writes", "spark-memory-tuning", "spark-joins-skew", "spark-ui-debugging"]
---
## 🎯 TL;DR

> PySpark is a Python **front-end** for a plan that runs in the JVM. Write with built-in DataFrame functions so Spark can optimise the whole plan. Every Python function you add (a UDF) is a black box that costs a round trip between Python and the JVM.

- **Transformations** (`filter`, `select`, `withColumn`, `groupBy`, `join`) only build a plan. **Actions** (`count`, `collect`, `write`, `show`) run it.
- A **Python UDF** sends rows to a Python worker and back, row by row, and Catalyst cannot see inside it.
- A **Pandas UDF** (vectorised UDF) moves batches as Arrow columns, which is far cheaper than per-row calls, but still costs more than a built-in function.
- Prefer `F.*` functions, `explode`, `struct`, `array` and window specs over UDFs. Test jobs on small local DataFrames with a fixture.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A PySpark script is a recipe written by you and cooked by a kitchen (the JVM). Writing `df.filter(...)` only writes a line on the recipe. Nothing is cooked until you ask for the dish with an action such as `count()` or `write`.

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").appName("demo").getOrCreate()

df = spark.createDataFrame(
    [("ali", "data", 9000), ("sara", "data", 12000), ("maryam", "web", 8000)],
    ["name", "dept", "salary"],
)

high = df.filter(F.col("salary") > 8500)   # transformation: nothing runs yet
high.explain()                             # prints the physical plan
print(high.count())                        # action: the job runs -> 2
```

The DataFrame is a table with a schema. Columns are referenced with `F.col("name")` (or the plain string `"name"` in most places). Keep this habit: express each step as a function on columns, not as a loop over rows.

## 🔧 How it really works (Mid)

### Lazy plans and actions

Each transformation returns a new DataFrame that points at its parent. When an action runs, Spark's optimiser (Catalyst) rewrites the whole chain: it pushes filters down, prunes unused columns and chooses join strategies. This is why a `filter` written after a `join` can still be applied before it.

| Kind | Examples | What happens |
|---|---|---|
| Narrow transformation | `select`, `filter`, `withColumn` | Each partition is processed on its own; no shuffle |
| Wide transformation | `groupBy().agg`, `join`, `distinct`, `orderBy` | Data is redistributed by key: a shuffle |
| Action | `count`, `collect`, `show`, `write.parquet` | Triggers execution of the plan |

Never call `collect()` on a large DataFrame: it pulls every row into the driver's memory.

### Built-in functions versus UDFs

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Slow path: each row goes to a Python worker and back
@F.udf(returnType=DoubleType())
def with_tax_udf(salary):
    return salary * 1.09 if salary is not None else None

# Fast path: an expression Spark runs and optimises inside the JVM
df_fast = df.withColumn("with_tax", F.col("salary") * 1.09)
df_slow = df.withColumn("with_tax", with_tax_udf("salary"))
```

For a Python UDF, Spark serialises the row, sends it to a separate Python process, deserialises the result and returns it. That per-row cost dominates the job. The UDF is also opaque to Catalyst: it cannot push filters through it or reason about its nulls. The `df_fast` version has none of these costs.

### Pandas UDFs

When logic cannot be written with built-ins, use a Pandas UDF. It receives a batch of values as a `pandas.Series` via Apache Arrow, so the cost is paid per batch:

```python
import pandas as pd
from pyspark.sql import functions as F

@F.pandas_udf("double")
def with_tax_vec(s: pd.Series) -> pd.Series:
    return s * 1.09

df.withColumn("with_tax", with_tax_vec("salary")).show()
```

Pandas UDFs need `pyarrow` on the workers. Arrow also speeds up `toPandas()` when the Arrow setting is enabled (`spark.sql.execution.arrow.pyspark.enabled`); check the docs for your Spark version.

### Arrays, structs and explode

Real data is nested. `explode` turns one row with an array into one row per element; `explode_outer` keeps rows whose array is null or empty:

```python
orders = spark.createDataFrame([(1, ["book", "pen"]), (2, ["lamp"]), (3, [])],
                               ["order_id", "items"])

orders.select("order_id", F.explode("items").alias("item")).show()
# (1, book), (1, pen), (2, lamp): order 3 is dropped

orders.select("order_id", F.explode_outer("items").alias("item")).show()
# order 3 kept with item = null

orders.select("order_id", F.size("items").alias("n_items")).show()
# 2, 1, 0
```

`F.struct("a", "b")` builds a nested column and `F.col("s.a")` reads a field back out. Choose `explode` when you need one row per element, and use `F.size` or `F.array_contains` when you only need a property of the array.

### Window functions in PySpark

The same concepts as SQL windows apply, with a Python spec:

```python
from pyspark.sql import Window, functions as F

w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
top_per_dept = (df.withColumn("rn", F.row_number().over(w))
                  .filter(F.col("rn") == 1)
                  .drop("rn"))
top_per_dept.show()   # sara (data), maryam (web)
```

A window without `partitionBy` moves every row into one partition. On big data that is a single-task bottleneck, the same warning as in SQL.

### Testing jobs

Keep transformations as functions that take and return DataFrames. Then test them on tiny inputs with a local session:

```python
import pytest
from pyspark.sql import SparkSession, functions as F

@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield s
    s.stop()

def add_tax(df):
    return df.withColumn("with_tax", F.col("salary") * 1.09)

def test_add_tax(spark):
    df = spark.createDataFrame([(100.0,)], ["salary"])
    assert add_tax(df).collect()[0]["with_tax"] == pytest.approx(109.0)
```

## 🧠 Senior depth & trade-offs

- **Read the plan.** `df.explain(True)` shows the logical and physical plans. Look for `Exchange` (a shuffle), `BroadcastHashJoin` versus `SortMergeJoin`, and whether a Python UDF appears as `BatchEvalPython` or `ArrowEvalPython`. Those nodes are where the time goes.
- **Python-worker overhead.** Each executor runs Python worker processes next to the JVM. Memory is split between the JVM heap, off-heap, and Python. A UDF that builds large objects can be killed by YARN or Kubernetes even when the JVM heap looks fine. See [[spark-memory-tuning]].
- **Shuffles and partitions.** Wide transformations shuffle data. The partition count (`spark.sql.shuffle.partitions`, or adaptive execution) decides the task size. Too few partitions cause skew and spill; too many cause scheduling overhead and small files. See [[spark-shuffle]] and [[spark-aqe]].
- **`groupBy` with a UDF.** `groupBy(...).applyInPandas(fn, schema)` runs a function per group with a pandas DataFrame. It is convenient but materialises each group in one worker; a skewed key becomes one huge task. See [[data-skew]].
- **Driver memory.** `collect()`, `toPandas()` and `.first()` on large data are driver operations. In production, write results to storage (for example Parquet) instead of pulling them to one machine.
- **When not to use PySpark.** For small data (a few GB that fits on one machine) a single-machine engine is simpler and often faster; Spark pays a fixed startup and planning cost.

## 🧪 Try it

1. **Rewrite the UDF.** This UDF returns `"high"` if salary is above 10000, `"mid"` if it is between 8000 and 10000 inclusive, and `"low"` otherwise. Rewrite it with `F.when` and show that it gives the same results on the three sample rows.

   ```python
   @F.udf("string")
   def band(salary):
       if salary > 10000:
           return "high"
       if salary >= 8000:
           return "mid"
       return "low"
   ```
2. **Explode and count.** Given `events = spark.createDataFrame([(1, ["a", "b"]), (2, ["b"])], ["id", "tags"])`, produce the number of events per tag.
3. **Second highest salary per department** using a window and `dense_rank`.

<details><summary>Show solutions</summary>

**1.**
```python
from pyspark.sql import functions as F

rows = spark.createDataFrame([(12000,), (9000,), (7000,)], ["salary"])
banded = rows.withColumn(
    "band",
    F.when(F.col("salary") > 10000, "high")
     .when(F.col("salary") >= 8000, "mid")
     .otherwise("low"),
)
banded.show()   # high, mid, low
```
The `F.when` chain is evaluated inside the JVM and can be optimised, so there is no Python round trip.

**2.**
```python
from pyspark.sql import functions as F

events = spark.createDataFrame([(1, ["a", "b"]), (2, ["b"])], ["id", "tags"])
(events.select(F.explode("tags").alias("tag"))
       .groupBy("tag").count()
       .show())
# b: 2, a: 1
```

**3.**
```python
from pyspark.sql import Window, functions as F

w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
second = (df.withColumn("dr", F.dense_rank().over(w))
            .filter(F.col("dr") == 2)
            .select("dept", "salary")
            .distinct())
second.show()
```
Use `dense_rank` so tied top salaries do not hide the second distinct salary. Use `distinct()` if a department has more than one person at that rank.

</details>

## 💬 Interview corner

**"Rewrite this UDF with built-in functions."** Mid: "I'd replace the Python function with `F.when`, `F.col` arithmetic or string functions, so Catalyst can optimise it." Senior: "The UDF hides logic from the optimiser and pays per-row serialisation. I'd first check whether the logic can be a column expression. If it cannot, I'd use a Pandas UDF and test the output for null handling and type coercion, because those differences are where the rewrite breaks."

**"Why is my Python UDF slow?"** Mid: "It runs row by row in a Python process, so every row crosses the JVM boundary." Senior: "Three costs: serialisation of each row, the Python worker process per executor, and the fact that Catalyst cannot push predicates through it. I'd look at the plan for `BatchEvalPython`, check whether the UDF runs on skewed keys, and replace it with built-ins or an Arrow-based Pandas UDF."

> **🎤 Say this in the interview:** "I write transformations as column expressions so Spark can plan the whole job. I use a UDF only when no built-in exists, and then a vectorised one. I read the plan before I optimise, and I test the logic on small local DataFrames."

## ⚠️ Common mistakes

> **⚠️ Watch out:** A UDF without a return type, or with the wrong one, returns `null` silently for mismatched values. Declare `returnType` explicitly.

> **⚠️ Watch out:** Calling `collect()` or `toPandas()` on a big DataFrame to "just look at it". Use `show()` or `limit()`.

- Writing a Python `for` loop over `collect()` results to do work that a join or aggregation would do in the cluster.
- Expecting `explode` to keep empty arrays; use `explode_outer`.
- Using a window without `partitionBy` on a large table.
- Forgetting that transformations are lazy, so a timing measured without an action measures nothing.
- Joining without thinking about key skew or broadcasting the small side (see [[broadcast-join]]).
- Testing only on the full dataset; test on tiny DataFrames with known expected rows.

## ✅ Check yourself (quiz)

1. Which call actually starts execution of the job?
   - A) `df.filter(...)`
   - B) `df.withColumn(...)`
   - C) `df.count()`
   - D) `df.groupBy(...)`

<details><summary>Answer</summary>

**C** — `count()` is an action. The others are transformations that only extend the plan.

</details>

2. Why is `F.col("salary") * 1.09` faster than a Python UDF doing the same arithmetic?
   - A) The UDF cannot return a double
   - B) The expression runs inside the JVM with no per-row Python round trip, and Catalyst can optimise it
   - C) Expressions are cached on the driver
   - D) UDFs are always executed on the driver

<details><summary>Answer</summary>

**B** — a built-in expression avoids serialising each row to a Python worker, and Catalyst sees its semantics.

</details>

3. A row has an empty array in the `items` column. Which function keeps that row with a null element?
   - A) `explode`
   - B) `explode_outer`
   - C) `size`
   - D) `struct`

<details><summary>Answer</summary>

**B** — `explode` drops rows with null or empty arrays. `explode_outer` keeps them with a null element.

</details>

4. **(Senior)** A Pandas UDF job fails on the executors with memory errors even though the JVM heap looks fine. What is the likely cause and what do you check?

   - A) The window is missing `partitionBy`
   - B) Python worker memory outside the JVM heap, for example a large batch in the worker; check executor memory overhead settings, the batch size and the plan for skewed keys
   - C) `explode` produced too many rows
   - D) The driver collected the result

<details><summary>Answer</summary>

**B** — Python workers use memory outside the JVM heap. Check the overhead configuration, the size of each batch, and whether one skewed key produces an oversized group.

</details>

5. **(Senior)** You need the second highest distinct salary per department. Why use `dense_rank` rather than `row_number`?

   - A) `row_number` cannot be used in a window
   - B) `row_number` gives tied rows different numbers, so a tie at the top would hide the true second distinct salary; `dense_rank` gives ties the same rank without gaps
   - C) `dense_rank` is faster on every cluster
   - D) `row_number` returns strings

<details><summary>Answer</summary>

**B** — with `row_number`, two rows tied at the top get 1 and 2, so the "second" row is a duplicate of the top salary. `dense_rank` assigns the next distinct value rank 2.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Transformation vs action | Transformation extends the lazy plan; action runs it |
| Narrow vs wide transformation | Narrow stays within a partition; wide shuffles data by key |
| Why built-ins beat UDFs | No per-row Python round trip; Catalyst can optimise |
| Pandas UDF | Batch-wise via Arrow; cheaper than row UDFs, still not free |
| `explode` vs `explode_outer` | `explode` drops null/empty arrays; `explode_outer` keeps them |
| Window without `partitionBy` | One partition for all rows: a bottleneck |
| Why `collect()` is dangerous | Pulls all rows into driver memory |
| Testing Spark jobs | Pure functions on DataFrames, tested on tiny local sessions |

## 🔗 Go further

- **spark-catalyst** explains how the optimiser rewrites your DataFrame plan.
- **python-dataframes** compares pandas and Spark DataFrames for the same ideas.
- **spark-joins-skew** and **spark-shuffle** cover what the wide transformations cost on a cluster.
- **spark-writes** covers writing results to storage instead of collecting them to the driver.
- **spark-ui-debugging** shows how to find the slow stage and task for a UDF-heavy job.
