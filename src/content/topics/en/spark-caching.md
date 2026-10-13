---
order: 78
tid: "spark-caching"
track: "7"
section: null
num: "7.9"
title: "Caching, Persistence & Checkpointing"
type: "concept"
level: "intermediate"
priority: "P1"
est_hours: 1
summary: "When to cache and when not to, storage levels, and checkpointing to truncate long lineage."
covers: []
interview_qs: []
extras: []
prerequisites: ["spark-architecture"]
related: ["spark-memory-tuning", "spark-aqe", "spark-ui-debugging", "spark-shuffle", "spark-structured-streaming"]
---

## 🎯 TL;DR

> **Caching** keeps the result of a computation in executor memory or on disk so that later actions can reuse it instead of recomputing it from the source. **Checkpointing** writes the result to reliable storage and **cuts the lineage**, so a failure does not replay the whole history.

- `cache()` and `persist()` are **lazy**: nothing is stored until an action runs on the data.
- The default storage level is `MEMORY_ONLY` for RDDs and `MEMORY_AND_DISK` for DataFrames and Datasets. Check the docs for your version before quoting it.
- Cache only data that is **reused** by two or more actions or branches, and that fits in memory or spills acceptably to disk.
- Checkpoint when the **lineage** is very long (iterative loops, many chained transformations) or when recomputation is expensive and you need recovery that does not depend on executors.
- Cached data is not a source of truth: if the underlying files change, you must refresh or re-read.

## 🌱 Start simple (Junior)

> **💡 Analogy:** You are cooking a big dinner. Chopping the onions takes 20 minutes. If you need onions in three different dishes, you chop them once, put them in a bowl on the counter, and use the bowl three times. The bowl is the cache. If the bowl gets full, some onions go into the fridge (disk). Checkpointing is taking a photo of the prepared onions and writing the recipe as "start from this photo", so that if the dish fails you do not need to chop from scratch.

In Spark, a DataFrame is a **recipe**, not data. Every action (`count`, `write`, `collect`) runs the recipe from the start, including reading the files again. If two actions use the same expensive intermediate result, Spark computes it twice unless you cache it.

```python
from pyspark import StorageLevel

events = spark.read.parquet("/data/events").filter("country = 'IR'")

events.persist(StorageLevel.MEMORY_AND_DISK)   # marks it, does not compute yet
events.count()                                  # first action fills the cache

daily = events.groupBy("day").count()           # reuses the cached rows
users = events.select("user_id").distinct().count()

events.unpersist()                              # free the memory when done
```

Without the `persist` line, `count()` reads the Parquet files, and the two later queries read them again. With it, the files are read once. On a large dataset that can be the biggest saving in the job.

The shortcut `events.cache()` means the same thing as `persist()` with the default level.

## 🔧 How it really works (Mid)

**Lazy marking.** `persist()` only records the request in the plan. The first action computes each partition, stores it in the block manager on the executor that computed it, and later actions read those blocks. A partition that was never computed is computed when first needed.

**Storage levels.** The level answers three questions: memory or disk, serialized or deserialized, and how many replicas.

| Level | Where | Notes |
|---|---|---|
| `MEMORY_ONLY` | Heap, deserialized objects | Partitions that do not fit are recomputed when needed |
| `MEMORY_AND_DISK` | Heap, spill to disk | Usually the safe default for DataFrames |
| `MEMORY_ONLY_SER` / `MEMORY_AND_DISK_SER` | Heap, serialized bytes | Less memory, more CPU to read (Java and Scala APIs) |
| `DISK_ONLY` | Local disk | Useful when the data is huge and recomputation is more expensive than I/O |
| `*_2` variants | Two replicas | Faster recovery after an executor loss, twice the space |
| `OFF_HEAP` | Off-heap memory | Requires off-heap memory to be configured |

For DataFrames in the Python API, the default is `MEMORY_AND_DISK`. For RDDs, `cache()` uses `MEMORY_ONLY`. Check the API reference for the version you run, because defaults have differed between RDD, DataFrame and streaming APIs.

**Memory management.** Cached blocks live in the storage part of Spark's unified memory. When execution (joins, sorts, aggregations) needs space, Spark can evict cached blocks, usually least recently used first. An evicted block is recomputed (or re-read from disk if the level allows it). So caching can make other operators slower by taking memory, and a cache that is too big causes eviction churn.

**Using the cache.** After a DataFrame is cached, a later query whose logical plan matches the cached plan reads the cached data instead of the source. The Spark UI's **Storage** tab shows the cached DataFrames, their size in memory and on disk, and the fraction of partitions cached. A cache that shows 40% of partitions cached means the rest will be recomputed.

**SQL caching.** `CACHE TABLE t` and `CACHE SELECT ...` in Spark SQL store the result in Spark's in-memory columnar format, which is compressed. Use `UNCACHE TABLE` to release it. The SQL form is convenient for interactive work; the DataFrame form gives you explicit control of the storage level.

**Checkpointing.** A checkpoint writes the data of a DataFrame or RDD to a checkpoint directory on reliable storage (HDFS, S3, ABFS and similar) and then replaces its lineage with a read from that directory:

```python
spark.sparkContext.setCheckpointDir("hdfs:///tmp/spark-checkpoints")

df = df.checkpoint()  # eager by default: runs now and truncates the lineage
```

`localCheckpoint()` writes to executor storage instead. It is faster, but the data is lost with the executor, so it is not reliable for long lineages. Use it only when you know the executor loss risk is acceptable.

## 🧠 Senior depth & trade-offs

- **When caching helps.** Reuse is the trigger: a shared intermediate used by several actions, an iterative algorithm that reads the same training set many times, or an expensive join result used by several outputs. Caching a DataFrame that is used once adds write cost and memory pressure with no benefit.
- **When caching hurts.** If the cached data is larger than the memory you can spare, Spark spills or evicts and you pay both costs. Caching before a filter that keeps 1% of the data stores 100 times more than needed. Put the cache after the selective filter and projection, and keep only the columns you use.
- **Caching a shuffle output is not the same as caching the result.** A shuffle output is already materialized in the shuffle files, so Spark reuses it without a cache when the same exchange appears again in the same job. Caching adds value mostly for the transformation results that come before a shuffle or after expensive narrow work.
- **Lineage and failures.** Spark recovers lost partitions by recomputing them from lineage. For a cached partition that was lost, it recomputes it from the source, not from a replica (unless you used `_2`). A long chain of transformations makes recovery slow, and in iterative code the lineage grows each iteration. Checkpointing every N iterations bounds the recovery cost and also the size of the plan the driver must analyze.
- **Checkpoints cost a write.** A checkpoint is a full job plus a write to reliable storage, and it is a barrier. Place checkpoints at natural points (after an expensive join, every 10 iterations of a loop), not after every step.
- **Stale data.** The cache is a copy. If another pipeline overwrites the source files while your job runs, the cache still shows the old data. For file sources, refresh with `REFRESH TABLE` or by re-reading. In streaming, do not treat a cache as state; use proper state management (see [[state-store]]).
- **AQE and caching.** A cached intermediate has real statistics once materialized, which helps AQE choose better joins for later stages. Caching a small, filtered side of a join can turn a sort-merge join into a broadcast join in later queries. See [[spark-aqe]].
- **Measure.** Check the Storage tab, the eviction messages in the executor logs, GC time in the Executors tab, and the number of tasks in the stages that reuse the cache. A cache is useful only if the reuse stage is faster than recomputation would be.

## 🧪 Try it

Run these in PySpark 3.x or `spark-sql`. Use a generated dataset (for example `spark.range(10_000_000)` with a few computed columns) so you can compare times.

1. Build a DataFrame with an expensive transformation (a UDF or a heavy `withColumn` chain). Run three `count()` actions with and without `cache()`. Compare the time of the second and third actions in the Spark UI.
2. Persist with `StorageLevel.DISK_ONLY`, run an action, and check the Storage tab. Then `unpersist()` and confirm the entry disappears.
3. Build an iterative loop that adds one `withColumn` per iteration for 50 iterations. Measure the time of the last `count()` with no checkpoint, then with `df.checkpoint()` every 10 iterations.

<details><summary>Show solutions</summary>

**1.** Without cache, every action recomputes the chain, so each `count()` takes roughly the same time. With cache, the first action is slower (it computes and stores) and the next two are much faster, because they read from the block storage. Check that the Storage tab shows the cached size and that the stage reads from memory.

**2.** After `persist(StorageLevel.DISK_ONLY)` and one action, the Storage tab shows the DataFrame with a disk size and no memory size. After `unpersist()`, the entry is removed when Spark cleans up the blocks, which may take a moment.

**3.** Without checkpoints, the plan for iteration 50 has a long lineage and planning time grows with it. With checkpoints every 10 iterations, the lineage is truncated, so planning is shorter. The checkpoint itself costs a write, so the total time may not drop much on a short loop; the benefit shows up with longer chains and on recovery after failures.

</details>

## 💬 Interview corner

> **🎤 Say this in the interview:** "Caching stores a computed DataFrame in executor memory or disk, so later actions do not recompute it, but it is lazy and it competes with execution memory. I cache only data that is reused, and I put the cache after selective filters. Checkpointing writes to reliable storage and truncates lineage, which I use in long iterative jobs to keep recovery and planning cheap."

**Typical questions**

- *What is the difference between cache and checkpoint?* Cache keeps a copy for reuse and keeps lineage; checkpoint writes to reliable storage and removes lineage.
- *Why does `cache()` seem to do nothing?* It is lazy. Run an action to materialize it, and check the Storage tab.
- *When would you use `DISK_ONLY`?* When the data is too large for memory and reading it back from local disk is cheaper than recomputing it from a slow source.
- *Why can a cached job still be slow?* The cache was too big, was evicted by execution memory, or was placed before a filter that keeps only a small part of the data.

**Mid-level answer:** explains lazy evaluation, the default levels, when to unpersist, and how to confirm the cache in the UI.

**Senior answer:** adds eviction behavior under unified memory, checkpoint placement in iterative loops, the trade-off between `checkpoint` and `localCheckpoint`, the stale-data risk, and the interaction with AQE statistics.

## ⚠️ Common mistakes

- **Caching everything "to be safe".** Each cache uses memory and can slow down the rest of the job. Cache only what is reused.
- **Forgetting to unpersist.** Long-running notebooks and jobs keep cached data that no longer serves a purpose.
- **Expecting cache to survive a restart.** It lives in executor memory and disk of the current application. A new application starts with an empty cache.
- **Caching after a non-selective step and before a selective filter.** Cache after the filter and projection that you actually use.
- **Using `localCheckpoint` for a long pipeline that must survive executor loss.** It is not on reliable storage. Use `checkpoint()` with a configured directory.
- **Assuming the cached data is refreshed when files change.** It is not. Re-read or refresh the table.

## ✅ Check yourself (quiz)

1. What happens when you call `df.cache()` followed by `df.count()`?

   - A) Nothing; `cache()` is an action
   - B) `cache()` marks the DataFrame; the `count()` computes the partitions and stores them, so later actions reuse them
   - C) The data is written to HDFS as a checkpoint
   - D) The cache is only created in the driver's memory

<details><summary>Answer</summary>

**B** — `cache()` is lazy and only records the request. The first action materializes the partitions into executor storage. Checkpointing is different, and the cache is not on the driver.

</details>

2. A cached DataFrame shows 40% of its partitions cached in the Storage tab. What does that mean for the next action?

   - A) The job will fail
   - B) The uncached 60% are recomputed from lineage when needed, so the action is slower than fully cached
   - C) Spark has replicated the cache to disk automatically
   - D) The cache is invalid and must be dropped

<details><summary>Answer</summary>

**B** — the partitions that are not in storage are computed from the source, through the lineage. The result is correct but slower.

</details>

3. **(Mid)** When is `checkpoint()` a better choice than `cache()`?

   - A) Always, because it is faster
   - B) When the lineage is long (for example an iterative loop) or when you need recovery that does not depend on executor memory; checkpoint writes to reliable storage and truncates the lineage
   - C) When the data fits in memory and is reused once
   - D) When you need the data only in the driver

<details><summary>Answer</summary>

**B** — checkpoint writes the data to reliable storage and removes the dependency on the earlier lineage. Cache is faster to set up but does not shorten the lineage and does not survive executor loss.

</details>

4. **(Senior)** A job caches a 200 GB DataFrame with `MEMORY_AND_DISK`, and the later joins become slower than without the cache. What is the most likely cause?

   - A) Caching always makes joins slower
   - B) The cache takes storage memory that execution needs, so joins spill more and cached blocks are evicted and recomputed; cache a smaller, filtered and projected version
   - C) `MEMORY_AND_DISK` is not supported for joins
   - D) The job should use `MEMORY_ONLY` to be faster

<details><summary>Answer</summary>

**B** — the cached data competes with execution memory under unified memory management. Reducing the cached size (filter, select only needed columns, or cache after a shuffle that reduces data) fixes the pressure. Changing to `MEMORY_ONLY` would make eviction and recomputation more likely.

</details>

5. **(Senior)** Why can a checkpoint reduce the time of planning an iterative job, not only the time of recovery?

   - A) Checkpoints make the driver skip analysis
   - B) The plan the driver analyzes for each action depends on the lineage; a checkpoint replaces the long chain with a read from stored data, so the plan is short
   - C) Checkpoints compress the query plan
   - D) Checkpoints run on the driver only

<details><summary>Answer</summary>

**B** — the lineage determines the logical plan that grows with each iteration. Truncating it bounds the planning work and the recovery work. This is why long loops checkpoint every few iterations.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Is `cache()` lazy? | Yes. It marks the data; an action materializes it |
| Default storage level for DataFrames? | `MEMORY_AND_DISK` (check your version); RDD `cache()` is `MEMORY_ONLY` |
| What does `_2` in a storage level mean? | Two replicas of each cached partition, for faster recovery at twice the space |
| What is evicted first under memory pressure? | Cached blocks, usually least recently used, to make room for execution |
| What does checkpoint do to lineage? | Writes data to reliable storage and truncates the lineage |
| Cache vs checkpoint in one line? | Cache = fast reuse, keeps lineage; checkpoint = reliable recovery, cuts lineage |
| Why is a cache not a source of truth? | It is a copy; if the source changes, the cache is stale until refreshed |
| Where should a cache go? | After the selective filter and projection, before the reuse points |

## 🔗 Go further

- **spark-memory-tuning** explains the unified memory model that decides how much cache fits.
- **spark-aqe** shows how materialized intermediates give AQE real statistics for later joins.
- **spark-ui-debugging** covers the Storage tab and how to read cache hits and misses in stages.
- **spark-structured-streaming** explains why streaming state is not handled by ordinary caching.
- Glossary: [[checkpoint]], [[lineage]], [[state-store]].
