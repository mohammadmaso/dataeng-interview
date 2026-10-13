---
term: "Small files problem"
aliases: ["small file problem", "small files", "tiny files"]
category: lakehouse
short: "Having huge numbers of tiny files in a data lake, which overwhelms metadata and listing and slows every read."
related: ["compaction", "partition", "parquet", "iceberg"]
---
## In plain words (Junior)
Imagine a library where every page of every book is stored as its own separate piece of paper in a different drawer. To read one book you must open hundreds of drawers. Data lakes have the same trouble when data is chopped into millions of tiny files: opening and tracking them costs far more than reading the data.

## How it works (Mid)
Each file brings fixed overhead: a list/open request on object storage, a metadata entry (HDFS NameNode memory, table-format manifests), a footer read for Parquet, and often a separate task in the engine. When files are much smaller than the efficient read size (tens to hundreds of MB), overhead dominates and throughput collapses.

```
-- Symptoms
-- * 1 TB table made of 5 million files of ~200 KB
-- * Spark job spends minutes listing S3 and launching tiny tasks
-- * NameNode memory pressure on HDFS (each file/block = metadata object)

-- Causes: streaming micro-batches, over-partitioning, high spark.sql.shuffle.partitions
-- Fix: compact to ~128 MB-1 GB, fewer partitions, bigger commit interval
df.repartition(8).write.partitionBy("dt").parquet(path)   # control files per partition
```

Causes: frequent streaming commits, very granular partitioning (e.g. by hour and user), too many shuffle partitions writing one file each, and many incremental loads. Fixes: write fewer, larger files (coalesce/repartition before write, adaptive shuffle partition coalescing), compaction jobs (`OPTIMIZE`, Iceberg `rewrite_data_files`), partition coarser, longer trigger intervals, and auto-compaction in managed table formats.

## Senior angle & interview tips
Small files also bloat metadata (manifests and snapshots) and slow query planning, not only reads. Compaction competes with writers and itself leaves old files until snapshot expiry, so schedule it and clean up. The opposite extreme, giant files, hurts parallelism and makes updates expensive; target a range, not a number. For streaming ingestion, choose the commit interval as a latency vs file-size trade-off, or land in a buffer (Kafka, ClickHouse parts) and compact asynchronously. Monitor file count and average file size per partition as a table-health metric.

Interview question: "A table got slow after moving to streaming ingestion; why?" Model answer: frequent micro-batch commits created many small files, inflating listing, metadata and task counts; I'd compact to target size, tune the trigger interval, and add scheduled maintenance plus a file-size alert.
