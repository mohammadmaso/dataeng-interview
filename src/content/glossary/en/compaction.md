---
term: "Compaction"
aliases: ["compact", "file compaction", "merge small files"]
category: lakehouse
short: "Merging many small files or segments into fewer larger ones to speed up reads and reclaim space."
related: ["small-files-problem", "iceberg", "lsm-tree", "parquet"]
---
## In plain words (Junior)
Think of a drawer stuffed with hundreds of tiny paper notes. Finding anything takes forever. So you sit down and copy the notes into a few organized notebooks. Compaction does this for data files: many small pieces are merged into a few big ones so reading is faster and the system has less to keep track of.

## How it works (Mid)
Streaming and frequent micro-batch writes create many small files (or LSM segments, or ClickHouse parts). Each file adds open/list cost, metadata, and a task at read time. Compaction rewrites them into files near a target size (commonly 128 MB to 1 GB for Parquet on object storage).

```
-- Iceberg (Spark)
CALL catalog.system.rewrite_data_files(
  table => 'db.events',
  options => map('target-file-size-bytes', '536870912'));

-- Delta Lake
OPTIMIZE events WHERE dt >= '2024-05-01';

-- ClickHouse: force merge of parts
OPTIMIZE TABLE events FINAL;
```

In LSM-based stores (Cassandra, RocksDB, ClickHouse MergeTree) compaction is a built-in background process that merges sorted runs, drops overwritten values and tombstones, and bounds read amplification. In table formats (Iceberg, Delta, Hudi) it is a maintenance job you schedule, often combined with sorting or clustering and snapshot expiry.

## Senior angle & interview tips
Compaction trades write amplification and compute cost for read speed. Risks: competing with writers (Iceberg uses optimistic concurrency, so a long compaction can conflict and retry), rewriting huge partitions needlessly (compact only recent/dirty partitions), and old files lingering until snapshot expiry and orphan-file cleanup. In LSM systems, compaction debt causes latency spikes and disk-space amplification; pick the strategy (size-tiered vs leveled) by workload.

Interview question: "Your streaming job writes to Iceberg every minute and queries got slower; why?" Model answer: small-files problem; I'd schedule compaction to a target file size on hot partitions, expire snapshots, and consider larger commit intervals.
