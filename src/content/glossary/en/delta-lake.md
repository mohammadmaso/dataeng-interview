---
term: "Delta Lake (and Hudi)"
aliases: ["delta lake", "delta table", "delta log", "apache hudi", "hudi", "open table formats"]
category: lakehouse
short: "An open table format that adds a transaction log over Parquet files for ACID, time travel, schema enforcement and efficient upserts."
related: ["iceberg", "lakehouse", "time-travel", "compaction"]
---
## In plain words (Junior)
Think of a bank passbook: every deposit and withdrawal is a line in order, and your balance is what you get by reading the lines. Delta Lake keeps such a log of changes to a table's files, so the table's current content (and any past version) can be reconstructed reliably.

## How it works (Mid)
Delta stores data as Parquet and a transaction log (`_delta_log`) of JSON commits listing files added and removed, with periodic checkpoint files. A reader replays the log to learn which files make up a version. Writers commit by atomically creating the next numbered log entry (put-if-absent), using optimistic concurrency with conflict checks. This yields ACID transactions, snapshot isolation, time travel (read version N or timestamp T), and schema enforcement/evolution.

```
my_table/
  _delta_log/
    00000000000000000000.json      <- commit 0: add file a.parquet
    00000000000000000001.json      <- commit 1: add b.parquet, remove a.parquet
    00000000000000000010.checkpoint.parquet   <- periodic snapshot of log state
  part-0001.parquet  part-0002.parquet ...

-- table state = replay of the log;  commit = atomically create the next log file
MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
OPTIMIZE t ZORDER BY (user_id);  VACUUM t RETAIN 168 HOURS;
```

Features: `MERGE`/`UPDATE`/`DELETE`, deletion vectors, Z-ordering/liquid clustering for data skipping, `OPTIMIZE` compaction, `VACUUM` to remove unreferenced files, change data feed, and streaming read/write. Apache Hudi targets upserts and incremental pulls with copy-on-write and merge-on-read tables and a timeline; Iceberg uses a metadata-tree design.

## Senior angle & interview tips
The three formats converge in capability but differ in design: Delta's linear log (simple, fast listing, strongly tied to Spark/Databricks though broadly supported), Iceberg's snapshot/manifest tree (hidden partitioning, engine neutrality, partition evolution), Hudi's record-level indexing and timeline (strong upsert/CDC). Interop layers (UniForm, XTable) can expose one as another. Operational risks: the log growing without checkpoints, `VACUUM` retention shorter than readers/time-travel needs breaks queries, concurrent writers conflict on overlapping files, and small files without OPTIMIZE. Choose by ecosystem, catalog support and workload (streaming upserts vs append-only analytics).

Interview question: "How does Delta get atomic commits on S3?" Model answer: data files are immutable; a commit is a single atomic creation of the next log file (using conditional put or a lock service for stores without it), and readers derive table state from the log, so a transaction is either fully visible or not at all.
