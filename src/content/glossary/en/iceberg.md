---
term: "Apache Iceberg"
aliases: ["iceberg", "iceberg table", "iceberg tables"]
category: lakehouse
short: "An open table format that adds snapshots, ACID commits, hidden partitioning and schema evolution on top of files in object storage."
related: ["lakehouse", "parquet", "compaction", "schema-evolution"]
---
## In plain words (Junior)
Think of a library catalog that lists exactly which books (files) belong to the collection at each moment. To add or remove books you publish a new catalog edition; readers keep using the edition they opened. Iceberg is that smart catalog on top of data files, so many tools can read and write the same table safely.

## How it works (Mid)
An Iceberg table is metadata plus data files. A commit writes new data files, new manifests and a new metadata file, then atomically swaps the catalog pointer to it (optimistic concurrency, retry on conflict). Each commit is a snapshot, giving isolated reads, time travel and rollback.

```
catalog ──► metadata.json (current snapshot pointer, schema, partition spec)
              └─► manifest list (per snapshot)
                    └─► manifest files (list of data files + column stats)
                          └─► data files (Parquet) + delete files

-- time travel / maintenance
SELECT * FROM events FOR VERSION AS OF 123456789;
CALL system.expire_snapshots('db.events', TIMESTAMP '2024-05-01 00:00:00');
```

Manifests store per-file column stats and partition values, enabling file-level pruning without listing directories. Features: hidden partitioning (transforms like `day(ts)`, `bucket(16, id)` so queries filter on the raw column), partition and schema evolution by column id (renames and reorders are safe), row-level deletes via position/equality delete files, and branches/tags. Maintenance: compaction (`rewrite_data_files`), `expire_snapshots`, orphan file removal.

## Senior angle & interview tips
Strengths: engine-neutral (Spark, Flink, Trino, StarRocks...), safe schema/partition evolution, large-table scalability thanks to metadata pruning. Pitfalls: small files and metadata growth from frequent commits (tune commit interval, compact), equality deletes from streaming upserts that slow reads until compacted, catalog differences affecting atomicity, and commit conflicts under concurrent writers. Choose copy-on-write (rewrite files, fast reads) vs merge-on-read (delete files, fast writes) per workload.

Interview question: "How does Iceberg provide ACID on S3 with no rename?" Model answer: data files are immutable; a commit atomically replaces the pointer to the table metadata in the catalog (compare-and-swap), so readers see either the old or the new snapshot, and conflicting concurrent commits are retried or fail.
