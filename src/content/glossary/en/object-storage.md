---
term: "Object storage"
aliases: ["s3", "gcs", "azure blob", "adls", "minio", "blob storage"]
category: infra
short: "Scalable, durable, flat key-value storage for immutable objects (S3, GCS, ADLS), the foundation of data lakes with decoupled compute."
related: ["data-lake", "lakehouse", "parquet"]
---
## In plain words (Junior)
Object storage is a giant, cheap storage locker room: you hand in a whole file with a label (key) and later fetch it by that label. You can't edit one page inside the file; you replace the entire file. It can hold unimaginable amounts of data, and many machines can read at once.

## How it works (Mid)
A flat namespace of buckets and keys with immutable objects, accessed over HTTP. It offers very high durability and scale at low cost, with throughput from parallel requests and ranged GETs (which is how Parquet readers fetch footers and column chunks). Storage is decoupled from compute, so clusters can be created or destroyed independently.

```
s3://bucket/warehouse/events/dt=2024-05-01/part-0001.parquet     <- "folders" are just key prefixes

Operations:  PUT / GET (range reads!) / DELETE / LIST (prefix, paginated, slow at scale)
No in-place update, no append (traditionally), NO atomic rename of a "directory"
Durability ~11 nines via replication/erasure coding; cost: storage + requests + egress
```

Differences from file systems: no real directories (prefixes), no cheap rename (copy plus delete, not atomic), `LIST` is slow and paginated, per-request latency is tens of ms, and costs include requests and egress. Modern S3 offers strong read-after-write consistency. Table formats exist largely to compensate: atomic commits via a metadata pointer instead of renames, and manifest files instead of listing.

## Senior angle & interview tips
Performance practice: larger files (hundreds of MB), fewer files per query, parallel reads, ranged reads with column pruning, avoid hot prefixes in older systems, co-locate compute in the same region to avoid egress and latency. Operational features: lifecycle rules (tiering to cold storage, expiry), versioning, replication, encryption, access via IAM and short-lived credentials. Cost traps: millions of tiny files (request costs), cross-region transfer, forgotten buckets and versions. Compare with HDFS: data locality and POSIX-like semantics vs elasticity and cost; most modern platforms moved to object storage.

Interview question: "Why can't you safely do `rename` commits on S3, and what replaced them?" Model answer: S3 has no atomic directory rename (it is copy + delete), so output committers could expose partial results; table formats like Iceberg/Delta write immutable files and commit by atomically swapping a metadata pointer.
