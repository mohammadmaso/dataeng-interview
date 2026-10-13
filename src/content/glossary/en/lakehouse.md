---
term: "Lakehouse"
aliases: ["data lakehouse", "lakehouses"]
category: lakehouse
short: "An architecture combining cheap open-format lake storage with warehouse features: ACID tables, schema enforcement and fast SQL."
related: ["data-lake", "data-warehouse", "iceberg", "medallion-architecture"]
---
## In plain words (Junior)
Imagine a huge, cheap warehouse where you can dump anything, now with a good inventory system and rules so that you can find items quickly and trust what is on the shelves. A lakehouse gives a data lake that kind of organization, so one place can serve both big data processing and reliable analytics.

## How it works (Mid)
Traditionally you had a data lake (cheap, flexible, but unreliable "swamp") and a data warehouse (fast and governed, but proprietary and costly), with ETL copying between them. A lakehouse puts a transactional table format (Iceberg, Delta Lake, Hudi) over Parquet files in object storage. The metadata layer gives ACID commits, snapshots and time travel, schema enforcement and evolution, and efficient pruning; storage and compute are decoupled, so multiple engines can use the same tables.

```
Engines:   Spark | Trino | Flink | StarRocks | DuckDB ...
              │         (open table format API)
Table fmt: Iceberg / Delta / Hudi   (snapshots, ACID, schema evolution)
              │
Files:     Parquet / ORC on object storage (S3, GCS, ADLS, MinIO)
Catalog:   Hive Metastore / Glue / Nessie / REST catalog
```

Typical flow: ingest into bronze, refine to silver and gold, serve BI with a SQL engine and ML directly from the same data, with a catalog and governance (access control, lineage) on top.

## Senior angle & interview tips
The real benefits are open formats (less vendor lock-in), one copy of data for BI, ML and streaming, and elastic compute. Trade-offs: you operate maintenance (compaction, snapshot expiry, orphan-file cleanup), concurrency is optimistic (commit conflicts on hot tables), object-store latency means point lookups and sub-second dashboards often need a serving layer (ClickHouse, StarRocks) or caching, and catalog choice matters for multi-engine consistency. "Lakehouse" is partly marketing: be specific about format, catalog and engines.

Interview question: "Lakehouse vs warehouse?" Model answer: a warehouse tightly couples storage and engine with great performance and governance out of the box; a lakehouse uses open files and table formats so any engine can read, at the cost of managing maintenance, catalog and sometimes a faster serving layer for low-latency queries.
