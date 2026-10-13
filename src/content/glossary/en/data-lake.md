---
term: "Data lake"
aliases: ["data lakes", "datalake"]
category: lakehouse
short: "A central repository on cheap object storage that holds raw data in its original format, structured or not, applying schema on read."
related: ["lakehouse", "data-warehouse", "object-storage"]
---
## In plain words (Junior)
A data lake is a huge storage room where you can drop anything: tables, logs, JSON, images, as it arrives, without sorting it first. It's cheap and flexible, and you decide how to organize and read the data later. Without care, though, it can turn into a messy swamp.

## How it works (Mid)
Data lakes sit on object storage (S3, GCS, ADLS, HDFS historically), storing files in open formats (Parquet, ORC, Avro, JSON, CSV). Schema is applied on read, in contrast to a warehouse's schema-on-write. Storage and compute are decoupled; various engines (Spark, Trino, Hive, Presto) query the same files. Typical zones: raw/landing, cleaned/curated, and consumption-ready.

```
s3://company-lake/
  raw/orders/dt=2024-05-01/part-0001.json.gz      # as received
  curated/orders/dt=2024-05-01/part-0001.parquet  # cleaned, columnar
  ml/features/...                                 # images, logs, text also live here
-- schema-on-read: structure is applied when you query
CREATE EXTERNAL TABLE raw_orders (...) LOCATION 's3://company-lake/raw/orders/';
```

Strengths: low cost per TB, any data type, flexibility for data science and ML, no vendor lock-in on file formats. Weaknesses of plain lakes: no ACID (partial writes visible), no consistent updates/deletes, schema drift, poor governance and discoverability, and performance depends on file layout.

## Senior angle & interview tips
Those weaknesses are why table formats (Iceberg, Delta, Hudi) and the lakehouse emerged: they add transactions, schema enforcement, and metadata-based pruning on top of the same files. Lakes become swamps without catalogs, ownership, naming, lifecycle rules and quality checks. Governance concerns: PII in raw zones, access control at the table/column level, retention and deletion for GDPR (hard in immutable files). Cost traps: egress, listing requests on many files, duplicating data across zones, and forgotten data with no owner.

Interview question: "Data lake vs data warehouse?" Model answer: a lake stores raw and varied data cheaply with schema-on-read and open files, flexible but needing governance; a warehouse stores modeled data with schema-on-write and tuned performance; lakehouse formats now blend them, giving ACID and SQL over lake storage.
