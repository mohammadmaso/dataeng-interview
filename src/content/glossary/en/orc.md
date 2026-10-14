---
term: "ORC"
aliases: ["optimized row columnar","orc format"]
category: storage
short: "A columnar file format from the Hive ecosystem with built-in indexes and ACID support."
related: ["parquet","columnar-storage","predicate-pushdown"]
---
## In plain words (Junior)
A cousin of Parquet: also stores data column by column with compression, and keeps little summary notes per block so queries can skip irrelevant chunks.

## How it works (Mid)
ORC files hold stripes with column data, lightweight indexes (min/max, bloom filters) and a footer with statistics. It supports predicate pushdown and, in Hive, ACID transactional tables via delta files.

## Senior angle & interview tips
Parquet dominates in Spark/lakehouse ecosystems; ORC is natural in Hive/Presto/Trino shops. Both are similar in compression and speed, so choose by ecosystem support and table format (Iceberg works with both).

Interview question: "Parquet or ORC?" Answer: nearly interchangeable technically; pick whichever your engines optimize best, usually Parquet.
