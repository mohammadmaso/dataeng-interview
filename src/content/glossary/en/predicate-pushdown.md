---
term: "Predicate pushdown"
aliases: ["predicate push-down", "filter pushdown", "projection pushdown", "data skipping"]
category: batch
short: "Pushing filters (and column selection) down to the storage layer so less data is read and processed."
related: ["parquet", "partition", "columnar-storage"]
---
## In plain words (Junior)
If you ask a librarian for "red books published in 2020", it is much faster if the librarian checks only the 2020 shelf and the red labels, instead of bringing you every book for you to filter yourself. Predicate pushdown means the filter is applied as close to the data as possible, so most of the data is never read at all.

## How it works (Mid)
Instead of reading everything and filtering in the engine, the optimizer pushes the `WHERE` conditions down to the scan. At the storage layer this enables: partition pruning (skip whole directories), file skipping using table-format metadata (Iceberg/Delta min/max per file), row-group/page skipping using Parquet statistics, and Bloom filter checks. Projection pushdown similarly reads only referenced columns.

```
EXPLAIN SELECT amount FROM events
WHERE dt = '2024-05-01' AND country = 'IR';

-- look for in the plan:
--  PartitionFilters: [dt = 2024-05-01]          (partition pruning)
--  PushedFilters:    [country = IR]             (pushed to Parquet reader)
--  ReadSchema:       struct<amount, country>    (projection pushdown)
```

It works only if the predicate is pushable: simple comparisons on raw columns. Wrapping the column in a function (`WHERE DATE(ts) = ...`, `CAST`) or using a UDF often defeats it, and effectiveness depends on data layout: if the filter column is randomly distributed, min/max stats cannot skip anything.

## Senior angle & interview tips
Make pushdown effective by sorting or clustering data on common filter columns (Z-order, sorted keys, ClickHouse ORDER BY), keeping partition columns in filters, and choosing types that compare cheaply. Pushdown also applies to JDBC sources (the database does the filtering), and across joins (dynamic partition pruning and runtime filters push build-side values to the probe-side scan). Debug by reading the plan for pushed filters and the bytes actually read, not by trusting the query text.

Interview question: "Why did my query scan the whole table despite a date filter?" Model answer: likely the filter wraps the partition column in a function or has a type mismatch, the table is not partitioned or clustered on it, or stats are missing; check the plan for partition filters and fix the predicate to compare the raw column.
