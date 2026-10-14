---
term: "Partition pruning"
aliases: ["partition elimination","pruning"]
category: batch
short: "Skipping whole partitions that cannot match a query filter."
related: ["partition","predicate-pushdown","small-files-problem"]
---
## In plain words (Junior)
If you only want March invoices and they are filed in monthly folders, you open just the March folder.

## How it works (Mid)
When data is laid out by partition columns (for example dt=2026-03-01), the planner uses filters on those columns to read only matching partitions. Static pruning happens at plan time; dynamic pruning uses runtime values (from the other side of a join).

## Senior angle & interview tips
Pruning fails if the filter wraps the column in a function or compares to a differently typed value, so write `dt = date` and not `date(ts) = ...` unless the engine supports it. Over-partitioning creates tiny files and kills the benefit. Hidden partitioning (Iceberg) lets queries on the source column prune without knowing the layout.

Interview question: "Why did the query scan the whole table despite a partition?" Answer: the predicate was not on the partition column or was wrapped in a function.
