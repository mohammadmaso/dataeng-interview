---
term: "GROUP BY and aggregation"
aliases: ["group by","aggregate function","aggregation","having"]
category: sql
short: "Collapsing rows into groups and computing summaries like COUNT, SUM and AVG."
related: ["window-function","cardinality","cte"]
---
## In plain words (Junior)
Sorting receipts into piles by day and adding up each pile to get a daily total.

## How it works (Mid)
GROUP BY partitions rows by key columns and aggregate functions (COUNT, SUM, AVG, MIN, MAX, approx_count_distinct) reduce each group to one row. HAVING filters groups after aggregation, WHERE before. Distributed engines do a partial aggregate per node and then shuffle by key to combine.

## Senior angle & interview tips
COUNT(DISTINCT) is costly at scale; use approximate sketches (HyperLogLog) when acceptable. Skewed group keys cause straggler tasks. Every non-aggregated selected column must be in GROUP BY, except where functionally dependent.

Interview question: "WHERE vs HAVING?" Answer: WHERE filters rows before grouping; HAVING filters groups using aggregate results afterwards.
