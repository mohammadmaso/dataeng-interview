---
term: "Cardinality"
aliases: ["high cardinality", "low cardinality", "selectivity", "distinct count", "one-to-many"]
category: sql
short: "The number of distinct values in a column (or the row-count relationship between tables); it drives indexing, partitioning, encoding and plan choices."
related: ["database-index", "partition", "cost-based-optimizer"]
---
## In plain words (Junior)
In a class of 30 students, "first name" has many different values (high cardinality) while "grade level" might have only one or two (low cardinality). Knowing how varied a column is helps you decide how to store it, search it, and split it.

## How it works (Mid)
Two meanings: column cardinality (distinct values) and relationship cardinality (1:1, 1:N, N:M between tables). Column cardinality guides design: high-cardinality columns suit indexes and bloom filters for point lookups but are bad partition keys (millions of partitions) and blow up group-by state; low-cardinality columns suit dictionary encoding, bitmap indexes, partitioning (in moderation) and `LowCardinality` types in ClickHouse.

```
SELECT count(*) AS rows,
       count(DISTINCT country)  AS country_card,   -- ~200   : LOW  -> good partition/dictionary
       count(DISTINCT user_id)  AS user_card,      -- ~50 M  : HIGH -> bad partition key
       count(DISTINCT status)   AS status_card     -- 4      : very low
FROM orders;
-- selectivity = distinct / rows  (user_id ~ 1.0 selective; status ~ 0.0 not selective)
-- Relationship cardinality: customers 1 --- N orders N --- M products (via order_items)
```

The optimizer uses estimated distinct counts and selectivity to choose join order, join type and whether an index helps. Relationship cardinality determines join behavior: joining a 1:N relation multiplies rows, which is the root of fan-out bugs and double-counted measures.

## Senior angle & interview tips
Know the failure modes: metrics systems and logs with high-cardinality labels (user ids as tags) explode storage and memory (Prometheus, time-series DBs); `COUNT(DISTINCT)` on high-cardinality columns is expensive and often replaced by HyperLogLog; group-by on high cardinality keys needs large state in streaming; low-cardinality skew (80% one value) still creates data skew. Cardinality estimates are heuristic and degrade with correlated columns; wrong estimates lead to bad plans. In modeling, declare relationship cardinality explicitly and test it (unique keys, relationships tests) to prevent silent fan-out.

Interview question: "Which column would you partition a 10B-row events table on?" Model answer: a low-to-moderate cardinality column that matches common filters, typically event date (optionally hour), not user_id; high-cardinality columns go into sort keys, bloom filters or bucketing instead.
