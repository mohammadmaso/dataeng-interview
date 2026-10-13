---
term: "Materialized view"
aliases: ["materialized views", "mv", "rollup table", "pre-aggregation", "summary table"]
category: sql
short: "A view whose query result is stored physically and refreshed, trading storage and maintenance for much faster reads."
related: ["olap", "incremental-processing", "cardinality"]
---
## In plain words (Junior)
A normal view is a saved question that's answered from scratch every time. A materialized view saves the answer too, like a pre-calculated summary table. Reading it is instant, but when the underlying data changes the saved answer has to be updated.

## How it works (Mid)
A regular view is just a stored query. A materialized view stores the result, so dashboards read a small pre-aggregated table instead of scanning billions of rows. Refresh strategies: full refresh (recompute on schedule or on demand), incremental (apply only the changes), and streaming/real-time maintenance on insert (ClickHouse MVs act as insert triggers into a target table; StarRocks/Snowflake/BigQuery maintain them automatically with limits on supported SQL).

```
-- ClickHouse: incrementally maintained on insert
CREATE MATERIALIZED VIEW daily_revenue_mv
ENGINE = SummingMergeTree ORDER BY (dt, country) AS
SELECT toDate(ts) AS dt, country, sum(amount) AS revenue
FROM orders GROUP BY dt, country;

-- Postgres: full recompute on demand
CREATE MATERIALIZED VIEW daily_revenue AS SELECT ...;
REFRESH MATERIALIZED VIEW CONCURRENTLY daily_revenue;
```

Query engines can also rewrite queries to use a matching MV automatically (StarRocks, BigQuery, Oracle). The cost is storage, refresh compute and staleness between refreshes.

## Senior angle & interview tips
Important details: not every aggregate is incrementally maintainable (distinct counts need sketches, medians don't compose); deletes and updates in the base table complicate incremental maintenance (ClickHouse MVs see inserts only, so corrections need reprocessing); consistency vs freshness trade-offs; MVs on large joins can be costly to refresh; dependency chains of MVs create operational complexity. In ELT, dbt incremental models and pre-aggregated rollup tables serve the same purpose with explicit control over logic and backfills.

Interview question: "Dashboard on 10B rows is slow; options?" Model answer: pre-aggregate with a materialized view or rollup table at the grain the dashboard needs, maintained incrementally, plus partitioning and sort-key tuning; I'd keep raw data for ad hoc queries and accept bounded staleness.
