---
term: "Cost-based optimizer"
aliases: ["cbo", "query optimizer", "query planner", "table statistics", "explain plan"]
category: sql
short: "A query planner component that uses table statistics to estimate costs and pick the cheapest execution plan, including join order and join type."
related: ["predicate-pushdown", "broadcast-join", "database-index"]
---
## In plain words (Junior)
A GPS compares possible routes and picks the one estimated to be quickest based on known road lengths and traffic. A query optimizer does the same for a SQL query: it considers different ways to run it and chooses the cheapest by estimating how much data each step will handle.

## How it works (Mid)
SQL says what you want, not how. The optimizer enumerates logically equivalent plans: join order, join algorithm (nested loop, hash, sort-merge, broadcast), access path (index vs scan), predicate and projection pushdown, partition pruning, and aggregate placement. It scores them with a cost model fed by statistics (row counts, distinct values, histograms, min/max, null fraction) and selects the lowest estimated cost. Rule-based rewrites (pushdown, constant folding) run before cost-based choices.

```
-- Collect statistics so the optimizer can estimate row counts
ANALYZE TABLE sales COMPUTE STATISTICS FOR ALL COLUMNS;     -- Spark
ANALYZE sales;                                              -- Postgres

EXPLAIN (ANALYZE, BUFFERS) SELECT ...;     -- compare estimated vs actual rows
-- Big gap between "rows=100" estimated and 10,000,000 actual => bad plan risk
```

Cardinality estimation is the weak point: errors compound through joins, especially with correlated columns or skew, and the optimizer may pick a nested loop on what is really millions of rows or a broadcast on a huge table. Engines compensate with adaptive/runtime re-optimization (Spark AQE), runtime filters and sampling.

## Senior angle & interview tips
Most "why is my query slow" stories are bad plans from stale or missing statistics, correlated predicates, UDFs that are opaque to the optimizer, or too many joins for exhaustive search (heuristics kick in). Tools: `EXPLAIN`/`EXPLAIN ANALYZE`, compare estimated and actual rows, refresh stats after big loads, use hints sparingly, add extended statistics or histograms, rewrite to help (avoid functions on filter columns). In lakehouse tables, file-level stats in manifests feed pruning even where no CBO stats exist; Trino/Spark need stats collected explicitly for good join ordering.

Interview question: "The planner picked a bad join strategy; what do you do?" Model answer: compare estimated vs actual row counts in the plan, refresh or add statistics, fix non-sargable predicates, and only then use hints or rewrite the query; for Spark also enable AQE so strategy adapts at runtime.
