---
term: "Query plan"
aliases: ["execution plan","explain plan","explain"]
category: sql
short: "The step-by-step strategy the engine chooses to execute a query."
related: ["cost-based-optimizer","predicate-pushdown","database-index"]
---
## In plain words (Junior)
A route chosen by a GPS: the same start and destination, but the engine picks which roads (scan, index, join order) are cheapest.

## How it works (Mid)
The optimizer parses SQL into a logical plan, applies rewrites (predicate pushdown, projection pruning, join reordering), then picks a physical plan using statistics. EXPLAIN shows it; EXPLAIN ANALYZE also shows actual rows and timings.

## Senior angle & interview tips
Read plans for: full scans where an index should be used, row-estimate vs actual mismatches (stale stats), shuffle/exchange operators, join order and type, spill to disk. Fix with statistics, indexes, rewriting predicates, or hints as a last resort.

Interview question: "A query suddenly got slow. First step?" Answer: compare EXPLAIN ANALYZE plans, look for changed join type or bad cardinality estimates, refresh statistics.
