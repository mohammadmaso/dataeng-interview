---
term: "Database view"
aliases: ["sql view","virtual table"]
category: sql
short: "A saved query that behaves like a virtual table."
related: ["materialized-view","cte","data-masking"]
---
## In plain words (Junior)
A saved search you can open like a table; it shows fresh results each time without storing its own copy.

## How it works (Mid)
A view stores the query text, not data; the engine expands it into the outer query at run time. Uses: simplify complex joins, expose a stable interface over changing tables, restrict columns/rows for security. Materialized views, by contrast, store results.

## Senior angle & interview tips
Views add no performance on their own, and stacking many layers can hide expensive plans. Views are a clean contract boundary: refactor tables behind them without breaking consumers. Secure views and column masking help governance.

Interview question: "View vs materialized view vs table?" Answer: view recomputes every time, materialized view stores refreshable results, table stores data you own and load.
