---
term: "Database index"
aliases: ["index", "indexes", "indices", "composite index", "covering index", "secondary index"]
category: sql
short: "An auxiliary data structure that lets a database find rows without scanning the whole table, at the cost of extra storage and slower writes."
related: ["b-tree", "predicate-pushdown", "cardinality"]
---
## In plain words (Junior)
A book's index lets you jump to the right pages instead of reading the whole book. A database index does the same for a table column: the database can find "orders of user 7" immediately. But each time you add new content, the index must be updated too, so more indexes make writing slower.

## How it works (Mid)
Most indexes are B+trees on one or more columns. A composite index on `(a, b)` is sorted by `a` then `b`, so it helps filters on `a` or `a,b` but not on `b` alone (leftmost-prefix). A covering index includes all columns a query needs so the table isn't touched (index-only scan). Other types: hash, GIN/inverted (full text, arrays, JSON), GiST/spatial, bitmap, Bloom, partial indexes (only rows matching a condition), expression indexes.

```
CREATE INDEX idx_orders_user_ts ON orders (user_id, order_ts DESC);   -- composite

-- Uses the index (leftmost prefix, then range):
SELECT * FROM orders WHERE user_id = 7 AND order_ts > '2024-05-01';
-- Cannot use it well (skips leading column / wraps column in function):
SELECT * FROM orders WHERE order_ts > '2024-05-01';
SELECT * FROM orders WHERE date(order_ts) = '2024-05-01';

EXPLAIN (ANALYZE) SELECT ...;      -- Index Scan vs Seq Scan
```

The optimizer decides whether to use an index based on selectivity statistics: for a filter matching a large share of rows, a sequential scan is cheaper than many random lookups. Use `EXPLAIN` to verify. Keep statistics fresh with `ANALYZE`.

## Senior angle & interview tips
Every index costs write amplification, storage and maintenance; unused or duplicate indexes are common waste. Design indexes from query patterns: equality columns first, then range/sort columns; high selectivity first is a heuristic, not a rule. Pitfalls: functions or implicit casts on indexed columns, low-cardinality columns alone, `OR` conditions, leading wildcards in `LIKE`. For data engineering, building indexes on huge tables can lock or take hours (use concurrent builds). Analytical/columnar engines use sort keys, zone maps and skip indexes instead of per-row indexes.

Interview question: "Query is slow despite an index; why?" Model answer: check the plan: maybe the predicate isn't sargable (function or cast on the column), the leading column isn't filtered, statistics are stale, or the filter isn't selective so a full scan wins; fix the predicate, index order or add a covering/partial index.
