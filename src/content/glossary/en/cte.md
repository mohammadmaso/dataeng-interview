---
term: "CTE (common table expression)"
aliases: ["cte", "common table expression", "with clause", "recursive cte"]
category: sql
short: "A named temporary result defined with WITH that a query can reference, improving readability and enabling recursion."
related: ["window-function", "grain"]
---
## In plain words (Junior)
When solving a long math problem you write intermediate results on the side with names: "let A be the total, then B is A divided by 3." A CTE lets you do that in SQL. You give a sub-result a name and then use it in the final query, which makes long queries readable step by step.

## How it works (Mid)
A CTE is introduced by `WITH name AS (subquery)` and scoped to the single statement. You can chain several, each referencing earlier ones. It does not store data by itself. Whether it is inlined or materialized depends on the engine: Postgres 12+ inlines by default unless it is referenced several times or marked `MATERIALIZED`; other engines may re-execute it for each reference or cache it.

```
WITH recent AS (
  SELECT * FROM orders WHERE order_ts >= current_date - 30
),
per_user AS (
  SELECT user_id, SUM(amount) AS total FROM recent GROUP BY user_id
)
SELECT * FROM per_user WHERE total > 1000;

-- recursive: org chart
WITH RECURSIVE tree AS (
  SELECT id, manager_id, 1 AS lvl FROM emp WHERE manager_id IS NULL
  UNION ALL
  SELECT e.id, e.manager_id, t.lvl + 1 FROM emp e JOIN tree t ON e.manager_id = t.id
)
SELECT * FROM tree;
```

`WITH RECURSIVE` has an anchor member plus a recursive member joined with `UNION ALL`; it iterates until no new rows appear. Use for hierarchies, graph traversal, and generating series. Guard against cycles with a depth limit or a visited-path column.

## Senior angle & interview tips
Do not assume a CTE is an optimization barrier or a cache: this differs between engines, so check the plan. A CTE referenced twice may be computed twice in engines that inline (expensive on big shuffles), and a materialized one can block predicate pushdown. For reuse of heavy intermediate results, use a temp table or materialized view. CTEs are preferred over nested subqueries for readability and in dbt models; a long chain may hide that a `GROUP BY` before a join is what really matters for grain.

Interview question: "CTE vs subquery vs temp table?" Model answer: CTE and subquery are logically equivalent and mostly about readability; a temp table physically stores results so it helps when reused many times or when you need statistics and indexes; recursion is the one thing only CTEs do.
