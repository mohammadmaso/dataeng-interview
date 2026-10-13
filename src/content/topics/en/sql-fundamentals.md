---
order: 16
tid: "sql-fundamentals"
track: "2"
section: null
num: "2.1"
title: "SQL Fundamentals & Logical Query Order"
type: "concept"
level: "beginner"
priority: "P0"
est_hours: 3
summary: "SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT and the logical execution order (FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY). Understanding this prevents many mistakes."
covers: ["aliases scope","DISTINCT","CASE WHEN","COALESCE","aggregate functions","COUNT(*) vs COUNT(col)"]
interview_qs: []
extras: []
prerequisites: []
related: ["sql-advanced-grouping", "sql-nulls", "sql-joins", "sql-subqueries-ctes", "sql-window-functions"]
---
## 🎯 TL;DR

> A SQL query is written as a list of clauses, but the database does **not** evaluate them in the order you type them. The logical order is `FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT`. Most "why doesn't my query work?" questions are answered by that order.

- `WHERE` filters **rows** before grouping. `HAVING` filters **groups** after aggregation.
- An alias defined in `SELECT` does not exist yet inside `WHERE` or `HAVING`.
- `NULL` means "unknown", not zero and not an empty string. Compare it with `IS NULL`, never with `=`.
- `COUNT(*)` counts rows, `COUNT(col)` counts non-NULL values, and `CASE WHEN` is SQL's if/else.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Think of building a sales report by hand. First you take the sales slips out of the drawer (`FROM`). You throw away the refunded ones (`WHERE`), pile the rest by customer (`GROUP BY`), keep only the big customers (`HAVING`), choose which columns go on the report (`SELECT`), sort it (`ORDER BY`), and print the top 10 (`LIMIT`).

We will use this table in the whole lesson:

```sql
CREATE TABLE orders (id int, customer_id int, amount numeric, status text);
INSERT INTO orders VALUES
 (1, 10, 120.00, 'paid'),
 (2, 10,  80.00, 'refunded'),
 (3, 11, 200.00, 'paid'),
 (4, 12,    NULL, 'pending'),
 (5, 11,  50.00, 'paid');
```

Here is a query that uses every main clause:

```sql
SELECT customer_id, COUNT(*) AS n_orders, SUM(amount) AS paid_total
FROM orders
WHERE status = 'paid'
GROUP BY customer_id
HAVING SUM(amount) > 100
ORDER BY paid_total DESC
LIMIT 10;
```

**Predict before you look:** which customers appear, and in what order?

<details><summary>Your guess?</summary>

Customer 11 first (two paid orders, total 250.00), then customer 10 (one paid order, total 120.00). Customer 12 has only a `pending` order, so `WHERE` removes that row before grouping even starts. The result is `(11, 2, 250.00)` and `(10, 1, 120.00)`.

</details>

Each clause answers one question:

| Clause | Question it answers | Works on |
|---|---|---|
| `FROM` | Which tables do the rows come from? | tables → rows |
| `WHERE` | Which rows should I keep? | single rows |
| `GROUP BY` | Which rows belong together? | rows → groups |
| `HAVING` | Which groups should I keep? | groups |
| `SELECT` | Which columns or expressions do I output? | groups or rows |
| `ORDER BY` | In what order do I show them? | output rows |
| `LIMIT` | How many do I show? | output rows |

### Aliases and their scope

An alias (`AS paid_total`) is created in `SELECT`, which runs late. So this fails in PostgreSQL:

```sql
SELECT amount * 0.9 AS net
FROM orders
WHERE net > 100;   -- ERROR: column "net" does not exist
```

The fix is to repeat the expression in `WHERE`, or to move the query into a subquery or CTE (covered in **sql-subqueries-ctes**). `ORDER BY` can use the alias because it runs after `SELECT`.

### Counting, NULLs and DISTINCT

```sql
SELECT COUNT(*)                    AS all_rows,     -- 5
       COUNT(amount)               AS with_amount,  -- 4, the NULL is skipped
       AVG(amount)                 AS avg_amount,   -- 450 / 4 = 112.5
       COALESCE(SUM(amount), 0)    AS total         -- 450
FROM orders;

SELECT DISTINCT status FROM orders;  -- paid, refunded, pending (row order not guaranteed)
```

`SUM` and `AVG` ignore NULL values. They do not treat NULL as zero, which is why the average divides by 4 and not 5. `COALESCE` returns the first non-NULL argument, so it turns a missing amount into a default.

### CASE WHEN

```sql
SELECT id,
       CASE
         WHEN status = 'paid'     THEN 'revenue'
         WHEN status = 'refunded' THEN 'money back'
         ELSE 'open'
       END AS bucket,
       COALESCE(amount, 0) AS amount_or_zero
FROM orders
ORDER BY id;
```

Rows: `1 revenue 120`, `2 money back 80`, `3 revenue 200`, `4 open 0`, `5 revenue 50`. `CASE` checks the conditions from top to bottom and returns the first match. Without an `ELSE`, the result is `NULL`.

## 🔧 How it really works (Mid)

The logical order is a **semantic contract**: it tells you what the result must be. The engine's planner is free to run things in a different physical order (for example, push a filter down to the scan), as long as the result is the same. Think of the logical order as the rules of the game, and the planner as the referee who may move faster on the field.

The order explains the common rules:

- **Why `WHERE` cannot see aggregates:** `SUM(amount)` does not exist until `GROUP BY` has run. Use `HAVING` for that condition.
- **Why `WHERE` cannot see `SELECT` aliases:** `SELECT` has not run yet.
- **Why `ORDER BY` can see aliases:** it runs after `SELECT`.
- **Why filtering in `WHERE` is cheaper:** the rows removed by `WHERE` never reach the grouping step. `HAVING` removes groups after they were built.

```mermaid
flowchart LR
  A[FROM: pick tables] --> B[WHERE: filter rows]
  B --> C[GROUP BY: build groups]
  C --> D[HAVING: filter groups]
  D --> E[SELECT: compute columns]
  E --> F[ORDER BY: sort]
  F --> G[LIMIT: cut output]
```

### NULL uses three-valued logic

A comparison with NULL returns `UNKNOWN`, and `WHERE` keeps only rows where the condition is `TRUE`. Try this on the same table:

```sql
SELECT id FROM orders WHERE amount = NULL;               -- 0 rows, never true
SELECT id FROM orders WHERE amount IS NULL;              -- 4
SELECT id FROM orders WHERE amount > 100 OR amount <= 100; -- 4 rows (1, 2, 3, 5), not 5
```

The third query looks like it should return every row. It does not: row 4 has `amount = NULL`, so both sides are `UNKNOWN`, and `UNKNOWN OR UNKNOWN` is still `UNKNOWN`.

Also, `WHERE status <> 'paid'` silently drops rows where `status` is NULL. If you need them, write `WHERE status IS DISTINCT FROM 'paid'` (PostgreSQL syntax, also available in several other engines).

## 🧠 Senior depth & trade-offs

- **Determinism.** A `LIMIT` without `ORDER BY` returns an arbitrary subset. This is common in dashboards that "change every refresh". Always add an `ORDER BY` with a unique tie-breaker (for example, `ORDER BY paid_total DESC, customer_id`). The same applies to views and to distributed engines such as Spark and Trino, where row order is not preserved between stages.
- **Column pruning on columnar storage.** On Parquet, ClickHouse or StarRocks, `SELECT *` reads every column from disk, so it costs far more than a list of the columns you need. In a warehouse, explicit column lists are a cost decision, not only a style choice.
- **`DISTINCT` can hide bugs.** If a join duplicates rows (see **sql-joins**), `DISTINCT` makes the report look right while the numbers are still wrong. Treat `DISTINCT` on a large table as a smell to investigate, and `COUNT(DISTINCT ...)` as an expensive aggregate. Approximate distinct counts (for example `uniq` in ClickHouse or HyperLogLog-based functions) trade a small error for a large speed-up.
- **When not to use `HAVING`.** If the condition only uses columns from the rows (not aggregates), put it in `WHERE`. The result is the same, and the engine processes fewer rows.
- **Check the docs for your version** for how `NULLS FIRST`/`NULLS LAST` defaults differ between engines when you sort.

## 🧪 Try it

Run the dataset from the beginning of this lesson in PostgreSQL.

1. **Groups with more than one row.** Show `status`, the number of orders, and the total amount per status, but only for statuses that have more than one row.
2. **Missing or small amounts.** List the `id` of orders whose amount is missing or below 100, sorted by `id`.
3. **Broken query.** The following query fails. Explain why, then fix it so it returns the customers whose total is above 100.

```sql
SELECT customer_id, SUM(amount) AS total
FROM orders
WHERE total > 100
GROUP BY customer_id;
```

<details><summary>Show solutions</summary>

**1.**
```sql
SELECT status, COUNT(*) AS n, SUM(amount) AS total
FROM orders
GROUP BY status
HAVING COUNT(*) > 1;
```
Result: `paid 3 370.00`. The `refunded` and `pending` statuses have one row each, so they are removed by `HAVING`.

**2.**
```sql
SELECT id
FROM orders
WHERE amount IS NULL OR amount < 100
ORDER BY id;
```
Result: `2`, `4`, `5`. Note that `amount < 100` alone would miss row 4.

**3.** `total` is an alias, and `WHERE` runs before `SELECT`, so the name does not exist yet. Also, an aggregate such as `SUM` cannot be used in `WHERE` at all. The fix moves the condition to `HAVING`:
```sql
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 100;
```
Result: customer 10 with total 200 and customer 11 with total 250. Customer 12 has a NULL total, so `NULL > 100` is not true and the row is dropped.

</details>

## 💬 Interview corner

**Q: Why can't I use a `SELECT` alias in `WHERE`?**

> **🎤 Say this in the interview:** SQL evaluates clauses in a logical order: `FROM`, `WHERE`, `GROUP BY`, `HAVING`, `SELECT`, `ORDER BY`. The alias is created in the `SELECT` step, which runs after `WHERE`, so `WHERE` cannot see it. `ORDER BY` can use it because it runs later. The planner may execute things in a different physical order, but the result must match this logical order.

A **junior** answer stops at "because the order is FROM, WHERE, ...". A **mid** answer explains `WHERE` versus `HAVING` and why aggregates belong in `HAVING`. A **senior** answer adds that the logical order is the semantic contract, that `LIMIT` without `ORDER BY` is non-deterministic, and that filtering early reduces the work for the grouping step.

**Q: What is the difference between `COUNT(*)` and `COUNT(col)`?**

`COUNT(*)` counts rows. `COUNT(col)` counts rows where `col` is not NULL. On the sample table, `COUNT(*)` is 5 and `COUNT(amount)` is 4.

## ⚠️ Common mistakes

- **`= NULL` instead of `IS NULL`.** `WHERE amount = NULL` returns nothing. Use `IS NULL` or `IS NOT NULL`.
- **Aggregates in `WHERE`.** `WHERE SUM(amount) > 100` is an error. Use `HAVING`.
- **Aliases in `WHERE`.** Repeat the expression, or wrap the query.
- **Assuming `NOT` and `<>` keep NULLs.** `WHERE status <> 'paid'` drops rows where `status` is NULL.
- **Confusing the two counts.** `COUNT(amount)` silently skips NULL amounts, so averages and counts can disagree with what you expected.
- **`LIMIT` without `ORDER BY`.** The result is not stable between runs.
- **`SELECT *` in production code.** Schema changes can break downstream jobs, and columnar engines pay for every column you did not need.

## ✅ Check yourself (quiz)

1. Which clause runs first?
   - A) `SELECT`
   - B) `WHERE`
   - C) `ORDER BY`
   - D) `LIMIT`

2. Why does `SELECT amount AS a FROM orders WHERE a > 100;` fail in PostgreSQL?
   - A) `amount` is not numeric
   - B) `a` is created in `SELECT`, which runs after `WHERE`
   - C) `WHERE` cannot use comparisons with `>`
   - D) The alias must be quoted

3. How many rows does `SELECT COUNT(amount) FROM orders;` return, using the sample table?
   - A) 5
   - B) 4
   - C) 1
   - D) 0

4. What does `SELECT id FROM orders WHERE amount = NULL;` return?
   - A) All rows
   - B) The rows where `amount` is 0
   - C) Zero rows
   - D) An error

5. **Senior question.** A dashboard shows "top 10 customers" with `LIMIT 10` and no `ORDER BY`. The list changes on every refresh. Why, and what is the fix?

<details><summary>Answer</summary>

**1 — B.** `FROM` runs first, then `WHERE`. `SELECT`, `ORDER BY` and `LIMIT` come later.

**2 — B.** The alias is defined in `SELECT`, and `WHERE` runs before `SELECT`. Repeat the expression or use a subquery.

**3 — B.** `COUNT(col)` skips NULL values, and one of the five `amount` values is NULL, so the result is 4.

**4 — C.** `amount = NULL` is `UNKNOWN` for every row, and `WHERE` keeps only `TRUE`. Use `IS NULL`.

**5 — (senior answer).** Without `ORDER BY`, the database has no obligation to return the same rows in the same order, so `LIMIT 10` returns an arbitrary subset. Add `ORDER BY total_spent DESC, customer_id` so the order is stable, with a unique tie-breaker.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Logical order of a query | FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT |
| WHERE vs HAVING | WHERE filters rows before grouping; HAVING filters groups after aggregation |
| Why can't WHERE use a SELECT alias? | Aliases are created in SELECT, which runs after WHERE |
| `COUNT(*)` vs `COUNT(col)` | Rows vs non-NULL values of `col` |
| How do you test for NULL? | `IS NULL` / `IS NOT NULL` (never `= NULL`) |
| What does COALESCE do? | Returns the first non-NULL argument |
| Is `LIMIT` without `ORDER BY` stable? | No. The subset of rows is arbitrary |

## 🔗 Go further

- **sql-advanced-grouping** extends `GROUP BY` with `ROLLUP`, `CUBE` and `GROUPING SETS`.
- **sql-nulls** covers NULL behavior in depth, including sorting and indexes.
- **sql-joins** is the next step: combining tables without duplicating or losing rows.
- **sql-window-functions** shows how to keep every row while still computing group-level values.
- Read the NULL-handling and sorting pages of your engine's documentation once; defaults differ between PostgreSQL, MySQL and warehouses.
