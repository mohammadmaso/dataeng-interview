---
order: 22
tid: "sql-nulls"
track: "2"
section: null
num: "2.7"
title: "NULLs & Three-Valued Logic"
type: "concept"
level: "beginner"
priority: "P0"
est_hours: 1
summary: "NULL equals nothing, not even itself. Its impact on predicates, aggregates, joins and NOT IN. A frequent source of bugs and trick questions."
covers: ["IS DISTINCT FROM","NULL ordering","NULLIF","COUNT behavior"]
interview_qs: []
extras: []
prerequisites: ["sql-fundamentals", "sql-joins"]
related: ["sql-subqueries-ctes", "sql-window-functions", "sql-datetime", "data-quality"]
---

## 🎯 TL;DR

> In SQL, `NULL` means **"unknown"**, not zero and not an empty string. Any comparison with `NULL` gives **UNKNOWN**, and `WHERE` keeps only rows where the condition is **TRUE**.

- `x = NULL` is never true, even when `x` is also `NULL`. Use `IS NULL` / `IS NOT NULL`, or `IS DISTINCT FROM` for a null-safe comparison.
- Aggregates skip `NULL` (`COUNT(col)`, `SUM`, `AVG`), but `COUNT(*)` counts rows.
- `NOT IN (subquery)` returns **nothing** as soon as the subquery returns one `NULL`. Prefer `NOT EXISTS`.
- Helpers: `COALESCE` replaces `NULL`, `NULLIF(a, b)` turns a value into `NULL` (for example, to avoid division by zero).

## 🌱 Start simple (Junior)

> **💡 Analogy:** A sealed envelope. Someone asks, "Is the number in the envelope equal to 5?" The honest answer is not "no". It is "I don't know, I haven't opened it." `NULL` is that sealed envelope. Two sealed envelopes are not known to be equal, even though both are sealed.

Here is a small dataset for the whole lesson:

```sql
CREATE TABLE users (id int, name text, phone text);
INSERT INTO users VALUES
 (1,'Ali','0912'), (2,'Sara',NULL), (3,'Reza','0935'), (4,'Nima',NULL);

CREATE TABLE orders (id int, user_id int, amount int);
INSERT INTO orders VALUES
 (10,1,50), (11,1,NULL), (12,3,70), (13,NULL,20);
```

Try these two queries. The first returns nothing, which surprises most beginners:

```sql
SELECT name FROM users WHERE phone = NULL;     -- 0 rows
SELECT name FROM users WHERE phone IS NULL;    -- Sara, Nima
```

`phone = NULL` is UNKNOWN for every row, so `WHERE` drops every row. Only `IS NULL` asks the right question: "is this value missing?"

## 🔧 How it really works (Mid)

SQL uses **three-valued logic**: every condition is TRUE, FALSE or UNKNOWN. The rules that matter:

| Expression | Result |
|---|---|
| `NULL = NULL`, `NULL <> 1`, `NULL > 0` | UNKNOWN |
| `NULL AND FALSE` | FALSE |
| `NULL AND TRUE` | UNKNOWN |
| `NULL OR TRUE` | TRUE |
| `NOT NULL` | UNKNOWN |

`WHERE`, `ON` and `HAVING` keep a row only when the condition is TRUE. UNKNOWN is treated like FALSE there. A `CASE` expression also skips UNKNOWN branches and goes to the next `WHEN` or to `ELSE`.

**Aggregates skip NULL inputs.** This is a common source of wrong averages:

```sql
SELECT COUNT(*)      AS all_rows,   -- 4
       COUNT(amount) AS with_amount, -- 3
       AVG(amount)   AS avg_amount,  -- (50 + 70 + 20) / 3 = 46.67, NULL row ignored
       SUM(amount) * 1.0 / COUNT(*) AS sum_over_rows -- 140 / 4 = 35.0
FROM orders;
```

`orders` has 4 rows but only 3 non-NULL amounts. `AVG` divides by 3. Dividing the sum by `COUNT(*)` divides by 4, so it gives a different number. If a missing amount should count as zero, say so and write `COALESCE(amount, 0)` explicitly. Do not let the engine's default decide the meaning of your metric.

**Null-safe comparison.** When you need "equal, treating two NULLs as equal" (typical for change detection or for comparing two versions of a row), use the standard predicate `IS DISTINCT FROM` (`IS NOT DISTINCT FROM` for equality). Engines differ in syntax: MySQL uses `<=>`.

```sql
SELECT id FROM users
WHERE phone IS DISTINCT FROM 'x';   -- true for NULL phones too
```

**Helpers.**

- `COALESCE(a, b, ...)` returns the first non-NULL argument: `COALESCE(phone, 'none')`.
- `NULLIF(a, b)` returns `NULL` when `a = b`, otherwise `a`. Use it to avoid division by zero: `amount / NULLIF(divisor, 0)`.

**Joins.** An equality join never matches `NULL` with `NULL`, so orders with `user_id` NULL are dropped by an inner join:

```sql
SELECT u.name, o.id
FROM users u JOIN orders o ON u.id = o.user_id;
-- Ali 10, Ali 11, Reza 12   (order 13 has no user; it disappears)
```

A `LEFT JOIN` keeps every user, with `NULL` for the missing order columns: Sara and Nima appear with `NULL` in `o.id`.

**Ordering.** NULLs have no natural position, so engines pick one. PostgreSQL sorts NULLs last for ascending order by default; MySQL and SQLite put them first. Write `NULLS FIRST` or `NULLS LAST` when the engine supports it, and never rely on the default in a report.

## 🧠 Senior depth & trade-offs

- **`NOT IN` is the classic trap.** If the subquery can return a `NULL`, `x NOT IN (...)` is UNKNOWN for every `x` that is not in the list, so the query returns zero rows. The same data passes through `NOT EXISTS` correctly:

  ```sql
  SELECT u.name FROM users u
  WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);
  -- Sara, Nima
  ```

  Anti-joins written with `LEFT JOIN ... IS NULL` behave the same way and are often planned well.

- **Silent metric errors.** NULL is the most common reason a dashboard number is wrong without any error. A `JOIN` on a nullable key drops rows, a `COUNT(col)` and `COUNT(*)` disagree, and a ratio uses a different denominator than expected. Add explicit `not_null` and `unique` tests in your data-quality layer (see [[data-contract]] and the [[idempotency]] discussion for pipelines that reprocess data).
- **Schema design.** Make columns `NOT NULL` when the business rule says the value always exists. Nullable columns cost a little storage and force every query to think about three values. Keep `NULL` for "unknown or not applicable", and do not use it as a stand-in for "zero" or "empty string".
- **Distributed engines follow the same rules.** In Spark, `col("x") == None` is not a null test; use `isNull()`, and use `eqNullSafe` for null-safe equality. Columnar formats such as Parquet record nulls in metadata next to the values, so a mostly-NULL column is cheap to store. The storage format does not change the logic: a NULL is still unknown to the query engine.
- **Trade-off.** A `COALESCE` default hides missing data. It is right for display or for a documented business default. It is wrong when the NULL means "not measured yet", because the average then looks valid when it is not.

## 🧪 Try it

Use the `users` and `orders` tables from the beginning of this lesson in any engine that supports standard SQL.

1. List users who have **no** order, using `NOT EXISTS`. Then explain why `NOT IN (SELECT user_id FROM orders)` returns nothing.
2. Compute the average order amount with NULL amounts treated as **0**, and as **ignored**. Show both results.
3. Count users per phone status with one query: one count for `phone IS NULL` and one for `phone IS NOT NULL`, using conditional aggregation.

<details><summary>Show solutions</summary>

**1.**
```sql
SELECT u.name
FROM users u
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);
-- Sara, Nima
```
`NOT IN` fails because the subquery returns `NULL` (order 13 has no `user_id`). For every user, `id NOT IN (1, 1, 3, NULL)` is never TRUE, so nothing is returned.

**2.**
```sql
SELECT AVG(amount)                   AS ignored_nulls,  -- 46.67
       AVG(COALESCE(amount, 0))      AS nulls_as_zero   -- 35.0
FROM orders;
```

**3.**
```sql
SELECT COUNT(CASE WHEN phone IS NULL THEN 1 END)      AS without_phone,  -- 2
       COUNT(CASE WHEN phone IS NOT NULL THEN 1 END)  AS with_phone      -- 2
FROM users;
```

</details>

## 💬 Interview corner

**Question: "What is the difference between `COUNT(*)` and `COUNT(col)`?"** Mid: "`COUNT(*)` counts rows, `COUNT(col)` counts non-NULL values." Senior: "That is why they disagree on a nullable column. I'd state which one a metric means and test both when a number looks off after a join."

**Question: "Why can `NOT IN` return no rows?"** Mid: "Because of NULL." Senior: "If the subquery returns a NULL, every comparison outside the list is UNKNOWN, so the filter removes everything. I'd use `NOT EXISTS`, which is also null-safe, and I'd check the nullability of the subquery column."

**Question: "How do you compare two columns that may be NULL?"** Mid: "`IS NOT DISTINCT FROM` or `COALESCE`." Senior: "I'd use the null-safe predicate for change detection, and I'd make sure the engine's syntax matches the one the team uses."

> **🎤 Say this in the interview:** "NULL is unknown, so every comparison with it is unknown and `WHERE` drops it. I use `IS NULL`, `IS DISTINCT FROM`, `NOT EXISTS` and explicit `COALESCE` so the result does not depend on hidden defaults."

## ⚠️ Common mistakes

> **⚠️ Watch out:** `WHERE col <> 'x'` silently drops rows where `col` is NULL. If those rows should be kept, write `WHERE col IS DISTINCT FROM 'x'`.

- Writing `= NULL` or `<> NULL` instead of `IS NULL` or `IS NOT NULL`.
- Expecting `AVG` to treat NULL as zero.
- Using `NOT IN` with a subquery that can return NULL.
- Assuming an inner join keeps rows whose key is NULL.
- Relying on the default NULL ordering when moving a query to another engine.
- Using `COALESCE(x, 0)` to hide missing data that should stay missing.

## ✅ Check yourself (quiz)

1. What does `SELECT * FROM users WHERE phone = NULL;` return?
   - A) Every row
   - B) Only rows where `phone` is NULL
   - C) No rows
   - D) An error in every engine

<details><summary>Answer</summary>

**C** — `phone = NULL` is UNKNOWN for every row, and `WHERE` keeps only TRUE. Use `phone IS NULL`.

</details>

2. In the `orders` table, `amount` has values 50, NULL, 70, 20. What does `AVG(amount)` return?
   - A) 35
   - B) 46.67
   - C) 140
   - D) NULL

<details><summary>Answer</summary>

**B** — `AVG` ignores the NULL and divides by 3: (50 + 70 + 20) / 3 = 46.67. Dividing by `COUNT(*)` (4) would give 35, which is a different metric.

</details>

3. Which query correctly returns users with no orders, even when `orders.user_id` contains NULL?
   - A) `SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders)`
   - B) `SELECT name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)`
   - C) `SELECT name FROM users WHERE id = NULL`
   - D) `SELECT name FROM users WHERE id <> ANY (SELECT user_id FROM orders)`

<details><summary>Answer</summary>

**B** — `NOT EXISTS` is not affected by NULL in the subquery. Option A returns nothing when any `user_id` is NULL, and option D does not express "no orders".

</details>

4. Which expression gives `NULL` instead of an error when `divisor` is 0?
   - A) `amount / COALESCE(divisor, 0)`
   - B) `amount / NULLIF(divisor, 0)`
   - C) `amount / IFNULL(divisor, 0)`
   - D) `amount / (divisor + 0)`

<details><summary>Answer</summary>

**B** — `NULLIF(divisor, 0)` returns NULL when the divisor is 0, and dividing by NULL gives NULL. A and C replace NULL, not zero, so they still divide by zero. D still divides by zero.

</details>

5. **(Senior)** A nightly job joins `events` to `users` on `users.account_id`, which is nullable. The row count drops after a schema change that made `account_id` nullable for some users. What is the likely cause, and what would you change?

   - A) The join became a cross join
   - B) Rows with NULL `account_id` no longer match in the equality join; use a `LEFT JOIN` if those events must be kept, and add a `not_null` or relationship test to catch the change
   - C) NULL values are converted to 0 by the engine, causing duplicates
   - D) The fix is to add `DISTINCT`

<details><summary>Answer</summary>

**B** — equality joins never match NULL, so those events vanish from an inner join. The right fix depends on the business rule: keep them with a `LEFT JOIN`, or make the column `NOT NULL` at the source and test for it.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What does `NULL` mean in SQL? | Unknown or missing, not zero and not an empty string |
| Result of `NULL = NULL` | UNKNOWN (not TRUE), so `WHERE` drops the row |
| Null-safe comparison | `IS NOT DISTINCT FROM` (equal) or `IS DISTINCT FROM` (different); MySQL uses `<=>` |
| `COUNT(*)` vs `COUNT(col)` | Rows vs non-NULL values of `col` |
| Why does `NOT IN` return nothing? | A NULL in the subquery makes every non-matching comparison UNKNOWN |
| `NULLIF(a, b)` | Returns NULL when `a = b`, otherwise `a`; common for avoiding division by zero |
| `COALESCE(a, b, ...)` | First non-NULL argument |

## 🔗 Go further

- Next lesson: **Dates, Times & Time Zones in SQL** (`sql-datetime`) applies the same "missing value" thinking to timestamps.
- Review the join rules in `sql-joins` and the subquery forms in `sql-subqueries-ctes`.
- Pattern practice where NULL handling matters: `sql-interview-patterns`.
