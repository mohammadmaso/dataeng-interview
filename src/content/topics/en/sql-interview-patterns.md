---
order: 24
tid: "sql-interview-patterns"
track: "2"
section: null
num: "2.9"
title: "Classic SQL Interview Patterns"
type: "practice"
level: "advanced"
priority: "P0"
est_hours: 15
summary: "Patterns that recur over and over in interviews. Each has a standard solution you should know by heart."
covers: []
interview_qs: []
extras: [{"label":"patterns","items":["Top-N per group — ROW_NUMBER + filter","Gaps & Islands — consecutive days / streaks (ROW_NUMBER difference trick)","Sessionization — LAG + new-session flag + cumulative SUM","Running totals & moving averages","Retention & cohort analysis — first-activity cohort, day-N retention","Funnel analysis — step conversion with ordered events","Deduplication — keep latest record per key","Median & percentiles — PERCENTILE_CONT, manual median","Period-over-period — MoM / YoY growth with LAG","Point-in-time lookup — join to SCD2 with valid_from/valid_to","Anti-join — \"never did X\"","Pairs & combinations — self-join with `a.id < b.id`","Histogram / bucketing — WIDTH_BUCKET / FLOOR","Overlapping intervals — concurrent sessions, max concurrency","Market basket — products bought together","Fill forward — last non-null value","Pivot reports — conditional aggregation"]},{"label":"practice","items":["60+ problems (DataLemur, LeetCode SQL 50 + Hard, StrataScratch) tagged by pattern"]}]
prerequisites: ["sql-window-functions", "sql-nulls", "sql-datetime", "sql-subqueries-ctes"]
related: ["sql-joins", "sql-advanced-grouping", "sql-query-optimization", "dimensional-modeling"]
---

## 🎯 TL;DR

> Most SQL interview questions are one of about 17 patterns in disguise. Learn to **recognize the pattern from the wording**, then apply its standard skeleton. You rarely need a new idea; you need the right skeleton.

- Wording clues: "top N per", "consecutive", "streak", "session", "running", "first time", "never", "latest", "between two events".
- Most skeletons use one of three tools: a **window function** (`ROW_NUMBER`, `LAG`, running `SUM`), a **date spine** or **calendar**, or a **conditional aggregate** (`COUNT(CASE WHEN ...)`).
- Always state the tie rule and the grain before you write code.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A cook who knows the basic sauces. A new recipe is usually "make a roux, then add stock". Interview SQL works the same way: "top N per group" is a sauce you make in one pass, and you combine it with filters.

Two tables are enough to practice most patterns in this lesson:

```sql
CREATE TABLE employees (id int, name text, dept text, salary int);
INSERT INTO employees VALUES
 (1,'Ali','data',9000), (2,'Sara','data',12000), (3,'Reza','data',12000),
 (4,'Nima','data',10000), (5,'Maryam','web',8000), (6,'Kian','web',8000),
 (7,'Parisa','web',7000);

CREATE TABLE logins (user_id int, login_date date);
INSERT INTO logins VALUES
 (1,'2026-01-01'), (1,'2026-01-02'), (1,'2026-01-03'), (1,'2026-01-05'),
 (1,'2026-01-06'), (1,'2026-01-06'), (2,'2026-01-02'), (2,'2026-01-04');
```

The first pattern is **top N per group**: "the two highest salaries in each department."

```sql
SELECT dept, name, salary
FROM (
  SELECT dept, name, salary,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, id) AS rn
  FROM employees
) t
WHERE rn <= 2
ORDER BY dept, rn;
-- data: Sara, Reza; web: Maryam, Kian
```

The window numbers the rows inside each department, and the outer filter keeps the first two. The `id` in `ORDER BY` makes the tie order deterministic.

## 🔧 How it really works (Mid)

The patterns below are the ones that come up most. Each one is a skeleton you can adapt.

| Pattern | Clue in the question | Skeleton |
|---|---|---|
| Top-N per group | "top 3 per category" | `ROW_NUMBER() OVER (PARTITION BY g ORDER BY x DESC)` then filter |
| Gaps & islands | "consecutive days", "streak" | `value - ROW_NUMBER()` is constant inside a run |
| Sessionization | "sessions with 30-minute gap" | `LAG` → new-session flag → running `SUM` |
| Deduplication | "latest record per key" | `ROW_NUMBER` with `ORDER BY updated_at DESC, id DESC` |
| Funnel | "viewed, then carted, then bought" | one row per user with `MIN(CASE ...)` per step, then compare |
| Anti-join | "never did X" | `NOT EXISTS` (not `NOT IN`, see [[sql-nulls]]) |
| Fill forward | "last known value" | `MAX(...) OVER (... ROWS UNBOUNDED PRECEDING)` on a group counter |
| Period over period | "month over month growth" | `LAG` over a per-month aggregate |
| Pivot | "one column per status" | `SUM(CASE WHEN status = 'x' THEN 1 ELSE 0 END)` |

**Gaps & islands.** Within one user, `login_date - ROW_NUMBER()` stays constant while the dates are consecutive, because both move by one per row. Equal results mean one streak.

```sql
WITH d AS (SELECT DISTINCT user_id, login_date FROM logins),
g AS (
  SELECT user_id, login_date,
         login_date - (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date))::int AS grp
  FROM d
)
SELECT user_id, MIN(login_date) AS start_date, MAX(login_date) AS end_date, COUNT(*) AS days
FROM g
GROUP BY user_id, grp
ORDER BY user_id, start_date;
-- user 1: 01-01 to 01-03 (3 days), 01-05 to 01-06 (2 days); user 2: two 1-day streaks
```

`DISTINCT` is needed because the duplicate `(1, '2026-01-06')` row would otherwise break the difference.

**Sessionization.** A new session starts when the gap to the previous event is larger than the threshold. Flag those rows, then take a running sum of the flags to get a session number:

```sql
WITH o AS (
  SELECT user_id, ts, LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) AS prev_ts
  FROM page_views
),
f AS (
  SELECT user_id, ts,
         CASE WHEN prev_ts IS NULL OR ts - prev_ts > INTERVAL '30 minutes'
              THEN 1 ELSE 0 END AS new_session
  FROM o
)
SELECT user_id,
       SUM(new_session) OVER (PARTITION BY user_id ORDER BY ts) AS session_id,
       ts
FROM f;
```

The first row of each user has `prev_ts` NULL, so its flag is 1 and it starts session 1. The `IS NULL` check is written explicitly because `NULL > interval` is UNKNOWN (see [[sql-nulls]]).

**Funnel.** Use one row per user, with the first time of each step, and then count the users whose steps happened in order. Conditional `COUNT` ignores the NULL comparisons, so a user who never viewed is not counted as "carted after view":

```sql
WITH s AS (
  SELECT user_id,
         MIN(CASE WHEN ev = 'view' THEN ts END) AS v,
         MIN(CASE WHEN ev = 'cart' THEN ts END) AS c,
         MIN(CASE WHEN ev = 'buy'  THEN ts END) AS b
  FROM events GROUP BY user_id
)
SELECT COUNT(v) AS viewed,
       COUNT(CASE WHEN c > v THEN 1 END) AS carted_after_view,
       COUNT(CASE WHEN b > c THEN 1 END) AS bought_after_cart
FROM s;
```

**Deduplication.** Keep the newest row per key. Add a unique last column to the order so that equal timestamps still give one winner:

```sql
SELECT id, customer_id, updated_at, city
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id
                               ORDER BY updated_at DESC, id DESC) AS rn
  FROM customer_updates
) t
WHERE rn = 1;
```

**Other patterns you should name correctly.** Median: `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)` in engines that support it; a manual median averages the middle one or two rows. Histogram: `FLOOR(x / width) * width` gives bucket starts. Point-in-time lookup: join facts to an SCD2 table with `event_ts >= valid_from AND event_ts < valid_to` (half-open, so boundaries are not counted twice; see [[scd]]). Overlapping intervals: sort starts and ends as +1 and -1 events and take a running sum to get concurrency.

## 🧠 Senior depth & trade-offs

- **Pick the grain first.** Most mistakes come from counting at the wrong grain (per event instead of per user, per row instead of per day). Write the output grain in one sentence before you write a `SELECT`.
- **Window cost.** Each pattern usually needs a sort by `(partition key, order key)`. On a large table, that sort is the cost. Filter by date before the window, and do not build five windows with five different `ORDER BY`s when one pass would do.
- **Skew.** A top-N or sessionization over one hot key (one bot user with millions of events) becomes one slow task in Spark or one hot partition in a warehouse. Check the distribution of the key first. See [[data-skew]].
- **Streams and incremental loads.** In production, these patterns run incrementally. Sessionization across a batch boundary needs the last open session from the previous run, and dedup needs a lookback window. A correct interview answer on a static table can be wrong in a daily pipeline; say so.
- **Engine support.** `QUALIFY` replaces the outer filter in some engines, `PERCENTILE_CONT` and `WIDTH_BUCKET` are not in every engine, and interval syntax differs. Write the logic, then adapt the syntax to the stated engine.

## 🧪 Try it

Use the `employees` and `logins` tables from the beginning of this lesson in PostgreSQL 13 or later.

1. **Streak length.** Find the longest consecutive login streak for each user.
2. **Never logged in.** Return the employees whose `id` never appears in `logins` (assume `logins` has a `user_id` column that refers to employee ids). Use the form that is safe with NULLs.
3. **Second-highest distinct salary per department**, returning nothing when a department has only one distinct salary.

<details><summary>Show solutions</summary>

**1.**
```sql
WITH d AS (SELECT DISTINCT user_id, login_date FROM logins),
g AS (
  SELECT user_id, login_date,
         login_date - (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date))::int AS grp
  FROM d
),
streaks AS (
  SELECT user_id, COUNT(*) AS len FROM g GROUP BY user_id, grp
)
SELECT user_id, MAX(len) AS longest_streak
FROM streaks
GROUP BY user_id;
-- user 1: 3, user 2: 1
```

**2.**
```sql
SELECT e.id, e.name
FROM employees e
WHERE NOT EXISTS (SELECT 1 FROM logins l WHERE l.user_id = e.id);
```
`NOT EXISTS` is used because a `NULL` in `logins.user_id` would make `NOT IN` return no rows.

**3.**
```sql
SELECT dept, salary
FROM (
  SELECT dept, salary,
         DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
  FROM employees
) t
WHERE rnk = 2;
```
`DENSE_RANK` skips ties in the top value, so a department with one distinct salary has no row with `rnk = 2` and drops out.

</details>

## 💬 Interview corner

**Question: "Find users with a 3-day login streak."** Mid: "I'd use the islands trick, `date - ROW_NUMBER()`, then count per group and keep groups of 3 or more." Senior: "I'd first confirm the grain (one row per user per day) and deduplicate, because a double login would break the difference. I'd also say how the same logic runs incrementally, since a streak can cross the day when the batch boundary falls."

**Question: "Sessionize clickstream events with a 30-minute timeout."** Mid: "`LAG` to find the gap, flag new sessions, then a running `SUM`." Senior: "Same skeleton, but I'd check key skew first, and I'd say how state moves across batches, since a session can span two runs."

**Question: "Give the latest record per customer."** Mid: "`ROW_NUMBER` ordered by the timestamp, filter to 1." Senior: "I'd add a unique tiebreaker so the result is deterministic, and I'd say whether the dedup is in the warehouse or in the ingestion layer."

> **🎤 Say this in the interview:** "I identify the pattern from the wording, state the grain and tie rule, and use the standard skeleton. Then I check the NULLs, the duplicates, and whether the same logic still works incrementally."

## ⚠️ Common mistakes

> **⚠️ Watch out:** the islands trick breaks on duplicate dates. Always `DISTINCT` on `(user, day)` first.

- Choosing `ROW_NUMBER` for "top N" when ties should all be included (use `RANK` or `DENSE_RANK`).
- Forgetting the unique tiebreaker in dedup and getting a random winner.
- Using `NOT IN` for anti-joins with a nullable subquery.
- Filtering on a window result in `WHERE` instead of an outer query.
- Counting per event when the question is per user, so the funnel rates are inflated.
- Giving a static-table answer for a streaming or incremental problem without naming the state it needs.

## ✅ Check yourself (quiz)

1. For "top 2 salaries per department, including ties," which function fits best?
   - A) `ROW_NUMBER`
   - B) `RANK` or `DENSE_RANK`
   - C) `NTILE(2)`
   - D) `LAG`

<details><summary>Answer</summary>

**B** — `ROW_NUMBER` gives each tied row a different number, so `rn <= 2` can drop a tied employee arbitrarily. `RANK` or `DENSE_RANK` give tied rows the same number and keep them together. With `RANK <= 2` you get the top two positions including ties; with `DENSE_RANK <= 2` you get the top two distinct salaries. State which one the question means.

</details>

2. In the islands pattern, what does `login_date - ROW_NUMBER()` tell you?
   - A) The number of logins
   - B) A value that stays constant across one consecutive run, so it identifies the run
   - C) The gap between the first and last login
   - D) The user's rank by activity

<details><summary>Answer</summary>

**B** — inside a consecutive run, both the date and the row number increase by one each row, so the difference is constant. Different runs give different constants.

</details>

3. Why does the sessionization query use `prev_ts IS NULL OR ...` in the flag?
   - A) To remove duplicate events
   - B) The first event of a user has no previous event, and a comparison with NULL is UNKNOWN, so the first row must be flagged explicitly
   - C) `LAG` cannot be used with timestamps
   - D) To sort the output

<details><summary>Answer</summary>

**B** — the first event has `prev_ts` NULL. `ts - NULL > interval` is UNKNOWN, which the `CASE` would skip, so the explicit `IS NULL` check makes the first event start session 1.

</details>

4. **(Senior)** A nightly pipeline sessionizes events with a 30-minute timeout, processing one day per run. A user's session starts at 23:50 on day 1 and ends at 00:20 on day 2. Why do the per-day session counts disagree with a single full-history run, and what do you change?

   - A) The window function is non-deterministic
   - B) Each run loses the open session at the boundary; carry the last open session and its last timestamp into the next run (or use a lookback window)
   - C) The engine cannot process more than one day
   - D) Use `DISTINCT` on the session number

<details><summary>Answer</summary>

**B** — the daily run sees two separate sessions, one per day, because it cannot see across the boundary. Carry the open session state forward (or read a lookback window of the previous day) so the incremental result matches the full-history result.

</details>

5. **(Senior)** A dedup job uses `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC)`. In production, some customers get different cities on each re-run. What is the cause and the fix?

   - A) The partition key is too coarse
   - B) Ties on `updated_at` have an arbitrary order, so add a unique tiebreaker such as `id DESC`
   - C) `ROW_NUMBER` cannot be used for deduplication
   - D) The job should use `RANK` instead

<details><summary>Answer</summary>

**B** — when two rows share an `updated_at`, `ROW_NUMBER` may pick either one, and the choice can change between runs. A unique tiebreaker makes the winner deterministic.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Top-N per group | `ROW_NUMBER() OVER (PARTITION BY g ORDER BY x DESC)`, then filter in an outer query |
| Gaps & islands key | `date - ROW_NUMBER()` is constant within a consecutive run (dedupe first) |
| Sessionization | `LAG` → flag when gap > threshold → running `SUM` = session id |
| Dedup | `ROW_NUMBER` with a unique tiebreaker, keep `rn = 1` |
| Funnel | One row per user with `MIN(CASE ...)` per step, then conditional `COUNT` |
| Never did X | `NOT EXISTS`, not `NOT IN` on a nullable column |
| Point-in-time lookup | `event_ts >= valid_from AND event_ts < valid_to` on an SCD2 table |
| Overlapping intervals | +1 at start, -1 at end, running `SUM` gives concurrency |

## 🔗 Go further

- Build the foundations in `sql-window-functions`, `sql-nulls` and `sql-datetime` if any pattern above felt unclear.
- Subquery and CTE forms used in every pattern: `sql-subqueries-ctes`.
- For performance of these queries on large tables, read `sql-query-optimization`. For the modeling side of point-in-time lookups, read `dimensional-modeling`.
