---
term: "Window function"
aliases: ["window functions", "analytic function", "analytic functions", "over clause"]
category: sql
short: "A SQL function computed over a set of rows related to the current row without collapsing them into one, using OVER(...)."
related: ["cte", "sessionization", "grain"]
---
## In plain words (Junior)
Imagine a class results table where you want each student's score next to the class average, or their rank in class, while still seeing every student's row. A normal `GROUP BY` would collapse the table to one row per class. A window function lets each row look at its neighbors or group and compute something without losing the rows.

## How it works (Mid)
Syntax: `func() OVER (PARTITION BY ... ORDER BY ... frame)`. `PARTITION BY` splits rows into groups, `ORDER BY` orders inside, and the frame (`ROWS`/`RANGE BETWEEN`) defines which rows around the current one are included.

```
SELECT user_id, order_ts, amount,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_ts DESC) AS rn,
  SUM(amount)  OVER (PARTITION BY user_id ORDER BY order_ts
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
  LAG(amount)  OVER (PARTITION BY user_id ORDER BY order_ts) AS prev_amount
FROM orders;
```

Families: ranking (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`), offset (`LAG`, `LEAD`, `FIRST_VALUE`), and aggregates over a frame (`SUM`, `AVG`). Typical uses: top-N per group (filter `rn = 1` in an outer query), running totals, moving averages, gaps and islands, dedup, sessionization. Window functions run after `WHERE`/`GROUP BY`, so you cannot filter on them in the same `WHERE`; use a CTE or `QUALIFY` where supported.

## Senior angle & interview tips
Know the details interviewers probe: default frame with `ORDER BY` is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which treats ties as peers, so running sums with duplicate timestamps differ from `ROWS`; `RANK` leaves gaps, `DENSE_RANK` does not, `ROW_NUMBER` breaks ties arbitrarily unless you add a tiebreaker. Performance: each distinct window spec needs a sort/shuffle per partition key, so share specs and watch skewed partitions. In distributed engines a missing `PARTITION BY` sends everything to one node.

Interview question: "Get the latest order per customer." Model answer: `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC, order_id DESC)` in a subquery, then keep `rn = 1`; the extra tiebreaker keeps it deterministic.
