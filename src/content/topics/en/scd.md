---
order: 60
tid: "scd"
track: "6"
section: null
num: "6.5"
title: "Slowly Changing Dimensions"
type: "concept"
level: "intermediate"
priority: "P0"
est_hours: 4
summary: "Keeping dimension history: SCD types 0, 1, 2, 3, 4, 6. Implementing SCD2 with valid_from/valid_to/is_current and MERGE, and point-in-time joins."
covers: ["dbt snapshots","SCD2 in lakehouse (MERGE INTO)","late-arriving dimensions"]
interview_qs: ["Implement SCD2 in SQL","A customer changes city — how does last year's revenue report stay correct?"]
extras: []
prerequisites: ["dimensional-modeling"]
related: ["star-snowflake-obt", "pattern-history", "lakehouse-table-formats", "dbt", "sql-ddl-merge"]
---
## 🎯 TL;DR

> A **slowly changing dimension (SCD)** is a descriptive attribute that changes over time, such as a customer's city. **How you store the change decides which historical numbers stay true.**

- **Type 1** overwrites the old value. Simple, but the history is gone.
- **Type 2** adds a new row per version, with `valid_from`, `valid_to` and `is_current`. It keeps full history and is the default answer to "what was true then?".
- **Type 3** adds a column for the previous value. It remembers only one step back.
- Types 0, 4 and 6 are variants: keep the original value forever, move history to a side table, or mix 1, 2 and 3.
- Facts must join to SCD2 rows by **point in time** (the date of the event), not to whatever row is current today.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A school roster. When a student moves from class 7-A to 7-B, the admin can either erase the old class (Type 1), or write "7-A until March, 7-B from March" (Type 2). Only the second way answers "which class was she in when the exam happened?"

Imagine a customer, Ali (`customer_id = 42`), who moves from Tehran to Shiraz on 2026-03-01. If you simply update the row, every past order of Ali now appears to have been placed in Shiraz:

```sql
-- Type 1: overwrite. History is lost.
UPDATE dim_customer SET city = 'Shiraz' WHERE customer_id = 42;
```

With Type 2, the dimension keeps both versions as separate rows:

```text
customer_key | customer_id | city   | valid_from | valid_to   | is_current
-------------+-------------+--------+------------+------------+-----------
1            | 42          | Tehran | 2024-01-01 | 2026-03-01 | false
2            | 42          | Shiraz | 2026-03-01 | 9999-12-31 | true
```

The `customer_key` is a [[surrogate-key]]: a meaningless integer that identifies one *version*. The `customer_id` is the business key that identifies the *person*. A fact row stores the surrogate key, so it points at the version that was valid when the sale happened.

## 🔧 How it really works (Mid)

### Choosing a type

| Type | What changes in the warehouse | Keeps history? | Typical use |
|---|---|---|---|
| 0 | Nothing: the first value is kept forever | Original only | Birth date, original signup channel |
| 1 | Old value overwritten | No | Typo fixes, values that must always be current |
| 2 | New row, old row closed | Full | Customer city, product category, price tier |
| 3 | Extra column `previous_city` | One step | "Before and after" reporting for a reorg |
| 4 | Current table + separate history table | Full, in a side table | Very large or fast-changing dimensions |
| 6 | Hybrid: 1 + 2 + 3 in one row | Full, plus current value on every row | Reports that need both "as was" and "as is" |

### The SCD2 MERGE

Assume a staging table `stg_customer(customer_id, city, load_date)` with one row per customer for the latest extract. The MERGE must do three things: close the current version of changed customers, insert the new version of changed customers, and insert brand-new customers.

```sql
MERGE INTO dim_customer AS d
USING (
  -- 1) changed customers: match the current version so it gets closed
  SELECT s.customer_id AS merge_key, s.customer_id, s.city, s.load_date
  FROM stg_customer s
  JOIN dim_customer c ON c.customer_id = s.customer_id AND c.is_current
  WHERE c.city <> s.city
  UNION ALL
  -- 2) changed customers (again, with no match key) and new customers: insert
  SELECT NULL, s.customer_id, s.city, s.load_date
  FROM stg_customer s
  LEFT JOIN dim_customer c ON c.customer_id = s.customer_id AND c.is_current
  WHERE c.customer_id IS NULL OR c.city <> s.city
) AS src
ON d.customer_id = src.merge_key AND d.is_current
WHEN MATCHED THEN
  UPDATE SET valid_to = src.load_date, is_current = false
WHEN NOT MATCHED THEN
  INSERT (customer_id, city, valid_from, valid_to, is_current)
  VALUES (src.customer_id, src.city, src.load_date, DATE '9999-12-31', true);
```

The trick is the `NULL` match key. A `NULL` never matches, so the second copy of a changed row falls into `WHEN NOT MATCHED` and is inserted as the new version. The first copy matches the old current row and closes it. The exact MERGE rules (for example, how many source rows may match one target row) depend on the engine. Delta, Iceberg, Snowflake, BigQuery and Postgres 15+ all support this shape, but check your engine's documentation for the details.

### Reading SCD2 correctly: point-in-time joins

```sql
SELECT o.order_id, o.order_date, o.amount, c.city
FROM fact_orders AS o
JOIN dim_customer AS c
  ON c.customer_id = o.customer_id
 AND o.order_date >= c.valid_from
 AND o.order_date <  c.valid_to;
```

A January order joins to the Tehran row, and an April order joins to the Shiraz row. The `valid_to` boundary is **exclusive**, so a change on 2026-03-01 is never counted twice on that day.

> **⚠️ Watch out:** Joining the fact to `WHERE is_current` gives you today's city for every historical sale. The totals are then "revenue by where customers live now", which is a different and usually wrong business question.

### dbt snapshots

dbt snapshots generate SCD2 for you: you choose a `timestamp` strategy (use an `updated_at` column) or a `check` strategy (compare selected columns), and dbt writes the validity columns. Use them when you want the framework to own the bookkeeping. Write the MERGE yourself when you need custom logic, such as late data handling or a different load pattern.

## 🧠 Senior depth & trade-offs

- **Change detection at scale.** Comparing ten columns with `<>` in every join is slow and error-prone. Compute a `row_hash` over the tracked attributes (use a stable, delimiter-safe serialization so `'ab' || 'c'` and `'a' || 'bc'` do not collide) and compare hashes. Keep the hash column in the dimension so future loads are cheap.
- **Track only what matters.** Every Type 2 attribute multiplies the row count. A fast-changing attribute, such as a loyalty score that changes weekly, can make the dimension huge. Move it to a **mini-dimension** (a separate small dimension of band values, such as `tier` and `score_band`), and reference it from the fact.
- **Business time, not load time.** Set `valid_from` from the business event timestamp (when the change happened in the source), not from the moment your pipeline ran. Otherwise a late batch rewrites history in the wrong order.
- **Late-arriving dimensions.** A fact may arrive before the dimension version it refers to. Insert an **inferred member** (a placeholder row with a surrogate key and "unknown" attributes), then correct it when the real dimension record arrives. This is a Type 1 correction on an inferred row.
- **Late-arriving updates.** If an older change arrives after a newer one, you must re-open and re-close validity intervals in order. Recompute the affected customer's chain instead of blindly appending. The MERGE must be **idempotent**: running the same batch twice must not create a second version. Deduplicate staging by `(customer_id, change_timestamp)` first.
- **Concurrency.** Two MERGE jobs on the same dimension can conflict or rewrite the same files in a lakehouse table format. Serialize the dimension load, and make it the only writer.
- **Monitoring.** Test that every `customer_id` has exactly one `is_current = true` row, that validity ranges do not overlap, and that they do not leave gaps. These three invariants catch most SCD bugs. Watch the row growth per day, because it reveals an attribute that should not be Type 2.
- **When not to use SCD2.** If the business never asks "what was true then?", Type 1 is cheaper and simpler. Ask the question before you pay for history.

## 🧪 Try it

1. **Exercise.** A customer moves Tehran → Shiraz on 2026-03-01, then Shiraz → Isfahan on 2026-09-01. Write the three rows of `dim_customer` (ignore the surrogate key). Then say which city an order on 2026-06-15 maps to.

<details><summary>Solution</summary>

| customer_id | city | valid_from | valid_to | is_current |
|---|---|---|---|---|
| 42 | Tehran | 2024-01-01 | 2026-03-01 | false |
| 42 | Shiraz | 2026-03-01 | 2026-09-01 | false |
| 42 | Isfahan | 2026-09-01 | 9999-12-31 | true |

The order on 2026-06-15 is in the Shiraz row, because `2026-03-01 <= 2026-06-15 < 2026-09-01`.

</details>

2. **Exercise.** Run the MERGE above twice with the same staging data. What should the second run change? (Answer: nothing. If it changes something, the MERGE is not idempotent.)

<details><summary>Solution</summary>

Nothing. After the first run, the customer's current row already has the staging city, so the changed-customer branch returns no rows and no new customer is inserted. Re-running a batch is safe only because the predicate compares against the current state.

</details>

## 💬 Interview corner

**"Implement SCD2 in SQL."** Expect a MERGE with the close-then-insert pattern, `valid_from`, `valid_to`, `is_current`, and a stated open-ended date such as `9999-12-31`. Name the exclusive end boundary.

- **Mid answer:** "I update the current row's `valid_to` and `is_current`, then insert the new version with `valid_from` set to the change date."
- **Senior answer:** "I make the load idempotent, use a business change timestamp, hash the tracked columns, handle inferred members for late dimensions, and add tests for one current row per key and no overlapping ranges."

**"A customer changes city. How does last year's revenue report stay correct?"** Model answer: "The dimension is SCD Type 2, so the old city stays on its own row with a closed validity range. Facts store the surrogate key of the version that was valid at the sale, so last year's orders still point at the Tehran row. The report joins by point in time, using the order date between `valid_from` and `valid_to`, so it does not change when the customer moves. If the business needs 'where are customers now', that is a separate query on the current row."

## ⚠️ Common mistakes

- **Joining facts to the current row only.** This silently restates history. Always join by event date.
- **Wrong boundary.** Using `<=` on both ends counts a change day twice. Use `valid_from <= t < valid_to`.
- **Using load time instead of business time.** A late batch then closes the wrong version.
- **Comparing with `<>` on nullable columns.** `NULL <> 'x'` is not true, so a change from `NULL` to a value is missed. Use a null-safe comparison, such as `IS DISTINCT FROM`, where your engine supports it, or coalesce with a sentinel value.
- **Making everything Type 2.** The dimension grows without bound and loses its meaning.
- **Two current rows per key.** This usually means a non-idempotent load ran twice or a race happened.
- **Calling Type 1 "SCD" and believing you have history.** Overwriting is not history.

## ✅ Check yourself (quiz)

1. The CFO wants last quarter's sales grouped by each product's category **as it was when sold**, even though categories are reorganized every quarter. Which SCD type fits?
   - A) Type 1
   - B) Type 2
   - C) Type 0
   - D) Type 3 with no history table

<details><summary>Answer</summary>

**B** — Type 2 keeps every version of the category with a validity range, so each sale joins to the category that was valid then. Type 1 erases the old category, and Type 0 never changes the original value.

</details>

2. What does Type 3 keep?
   - A) The full history of every change
   - B) The current value and one previous value, in extra columns
   - C) Only the first value ever recorded
   - D) A copy of the fact table

<details><summary>Answer</summary>

**B** — Type 3 adds a column for the prior value. It cannot answer questions about two changes ago, which is why it is rarely used alone.

</details>

3. A sale on 2026-02-15 is joined to `dim_customer WHERE is_current`. The customer moved on 2026-03-01. What goes wrong?
   - A) Nothing; the join is correct
   - B) The sale is attributed to the customer's new city, which did not exist yet on that date
   - C) The sale is dropped
   - D) The join fails with a duplicate-key error

<details><summary>Answer</summary>

**B** — The current row is the Shiraz row, so a February sale is reported in Shiraz. A point-in-time join on the order date would return the Tehran row.

</details>

4. **(Senior)** Why compute a hash of the tracked attributes for change detection, and what must you guarantee about the serialization?
   - A) Hashes make the MERGE faster by skipping the join
   - B) It lets you compare one value per row instead of many columns; the serialization must be stable and delimiter-safe so different values cannot produce the same input string
   - C) Hashes remove the need for `valid_from`
   - D) Hashes are only needed for Type 1

<details><summary>Answer</summary>

**B** — One hash comparison replaces many column comparisons. The input must be serialized with separators and a fixed NULL marker, otherwise different tuples can collide and a real change is missed.

</details>

5. **(Senior)** A `loyalty_score` changes almost daily and is tracked as Type 2 in a 20-million-row customer dimension. What is the better design?
   - A) Switch it to Type 1 and accept losing history
   - B) Split it into a mini-dimension of score bands, and reference that from the fact table
   - C) Store it in the fact table only, as a free-text column
   - D) Add a unique index on `customer_id`

<details><summary>Answer</summary>

**B** — A mini-dimension keeps the history of the bands at a bounded size, and the core customer dimension stops growing with every score change. Option A loses history that may matter; option C mixes descriptive data into facts and makes it hard to reuse.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What is an SCD? | A dimension attribute that changes over time, and the rule for how its history is stored |
| SCD Type 1 | Overwrite the old value; no history |
| SCD Type 2 | New row per version, with validity range and a current flag |
| SCD Type 3 | Extra column for the previous value; one step of history |
| Why a surrogate key on SCD2? | It identifies one version; the business key identifies the entity |
| Point-in-time join condition | `valid_from <= event_time < valid_to` |
| What is an inferred member? | A placeholder dimension row for a fact that arrived before its dimension record |
| Why `9999-12-31` in `valid_to`? | An open-ended "still valid" marker that keeps range joins simple |

## 🔗 Go further

- **dimensional-modeling** explains dimensions, facts and grain, which SCD depends on.
- **star-snowflake-obt** shows how the dimension shape changes the trade-offs.
- **pattern-history** covers history patterns beyond dimensions, such as event logs and audit tables.
- **lakehouse-table-formats** explains how MERGE behaves on Delta, Iceberg and similar table formats.
- **dbt** covers snapshots and tests for SCD2 validity invariants.
- The surrogate key and grain terms are in the glossary: [[surrogate-key]] and [[grain]].
