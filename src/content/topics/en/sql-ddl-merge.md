---
order: 28
tid: "sql-ddl-merge"
track: "2"
section: null
num: "2.13"
title: "DDL, Upserts & MERGE"
type: "concept"
level: "intermediate"
priority: "P0"
est_hours: 2
summary: "Table design, constraints, partitioned tables, and upserts via MERGE or INSERT ... ON CONFLICT. The basis of incremental loading."
covers: ["idempotent loads","staging-table swap pattern","truncate-insert vs delete-insert per partition"]
interview_qs: []
extras: []
prerequisites: ["sql-fundamentals", "sql-transactions"]
related: ["dedup-idempotent-loads", "pattern-dedup", "pattern-backfill", "partitioning-sharding", "scd", "schema-evolution", "sql-window-functions"]
---

## 🎯 TL;DR

> **DDL** (Data Definition Language) defines the structure: tables, keys, constraints, partitions. An **upsert** means "insert the row if it is new, update it if it exists". **MERGE** does both, in one statement, for a whole batch.

- Design the **key** first. A natural key (`customer_id`) or a surrogate key identifies a row; an upsert needs a unique constraint on it.
- Upserts are done with `INSERT ... ON CONFLICT` (PostgreSQL) or `MERGE` (SQL standard, supported by PostgreSQL 15+, BigQuery, Snowflake, Databricks and SQL Server).
- An **idempotent** load gives the same final table when you run it twice. This is the goal of every incremental pipeline.
- Two patterns make loads safe: **staging-table swap** (build the new data aside, then switch) and **partition overwrite** (rebuild a whole partition instead of patching rows).

## 🌱 Start simple (Junior)

> **💡 Analogy:** A guest list at a party. When a guest arrives, you check the list. If the name is new, you add it. If it is already there, you update the details (a new phone number). An upsert is exactly that: "add if new, update if known". The guest name is the key.

Start with a small table that a nightly job fills:

```sql
CREATE TABLE dim_customer (
  customer_id text PRIMARY KEY,         -- the natural key from the source system
  city        text,
  updated_at  timestamp NOT NULL
);
```

Tomorrow the source sends two kinds of rows: new customers and changed customers. A plain `INSERT` fails on the changed ones because the key already exists. An upsert handles both:

```sql
INSERT INTO dim_customer (customer_id, city, updated_at)
VALUES ('C1', 'Tehran', '2026-03-01 10:00')
ON CONFLICT (customer_id)
DO UPDATE SET city = EXCLUDED.city, updated_at = EXCLUDED.updated_at;
```

`EXCLUDED` means "the row you tried to insert". Run the statement again with the same values and nothing changes: that is the first taste of idempotency.

## 🔧 How it really works (Mid)

### Constraints and keys

| Constraint | What it guarantees | Note |
|---|---|---|
| `NOT NULL` | every row has a value | cheap, always useful |
| `PRIMARY KEY` | unique and not null; one per table | the target for upserts and foreign keys |
| `UNIQUE` | no two rows share the value | needed as the conflict target for `ON CONFLICT` |
| `CHECK` | a row-level rule, e.g. `amount >= 0` | enforced in OLTP engines |
| `FOREIGN KEY` | the referenced row exists | many warehouses declare it but do **not** enforce it |

In warehouses (BigQuery, Snowflake, Redshift) primary and foreign keys are often **informational**: the optimizer may use them, but the engine does not reject bad rows. Your pipeline's data-quality checks must do that job.

### Partitioned tables

```sql
CREATE TABLE events (
  event_time timestamptz NOT NULL,
  user_id    bigint NOT NULL,
  payload    jsonb
) PARTITION BY RANGE (event_time);

CREATE TABLE events_2026_01 PARTITION OF events
  FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

Each partition is a physical table. Queries that filter on `event_time` skip the others (pruning, see [[sql-query-optimization]]), and old data can be dropped with `DROP TABLE` on one partition instead of a huge `DELETE`. In PostgreSQL, a primary key on a partitioned table must include the partition key.

### INSERT ... ON CONFLICT

The conflict target needs a unique index or constraint. Inside one statement, a row cannot affect the same target row twice, so a batch with two rows for `C1` fails with a "cannot affect row a second time" error. Deduplicate the batch first.

### MERGE

```sql
MERGE INTO dim_customer AS t
USING staging_customer AS s
   ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET city = s.city, updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, city, updated_at)
  VALUES (s.customer_id, s.city, s.updated_at);
```

`MERGE` handles inserts, updates and (in many engines) deletes in one statement. The `AND s.updated_at > t.updated_at` guard makes it ignore stale source rows. Like `ON CONFLICT`, a source with duplicate keys is an error in PostgreSQL and SQL Server, and a silent non-deterministic choice in some other engines. MySQL has no `MERGE`; it uses `INSERT ... ON DUPLICATE KEY UPDATE`.

### Deduplicate before you upsert

The usual staging step keeps the latest version of each key:

```sql
SELECT *
FROM (
  SELECT s.*,
         ROW_NUMBER() OVER (PARTITION BY customer_id
                            ORDER BY updated_at DESC, source_seq DESC) AS rn
  FROM staging_customer s
) x
WHERE rn = 1;
```

The tiebreaker (`source_seq`) makes the choice deterministic. See [[pattern-dedup]].

### The two load patterns

**Staging-table swap.** Load the batch into `dim_customer_staging`, validate it (row counts, uniqueness, DQ checks), then switch the names inside one transaction:

```sql
BEGIN;
ALTER TABLE dim_customer RENAME TO dim_customer_old;
ALTER TABLE dim_customer_staging RENAME TO dim_customer;
COMMIT;
```

Readers see either the old table or the new one, never a half-loaded state. PostgreSQL DDL is transactional, so this works. Warehouses have similar operations (Snowflake's `ALTER TABLE ... SWAP WITH`).

**Partition overwrite.** For a daily fact table, rebuild the day's partition instead of patching it:

- *Truncate-insert per partition:* drop or truncate the day's partition, insert the rebuilt rows. Fast, no dead rows, and a re-run gives the same result.
- *Delete-insert per partition:* `DELETE` the day's rows, then insert. Works on a shared table, but in PostgreSQL the deleted rows stay as dead tuples until `VACUUM`, and the delete is slower on large partitions.

Spark's `INSERT OVERWRITE` with dynamic partition overwrite mode does the truncate-insert pattern for the partitions it writes.

## 🧠 Senior depth & trade-offs

- **Idempotency is the contract.** Every load should be safe to re-run after a failure, which means: a deterministic dedup, an upsert on a real unique key, or a whole-partition overwrite. If the job appends, a retry duplicates data. See [[idempotency]].
- **MERGE cost.** `MERGE` joins the target against the source. On a large target, make the `ON` clause include the partition key (`ON t.event_date = s.event_date AND t.id = s.id`) so the engine prunes the target. Otherwise it scans the whole table for a small batch.
- **Locks.** `ALTER TABLE` takes a strong lock in PostgreSQL. A rename-based swap is fast, but it waits for running transactions that touch the table, and those waits can block a queue of queries. Set a `lock_timeout` in the job and fail fast.
- **Swaps and dependencies.** After a rename, views, grants and foreign keys still point at the old object in PostgreSQL. Check dependent objects before you swap, or create the views in the swap step.
- **Late and updated data.** Upserts handle updates but not history. If you need the old values, the target must be an SCD table ([[scd]]), and the MERGE becomes "close the old row, insert the new one".
- **Schema changes.** In PostgreSQL 11 and later, `ADD COLUMN ... DEFAULT <constant>` is a metadata change, not a table rewrite. Adding a `NOT NULL` column without a default to a populated table fails; plan a nullable column, backfill in batches, then add the constraint. See [[schema-evolution]].
- **Engine differences.** Warehouses often charge by bytes scanned, so a MERGE that rewrites a whole partition has a real cost. Check your engine's MERGE and partition docs for your version; semantics for duplicate matches differ.

## 🧪 Try it

Use PostgreSQL 15 or later so you can run both `ON CONFLICT` and `MERGE`.

1. Create `dim_customer` (above) and a `staging_customer` table with columns `customer_id`, `city`, `updated_at`, `source_seq`. Insert two rows for `C2` with different `updated_at`. Write the dedup query that keeps the latest one.
2. Run an `INSERT ... ON CONFLICT` upsert for the deduplicated rows. Run it again and check the row count and `updated_at`.
3. Write the same load as a `MERGE` that ignores source rows older than the target. Then explain what happens if the staging table still has two rows for `C2`.

<details><summary>Show solutions</summary>

**1.** Use the dedup query from the Mid section with `ORDER BY updated_at DESC, source_seq DESC`. The row with the latest `updated_at` wins; `source_seq` only decides a tie.

**2.** `INSERT INTO dim_customer SELECT customer_id, city, updated_at FROM (deduplicated query) d ON CONFLICT (customer_id) DO UPDATE SET city = EXCLUDED.city, updated_at = EXCLUDED.updated_at;` The second run changes zero values, so the result is identical: idempotent.

**3.** Use the `MERGE` from the Mid section. With two `C2` rows in staging, PostgreSQL raises an error because one target row would be affected twice. The fix is the dedup step, not a bigger `ON` clause.

</details>

## 💬 Interview corner

**Question: "How do you make a nightly load idempotent?"** Mid answer: "I use an upsert on the primary key, so re-running doesn't insert duplicates." Senior answer: "The upsert is only idempotent if the batch is deduplicated deterministically first, and the key is a real unique key. For fact tables I overwrite the day's partition instead, so a retry replaces data rather than adding to it. I validate the staging data before the swap, and I keep the job's run ID so I can see what each run wrote."

**Question: "MERGE or INSERT ... ON CONFLICT?"** Use `ON CONFLICT` for a simple row-by-row upsert in PostgreSQL. Use `MERGE` when you need insert, update and delete logic together, or when the warehouse only offers `MERGE` for batch loads.

> **🎤 Say this in the interview:** "I design the load so it can be run twice. I deduplicate the batch deterministically, upsert on a real unique key, and overwrite whole partitions for facts. I validate in staging and swap atomically, so readers never see half-loaded data."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Upserting a batch with duplicate keys. The statement fails (PostgreSQL, SQL Server) or picks an arbitrary row (some engines). Always deduplicate first with an explicit ordering.

> **⚠️ Watch out:** Using `ON CONFLICT (col)` on a column without a unique index. PostgreSQL rejects it. The conflict target must match a unique constraint or index exactly.

- Using plain `INSERT` in a retried job, which duplicates rows on every retry.
- Assuming a `FOREIGN KEY` in a warehouse rejects bad rows. Often it is only documentation.
- Running `DELETE` on a huge partition and then wondering why the table stays large. In PostgreSQL the space comes back only after `VACUUM`.
- Forgetting that `ALTER TABLE` takes a strong lock and waits behind long transactions.
- Writing a `MERGE` without a guard on `updated_at`, so an old source row overwrites a newer target row.
- Adding a `NOT NULL` column without a default to a populated table.

## ✅ Check yourself (quiz)

1. A nightly job appends rows to a fact table. The job fails halfway and is retried. What is the most likely result, and what fix is best?
   - A) Nothing; the database rolls everything back, so only a plain append is needed
   - B) Duplicate rows for the part that already loaded; overwrite the partition or upsert on a unique key instead of appending
   - C) Missing rows; use `TRUNCATE` on every retry
   - D) Foreign key errors; drop the foreign keys

<details><summary>Answer</summary>

**B** — a retry of a plain append repeats the rows that already loaded. Overwriting the partition, or upserting on a unique key, makes the retry produce the same final table. A is wrong unless the whole job runs in one transaction, which large loads often cannot do. C loses data for the other partitions, and D does not address duplicates.

</details>

2. Which statement is the most accurate about `ON CONFLICT`?
   - A) It works without any unique index on the table
   - B) The conflict target must match a unique constraint or unique index
   - C) It can update the same target row twice in one statement
   - D) It is the standard SQL way to write upserts in every engine

<details><summary>Answer</summary>

**B** — PostgreSQL needs a unique constraint or index that matches the conflict target. A is false, C is explicitly an error, and D is wrong because `ON CONFLICT` is PostgreSQL-specific and `MERGE` is the standard form.

</details>

3. A `MERGE` runs on a 2-billion-row fact table for a 50,000-row daily batch, and it is slow. What is the first change to consider?
   - A) Remove the `WHEN NOT MATCHED` branch
   - B) Include the partition key in the `ON` condition so the engine can prune the target
   - C) Replace `MERGE` with a plain `INSERT`
   - D) Add more `WHEN MATCHED` branches

<details><summary>Answer</summary>

**B** — a join that includes the partition key lets the engine read only the target partitions the batch touches. A breaks the upsert logic, C creates duplicates on re-runs, and D adds logic without reducing the scan.

</details>

4. (Senior) Your staging-swap job renames `dim_customer` to `_old` and renames staging to `dim_customer`. Afterwards a reporting view fails. Why, and how do you prevent it?
   - A) Views cannot reference tables in PostgreSQL; the view should be dropped
   - B) The view depends on the table object, and the swap changed what the name points to; check dependent objects before the swap and recreate or repoint the views inside the same transaction
   - C) Renaming a table deletes its data
   - D) The view needs a `MERGE` instead

<details><summary>Answer</summary>

**B** — dependents such as views, grants and foreign keys are tied to the objects, and the name now refers to a different object. Check dependencies before the swap, and recreate or repoint them in the same transaction. A is false, C is false (a rename keeps data), and D is unrelated.

</details>

5. (Senior, trade-off) For a daily partition of 400 million rows, which approach gives a safe re-run with the least long-term table bloat in PostgreSQL?
   - A) `DELETE` the day's rows and insert, then never vacuum
   - B) Build the day's rows in a staging table, validate, then attach it as the partition (or drop and recreate the partition) in one transaction
   - C) Insert the rows again without deleting anything
   - D) Use `SELECT *` into a new table every hour

<details><summary>Answer</summary>

**B** — building the partition aside and swapping it in leaves no dead rows and makes a re-run replace the data cleanly. A creates dead tuples and bloat, C duplicates rows, and D does not handle the partition's identity or the swap.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What is an upsert? | Insert the row if the key is new, update it if the key exists |
| What is `EXCLUDED` in `ON CONFLICT`? | The row that was proposed for insertion |
| What does `MERGE` do? | Insert, update (and optionally delete) rows in one statement, matching source to target on a condition |
| What does a unique constraint give an upsert? | A conflict target: the engine can detect that a row already exists |
| What is idempotent loading? | Running the same load again leaves the table in the same final state |
| What is the staging-table swap? | Build and validate the new table aside, then switch the names atomically |
| Truncate-insert vs delete-insert per partition? | Rebuild the whole partition (no dead rows) vs delete some rows and insert (leaves dead rows in PostgreSQL) |
| Why are warehouse foreign keys often not enforced? | Enforcing them costs time and the engine is built for bulk loads; the pipeline must check integrity |

## 🔗 Go further

- Prerequisites: [[sql-fundamentals]] (DML basics) and [[sql-transactions]] (why the swap must be atomic).
- Related: [[dedup-idempotent-loads]] and [[pattern-dedup]] (the full dedup strategy), [[pattern-backfill]] (re-running history safely), [[scd]] (history-preserving MERGE), [[partitioning-sharding]] (how partitions are chosen).
- Check the MERGE and partition docs for your engine and version (PostgreSQL "MERGE", "Table Partitioning"; your warehouse's DML reference).
