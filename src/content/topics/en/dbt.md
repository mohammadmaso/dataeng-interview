---
order: 125
tid: "dbt"
track: "11"
section: null
num: "11.4"
title: "dbt Deep Dive"
type: "technology"
level: "advanced"
priority: "P0"
est_hours: 5
summary: "SQL transformation with software engineering: models, ref and source, materializations (view, table, incremental, ephemeral), tests, snapshots for SCD2, macros and Jinja, docs and lineage. Adapters for ClickHouse, StarRocks, Spark and Trino."
covers: ["incremental strategies (append, merge, delete+insert, insert_overwrite)","model layering (staging → intermediate → marts)","CI with slim builds"]
interview_qs: []
extras: []
prerequisites: ["sql-fundamentals", "sql-ddl-merge"]
related: ["dimensional-modeling", "scd", "data-quality", "dataops-cicd", "dedup-idempotent-loads", "medallion-architecture", "airflow", "semantic-layer"]
---

## 🎯 TL;DR

> **dbt** (data build tool) lets you write the **T** of ELT as SQL `SELECT` statements in files. dbt turns each file into a table or view, works out the order from the references between them, runs tests, and generates documentation and lineage. It does not move data; the warehouse does the work.

- A **model** is one `.sql` file that holds a `SELECT`. dbt wraps it in `CREATE VIEW`, `CREATE TABLE`, or an incremental load, depending on its **materialization**.
- `{{ ref('model') }}` and `{{ source('system', 'table') }}` create the dependency graph. You never hard-code the schema or the build order.
- **Tests** (unique, not null, accepted values, relationships) and **snapshots** (history of changing rows, SCD Type 2) sit next to the models they check.
- The senior topics are **incremental strategies** (how a model handles only new data safely), **layering** (staging, intermediate, marts), and **CI with state** (build and test only what changed).

## 🌱 Start simple (Junior)

> **💡 Analogy:** A set of recipes in a cookbook, where each recipe says which other recipes it needs ("make the stock first"). A tool reads the cookbook, works out the order, cooks each dish in the right order, and checks each dish against a list of rules before serving it.

Without dbt, a warehouse often has a folder of SQL scripts, a schedule that runs them in the right order, and nobody sure which script feeds which report. With dbt, the dependency is written inside the SQL:

```sql
-- models/staging/stg_orders.sql
select
    order_id,
    customer_id,
    cast(order_ts as timestamp) as order_ts,
    lower(status) as status,
    amount_cents / 100.0 as amount
from {{ source('shop', 'raw_orders') }}
```

```sql
-- models/marts/customer_revenue.sql
select
    customer_id,
    count(*) as orders,
    sum(amount) as revenue
from {{ ref('stg_orders') }}
where status = 'shipped'
group by customer_id
```

Two things are new:

- `{{ source(...) }}` points at a raw table that dbt does not build. You declare it once in a YAML file.
- `{{ ref(...) }}` points at another model. dbt uses it to know that `customer_revenue` must run after `stg_orders`, and it replaces the name with the correct schema and table in your warehouse.

Run `dbt build` and dbt creates both models in order, then runs any tests you attached.

## 🔧 How it really works (Mid)

**Compilation.** When you run a command, dbt reads your project, renders the Jinja (`{{ ... }}` and `{% ... %}`), resolves every `ref` and `source` to a real relation name, and builds a DAG. Then it sends SQL to the warehouse in dependency order. You can inspect the result in `target/compiled/`, which is the first thing to check when a model misbehaves.

**Materializations.** The `materialized` config decides what is created:

| Materialization | What the warehouse gets | Use it when |
|---|---|---|
| `view` | A view; the query runs at read time | Small, cheap, or rarely queried models |
| `table` | A full rebuild each run | Small-to-medium tables, or when a rebuild is cheap |
| `incremental` | Created once, then new rows are added or merged each run | Large fact tables |
| `ephemeral` | Not created; injected as a CTE into the models that use it | Helper logic you do not need to query |

**Incremental models.** On the first run, the model builds the full table. On later runs, `is_incremental()` is true, and the model's query only selects the new or changed rows. `{{ this }}` refers to the existing table:

```sql
-- models/marts/fct_orders.sql (PostgreSQL-compatible dialect)
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id'
) }}

select order_id, customer_id, order_ts, status, amount
from {{ ref('stg_orders') }}
{% if is_incremental() %}
  -- look back 3 days so late-arriving updates are picked up
  where order_ts >= (select max(order_ts) from {{ this }}) - interval '3 days'
{% endif %}
```

**Incremental strategies.** The strategy decides how the new rows are written. Exact availability depends on the adapter, so check your adapter's docs:

- **append**: insert the new rows. Fast and simple, but it duplicates rows if a window is processed twice. Use it only for truly immutable events with a dedup step elsewhere.
- **merge**: upsert on `unique_key`. Rows that match are updated, others are inserted. The usual choice when rows can change.
- **delete+insert**: delete the rows that match the new batch's keys, then insert the batch. Useful where `MERGE` is slow or unavailable.
- **insert_overwrite**: replace whole partitions that the new data touches. Very efficient on partitioned storage (BigQuery, Spark-based engines), and idempotent by construction when partitions are complete.

**Tests.** Generic tests are declared in YAML and compiled to SQL. A test passes when its query returns no rows:

```yaml
# models/marts/schema.yml
version: 2
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'cancelled']
```

A singular test is a `.sql` file in `tests/` that returns the rows that break a rule. Packages such as `dbt_utils` add more generic tests.

**Snapshots.** A snapshot captures how a mutable source table changes over time, producing SCD Type 2 history (one row per version, with validity dates). dbt detects a change either from a timestamp column or by comparing selected columns:

```sql
{% snapshot customers_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}
select * from {{ source('shop', 'customers') }}
{% endsnapshot %}
```

Snapshots are a separate command (`dbt snapshot`) and are often scheduled on their own. See [[scd]].

**Macros and Jinja.** A macro is a reusable function written in Jinja that returns SQL text:

```sql
-- macros/cents.sql
{% macro cents_to_dollars(column_name) %}
    ({{ column_name }} / 100.0)
{% endmacro %}

-- used in a model
select {{ cents_to_dollars('amount_cents') }} as amount from {{ source('shop', 'raw_orders') }}
```

Use macros to avoid copy-pasted logic, not to hide business rules. A reviewer must still be able to read the SQL that runs.

**Docs and lineage.** `dbt docs generate` produces a site with descriptions, columns and a lineage graph built from `ref` and `source`. The lineage is the same graph dbt used to run the project, so it stays accurate as long as people use `ref`.

**Adapters.** dbt compiles to the dialect of the warehouse through an adapter. Adapters exist for many engines, including ClickHouse, StarRocks, Spark and Trino (community-maintained for some). The model SQL is portable only as far as the dialects agree; materializations, incremental strategies and functions differ per adapter.

## 🧠 Senior depth & trade-offs

- **Incremental correctness is the hard part.** The filter `where ts > (select max(ts) from {{ this }})` misses rows that arrive late with an older timestamp. A lookback window fixes this, but it re-processes rows, so the strategy must be idempotent (`merge` or `insert_overwrite`). The trade-off is cost against completeness. Measure how late data really arrives before choosing the window. See [[late-arriving-data]].
- **Full refresh is a valid fallback.** When logic changes, run `dbt run --full-refresh` for that model (or rebuild its partitions). A model that can only be built incrementally is a model that cannot be repaired easily; design for a rebuild.
- **Layering keeps the graph sane.** A common structure is:
  - **staging**: one model per source table, renaming and casting only, no joins;
  - **intermediate**: reusable joins and business logic, not exposed to users;
  - **marts**: the tables analysts and BI tools query, shaped for the questions (see [[dimensional-modeling]]).

  Layering stops marts from reading raw tables directly, so a source change touches one staging model instead of twenty reports.
- **Model grain and keys.** Every mart should declare its grain (one row per order, per customer per day) and test the key. Most data-quality incidents in dbt projects come from a join that multiplies rows; a `unique` test on the key catches it before a dashboard does. See [[data-quality]].
- **CI with state (slim builds).** Running the whole project on each pull request is slow and expensive. Keep a copy of the production manifest, then build only what changed and what depends on it:

  ```bash
  # in CI: compare with the manifest saved from the last production run
  dbt build --select state:modified+ --defer --state ./prod-artifacts
  ```

  `state:modified+` selects changed models and everything downstream. `--defer` makes unchanged upstream references point at production tables, so a PR does not need the whole graph built. Be aware that changes to macros or config can affect many models, and that a clean CI run still needs a schema per PR to avoid collisions. See [[dataops-cicd]].
- **Cost and warehouse behaviour.** A `view` on a large join gets recomputed by every reader. A `table` rebuilt daily on a billion-row source wastes money. Choose materializations from the read pattern and the rebuild cost, and check the warehouse's query profile before blaming dbt.
- **Snapshots at scale.** Snapshots compare every row on every run. On a large, wide source, prefer the timestamp strategy and make sure the source column is reliable. A snapshot is only as good as the source's `updated_at`.
- **When not to use dbt.** dbt is for SQL-shaped transformations inside a warehouse or lakehouse. Row-level stream processing, heavy Python feature engineering, or a pipeline whose logic is mostly API calls belongs elsewhere (see [[flink]] or [[airflow]]). dbt can still run as one task in an orchestrator.
- **What to monitor.** Test failures and their trend, run duration per model, how many rows an incremental run writes compared with the last run (a sudden spike or zero is a signal), and freshness of sources.

## 🧪 Try it

Use a PostgreSQL-compatible warehouse, or any engine where the SQL below runs unchanged.

1. **Write a staging model.** Given a source `raw_orders` with columns `id`, `cust`, `ts_text` (a string timestamp), and `status_code` (1 = placed, 2 = shipped, 3 = cancelled), write `stg_orders` with proper names, a timestamp type, and a decoded `status` column.

2. **Choose a strategy.** A `fct_payments` table receives payment status updates. A payment can move from `pending` to `captured` days later. Pick the incremental strategy and write the `config()` block, then explain why `append` is wrong.

3. **Find the test gap.** A mart has `order_id` and joins to `customers`. Which two generic tests would catch the most common join bugs, and how would you write one for the relationship?

<details><summary>Show solutions</summary>

**1.**
```sql
select
    id as order_id,
    cust as customer_id,
    cast(ts_text as timestamp) as order_ts,
    case status_code
        when 1 then 'placed'
        when 2 then 'shipped'
        when 3 then 'cancelled'
    end as status
from {{ source('shop', 'raw_orders') }}
```
The `case` statement decodes the code in one place, so every mart reads the same meaning.

**2.**
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id'
) }}
```
`append` is wrong because the same payment appears again with a new status; it would produce two rows for one payment. `merge` on `payment_id` updates the existing row. Combine it with a lookback window, because a payment can change days after it was created.

**3.** `unique` and `not_null` on `order_id` catch duplicated rows from a fan-out join. For the relationship, use the `relationships` test so every `customer_id` in the mart exists in `customers`:
```yaml
- name: customer_id
  tests:
    - relationships:
        to: ref('customers')
        field: customer_id
```
The `relationships` test catches orphaned keys, which otherwise drop silently in an inner join.

</details>

## 💬 Interview corner

**"What is the difference between a view, a table, and an incremental model?"**
- **Mid answer:** "A view runs the query at read time, a table is rebuilt each run, and an incremental model only adds or changes new rows."
- **Senior answer:** "It's a cost and correctness trade-off. A view costs nothing to build but every reader pays the compute. A table is simple and correct but costs a full rebuild each run. An incremental model is cheapest on large data, but it needs a key, a strategy that is idempotent, and a lookback for late data. I choose by volume, read pattern and how often the logic changes, and I plan the full-refresh path before I need it."

**"How do you run dbt in CI?"** Build only what changed, and everything downstream of it, using state comparison against the last production manifest. Defer unchanged upstream models to production. Run tests on the built models. Give each pull request its own schema so runs do not collide.

**"Why staging, intermediate and marts?"** Each layer has one job. Staging isolates the source from the rest of the project. Intermediate holds reusable logic. Marts expose stable tables to users. A source schema change then affects one place.

> **🎤 Say this in the interview:** "dbt gives me SQL models with explicit dependencies, tests and lineage. I pick the materialization from the volume and read pattern, I make incremental models idempotent with a key and a lookback window, and I use layering and slim CI so a change only rebuilds what it touches."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Hard-coding schema or table names instead of `ref` and `source`. The graph breaks, lineage is wrong, and CI cannot select changes correctly.

> **⚠️ Watch out:** Using `is_incremental()` filters on a timestamp without a lookback. Late rows are silently lost, and nobody notices until the totals disagree with the source.

- Choosing `append` for mutable data, which produces duplicates.
- Building every model as a `table` on a large source, then wondering why the nightly run takes hours.
- Putting business logic in staging, so every downstream model repeats it.
- Skipping the `unique` test on a mart key, so a fan-out join goes unnoticed.
- Forgetting that `dbt test` results are only as good as the tests you declare; no tests means no safety net.
- Treating a macro as a black box. A macro that hides a filter makes incidents harder to debug.

## ✅ Check yourself (quiz)

1. What does `{{ ref('stg_orders') }}` do that a plain table name does not?
   - A) It makes the query run faster
   - B) It creates a dependency edge in the DAG and resolves to the correct relation for the target environment
   - C) It caches the result
   - D) It copies the data into the model

<details><summary>Answer</summary>

**B** — `ref` tells dbt the build order and lineage, and it resolves to the right schema and name, so the same project runs in dev and prod. A hard-coded name gives neither.

</details>

2. An incremental model uses `merge` on `order_id`. A row changes status two days after it was first loaded. What is the most likely problem if the filter is `where order_ts > (select max(order_ts) from {{ this }})`?
   - A) `merge` cannot update rows
   - B) The filter only picks rows newer than the last loaded timestamp, so the updated row is never reprocessed
   - C) `unique_key` must be a timestamp
   - D) The model must be a view

<details><summary>Answer</summary>

**B** — the changed row has an old `order_ts`, so the filter skips it. The fix is a lookback window, or filtering on an `updated_at` column that the source maintains.

</details>

3. A pull request changes one macro used by 40 models. Which CI approach is most efficient and still safe?
   - A) Build the whole project on every commit
   - B) Build only the changed model and nothing else
   - C) Use `state:modified+` with `--defer` against the production manifest, so the changed macro's dependents are rebuilt and tested
   - D) Skip CI for macro changes

<details><summary>Answer</summary>

**C** — slim CI builds the changed nodes and their dependents, and `--defer` uses production tables for the rest. Building only the changed model misses the 40 dependents that the macro change may affect.

</details>

4. **(Senior)** A `fct_orders` incremental model has grown to 2 billion rows and each run takes four hours. Which change is the best next step, and what must it guarantee?
   - A) Switch to a `view`, so nothing is stored
   - B) Use a partition-aware strategy such as `insert_overwrite` for the recent partitions (if the adapter supports it), with a lookback window that covers the late data, and verify that reruns are idempotent
   - C) Remove the `unique_key`
   - D) Run the model as `ephemeral`

<details><summary>Answer</summary>

**B** — rebuilding only recent partitions reduces work, and the lookback covers late data. The guarantee to check is idempotence: rerunning a window must produce the same table. A view would make every reader recompute four hours of logic, and `ephemeral` does not store anything.

</details>

5. **(Senior)** You need full SCD Type 2 history for a customer table that is updated in place. Why is an incremental model the wrong tool, and what is the right one?
   - A) Incremental models cannot read sources
   - B) An incremental model keeps only the latest state per key, so the old versions are overwritten; a snapshot records each version with validity dates
   - C) Snapshots cannot use timestamps
   - D) Incremental models always append history

<details><summary>Answer</summary>

**B** — a merge keeps one current row per key. A snapshot is built for history: each change creates a new row with a start and end, which is SCD Type 2. Use [[scd]] concepts to choose the variant you need.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| `ref()` | Points to another model; creates a DAG edge and resolves to the right relation per environment |
| `source()` | Points to a raw table declared in YAML; dbt does not build it |
| `is_incremental()` | True on runs after the first; the filter inside it selects only new or changed rows |
| `unique_key` | The key used by merge or delete+insert to update existing rows instead of duplicating them |
| `ephemeral` | Not created in the warehouse; injected as a CTE into the models that use it |
| Snapshot | Records a changing source table over time (SCD Type 2) using a timestamp or check strategy |
| `state:modified+` | Selects changed models and everything downstream; used for slim CI with `--defer` |
| Lookback window | Reprocesses a recent period in an incremental model to catch late-arriving rows |

## 🔗 Go further

- [[dimensional-modeling]] and [[scd]] explain the mart shapes and history patterns dbt usually builds.
- [[dedup-idempotent-loads]] covers why incremental strategies must be repeatable.
- [[data-quality]] and [[dataops-cicd]] go deeper on tests and delivery pipelines around dbt.
- [[airflow]] is a common way to schedule dbt runs as tasks in a larger pipeline.
- [[semantic-layer]] covers the metric layer that often sits on top of dbt marts.
- Check the documentation for your adapter. Materialization names, incremental strategies and SQL functions differ between ClickHouse, StarRocks, Spark, Trino and cloud warehouses.
