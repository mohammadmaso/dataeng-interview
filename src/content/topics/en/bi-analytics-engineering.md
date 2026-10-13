---
order: 186
tid: "bi-analytics-engineering"
track: "15"
section: null
num: "15.4"
title: "BI & Analytics Engineering"
type: "technology"
level: "intermediate"
priority: "P2"
est_hours: 2
summary: "Superset and Metabase (common in Iran), the analytics engineer role, and designing marts for BI."
covers: []
interview_qs: []
extras: []
prerequisites: ["dimensional-modeling", "dbt"]
related: ["semantic-layer", "star-framework", "fact-table-types", "sql-window-functions", "data-lineage"]
---

## 🎯 TL;DR

> **Business intelligence (BI)** tools let people explore data through charts, dashboards and filters. **Analytics engineering** prepares the data those tools read: clean tables, agreed definitions of metrics, and models shaped for questions people ask.

- A BI tool is only as good as its **marts** (curated tables) and its **metric definitions**. A dashboard that disagrees with another dashboard is almost always a modelling problem, not a chart problem.
- **Apache Superset** and **Metabase** are open-source BI tools that many Iranian teams use, because they are free to self-host. Tableau and Looker are commercial alternatives; Looker adds a modelling layer (LookML).
- The **analytics engineer** sits between data engineering (pipelines, infrastructure) and analysis (questions, insight). Their core deliverable is tested, documented models.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A restaurant kitchen and the dining room. The data engineer builds the kitchen (pipes, storage, ovens). The analytics engineer prepares the ingredients into standard dishes with recipes everyone follows (marts and metrics). The BI tool is the menu that guests order from.

Suppose the business asks: "What were our sales per city last month?" The raw data is spread over an orders table, a customers table and a payments table. Without preparation, each analyst writes a different join, and each gets a different answer. A **mart** solves this: one table that already has the right join, the right filter and the right definition of "sale".

```sql
-- A small mart: one row per order, with the customer's city attached
CREATE TABLE mart_orders AS
SELECT
  o.order_id,
  o.order_date,
  c.city,
  o.amount_irr,
  o.status
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.status = 'paid';          -- one agreed definition of a "sale"
```

Now the BI tool needs one simple query: `SUM(amount_irr) GROUP BY city, month`. The logic lives in the mart, not in every chart.

## 🔧 How it really works (Mid)

A typical BI stack has four layers:

```mermaid
flowchart LR
    RAW["Raw tables<br/>landed by ingestion"] --> STG["Staging<br/>clean, rename, type"]
    STG --> MART["Marts<br/>facts and dimensions"]
    MART --> SEM["Metric definitions<br/>semantic layer"]
    SEM --> BI["BI tool<br/>charts, dashboards"]
```

**Marts** are curated tables, usually in a star schema: a **fact table** (events or measurements, one row per order line or per day) and **dimension tables** (customers, products, dates). See [[star-schema]], [[fact-table]] and [[dimension-table]]. A mart is named for the business area (`mart_sales`, `mart_support`), has a documented grain, and has tests (unique keys, not-null, referential checks).

A small star for daily sales:

```sql
-- fact: one row per order line
CREATE TABLE fct_order_line (
  order_id    bigint,
  line_no     int,
  product_key int,
  date_key    int,          -- YYYYMMDD
  quantity    int,
  amount_irr  bigint,
  PRIMARY KEY (order_id, line_no)
);

-- dimension: one row per product
CREATE TABLE dim_product (
  product_key int PRIMARY KEY,
  name        text,
  category    text
);

-- query a BI tool would generate for "sales by category, by month"
SELECT d.category,
       substr(CAST(f.date_key AS text), 1, 6) AS yyyymm,
       SUM(f.amount_irr) AS revenue_irr
FROM fct_order_line f
JOIN dim_product d ON d.product_key = f.product_key
GROUP BY d.category, yyyymm
ORDER BY yyyymm, category;
```

**Metric definitions.** A metric such as "active customer" or "gross margin" must be defined once. Options are:

- in the **transformation layer**, as a column in a mart (simple, testable, but duplicated if many marts need it);
- in a **semantic layer** (Looker's LookML, dbt's metrics, or a metrics service), where BI tools read the definition instead of re-deriving it;
- in the **BI tool** itself (fast to start, dangerous at scale, because each dashboard can drift).

**Superset and Metabase.** Both connect to SQL databases through drivers, let users build charts and dashboards, and let them write SQL. Superset is an Apache project with a richer set of chart types and a SQL Lab editor, and it is typically run by data platform teams. Metabase focuses on a simpler question-building interface and is often adopted by smaller teams and by business users. Both support caching and role-based access; check the documentation of your version for the exact permission model and supported databases.

**Looker** (Google Cloud) uses LookML, a modelling language in which you define tables, joins, dimensions and measures once, and users explore them without writing SQL. **Tableau** is strong at visual exploration and is widely used in enterprises; its semantic modelling is less centralized than Looker's. Both appear in interviews as "what do you know about BI tools?" questions, so know their model differences, not feature lists.

**The analytics engineer's workflow** (in dbt or similar tools): write SQL models, document columns, add tests, review in pull requests, deploy on a schedule, and check lineage from the dashboard back to the source table. See [[dbt]] and [[data-lineage]].

## 🧠 Senior depth & trade-offs

- **Grain first.** Every mart must have a stated grain ("one row per order line per day"). Mixed grains in one table produce double-counted revenue. Declare the grain in the model description and test uniqueness on the key.
- **Slowly changing dimensions.** If a customer changes city, historical sales should usually stay with the city at the time of the sale. That requires an SCD type 2 dimension or a snapshot. See [[scd]].
- **Fan-out and chasm traps.** Joining two fact tables at different grains multiplies rows and inflates sums. Aggregate each fact to the common grain first, then join.
- **Performance.** Dashboards issue many queries with filters on dates and dimensions. Pre-aggregated marts (daily rollups), partitioning by date, and columnar engines (ClickHouse, StarRocks, warehouses) reduce latency. Set cache TTLs with the business: a dashboard that refreshes every five minutes is rarely necessary for monthly finance numbers.
- **Governance.** Decide who can see which rows (row-level security) and which columns (personal data, salary). Enforce it in the database or semantic layer, not only by hiding dashboards. See [[data-masking]].
- **Adoption.** The most common failure is not technical: nobody trusts the numbers. Assign owners to metrics, publish definitions in a catalog, and keep one "certified" dashboard per key metric. See [[data-catalog]].
- **Self-hosting in Iran.** Self-hosted Superset or Metabase against an on-prem warehouse keeps data inside the network, which matters for banks and telcos. Plan backups for the BI metadata database as well as the warehouse, and keep the BI version updated for security fixes.
- **When not to put logic in the BI tool.** Anything that must be reused, tested or audited belongs in the transformation layer. Keep dashboards for presentation and exploration.

## 🧪 Try it

Using the star above, plus a second fact table `fct_refund(order_id, line_no, date_key, refund_irr)`:

1. Write a query that returns revenue and refunds by category and month **without** double-counting. Explain why a direct join of both facts on `order_id` would be wrong.
2. A dashboard shows monthly revenue of 1,000 but the finance report shows 1,050. List three causes you would check first in the mart layer.
3. Design the grain statement and two data tests for a `mart_daily_product_sales` table.

<details><summary>Show solutions</summary>

**1.** Aggregate each fact to the common grain (category, month) first, then combine the two results, for example with a `FULL OUTER JOIN` on category and month. Joining the raw facts on `order_id` would multiply each order line by the number of refund rows, inflating revenue.

```sql
WITH rev AS (
  SELECT d.category, substr(CAST(f.date_key AS text),1,6) AS yyyymm,
         SUM(f.amount_irr) AS revenue_irr
  FROM fct_order_line f JOIN dim_product d USING (product_key)
  GROUP BY 1, 2
),
ref AS (
  SELECT d.category, substr(CAST(r.date_key AS text),1,6) AS yyyymm,
         SUM(r.refund_irr) AS refund_irr
  FROM fct_refund r
  JOIN fct_order_line l ON l.order_id = r.order_id AND l.line_no = 1
  JOIN dim_product d ON d.product_key = l.product_key
  GROUP BY 1, 2
)
SELECT COALESCE(rev.category, ref.category) AS category,
       COALESCE(rev.yyyymm, ref.yyyymm)     AS yyyymm,
       rev.revenue_irr, ref.refund_irr
FROM rev FULL OUTER JOIN ref
  ON rev.category = ref.category AND rev.yyyymm = ref.yyyymm;
```
The `line_no = 1` join is a simplification for this exercise: in a real model, the refund table should carry the product key or the line key itself.

**2.** Check: (a) the filter (paid versus all orders, or refunds netted or not); (b) time zone or date boundary (order date versus payment date, UTC versus local); (c) duplicated rows from a join fan-out or from a re-run of the load.

**3.** Grain: "one row per product per calendar day, in local business time". Tests: `unique` on `(product_key, sale_date)`, and `not_null` on `sale_date` and `revenue_irr`. A third useful test is a row-count or total comparison with the source for the same day.

</details>

## 💬 Interview corner

**Question: "Two dashboards show different revenue for the same month. What do you do?"** Mid answer: "I compare the SQL behind both charts and find the filter that differs." Senior answer: "I first check the metric definition and the grain in each mart, then trace both dashboards back through lineage to the source. I make the definition single-sourced in the semantic or transformation layer, add a reconciliation test, and mark one dashboard as certified."

**Question: "Superset or Metabase, or Looker?"** Name the trade-offs: self-hosted and free versus managed; SQL-first versus question-builder; centralized LookML modelling versus model-in-the-BI-tool. Say which fits a team of five versus five hundred.

**Question: "What is an analytics engineer?"** Someone who models and tests data for analysis: builds marts, owns metric definitions, writes documentation and tests, and works with both engineers and business users.

> **🎤 Say this in the interview:** "I treat the BI layer as a presentation layer. The correctness lives in marts with a declared grain, tested keys, and single-sourced metric definitions. When two numbers disagree, I trace lineage before I touch a chart."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Defining the same metric separately in several dashboards. Each copy drifts, and nobody knows which one is right.

> **⚠️ Watch out:** Joining two fact tables at different grains and summing the result. The sum looks plausible and is wrong.

- Building marts that mirror source tables one to one, so every analyst still joins everything.
- Skipping the grain statement, then mixing daily and monthly rows in one table.
- Using a live query for a dashboard that is viewed by 500 people every morning, without caching or pre-aggregation.
- Granting dashboard access as the only security control, while the database remains open.
- Treating the BI tool's own model as the source of truth for business logic that must be tested.

## ✅ Check yourself (quiz)

1. What is the main purpose of a mart in an analytics stack?
   - A) To replace the source database
   - B) To provide curated, documented tables shaped for business questions
   - C) To store raw logs in a streaming format
   - D) To schedule BI dashboards

<details><summary>Answer</summary>

**B** — a mart is a curated table (often a star schema) with agreed definitions and a stated grain. It does not replace sources or handle streaming, and scheduling is a separate concern.

</details>

2. A fact table has one row per order line. Which query pattern is the most likely to double-count revenue?
   - A) Summing revenue after grouping by the order line
   - B) Joining it to another fact at the order grain and then summing its revenue
   - C) Filtering by a date range
   - D) Joining it to a dimension on a unique key

<details><summary>Answer</summary>

**B** — when a fact is joined to another fact at a finer or different grain, each row is repeated, and sums inflate (a fan-out). Aggregate each fact to the common grain first.

</details>

3. Which tool defines business metrics once in a modelling language that BI users explore without writing SQL?

   - A) Looker (LookML)
   - B) Superset SQL Lab
   - C) A CSV export
   - D) A Kafka topic

<details><summary>Answer</summary>

**A** — LookML is Looker's modelling layer, where tables, joins, dimensions and measures are defined once. SQL Lab is a query editor, and the other options are not modelling tools.

</details>

4. **(Senior)** A customer moves from Tehran to Shiraz. A dashboard of 2025 revenue by city now shows the 2025 sales in Shiraz. What is the likely design gap and the fix?

   - A) The chart type is wrong
   - B) The customer dimension overwrites history; use an SCD type 2 dimension or snapshot so each sale keeps the city valid at sale time
   - C) The BI tool needs more memory
   - D) Revenue should not be grouped by city

<details><summary>Answer</summary>

**B** — with an overwriting (type 1) dimension, history is rewritten. Type 2 keeps validity periods, so historical sales join to the city that was current then.

</details>

5. **(Senior)** When would you move a metric from a dashboard into the transformation layer?

   - A) Never; dashboards are the only correct place for metrics
   - B) When several dashboards or reports need it, when it must be tested, or when it must be audited
   - C) Only for real-time metrics
   - D) When the metric is a percentage

<details><summary>Answer</summary>

**B** — reuse, testing and audit are the reasons to centralize a definition. Dashboards are fine for presentation and exploration, but duplicated business logic drifts.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Mart | Curated, documented table shaped for a business area |
| Grain | What one row in a table represents (for example, one order line per day) |
| Fan-out trap | Joining facts at different grains repeats rows and inflates sums |
| Analytics engineer | Builds and tests marts and metric definitions between engineering and analysis |
| Superset | Apache open-source BI tool with SQL Lab and rich charts |
| Metabase | Open-source BI tool with a simple question-builder interface |
| LookML | Looker's modelling language for tables, joins, dimensions and measures |
| SCD type 2 | Dimension history kept with validity periods so past facts keep their old attributes |

## 🔗 Go further

- **dimensional-modeling** explains the star schema, grains and dimension design that marts are built on.
- **dbt** shows how analytics engineers write, test and document models in practice.
- **semantic-layer** covers centralized metric definitions in more depth.
- **star-framework** and **fact-table-types** give the modelling vocabulary used in interviews.
- **sql-window-functions** is the tool for "share of total" and "running total" metrics that dashboards often need.
- Check the documentation of your BI tool version for supported databases, caching and permission features.
