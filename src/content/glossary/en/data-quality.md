---
term: "Data quality"
aliases: ["data quality checks", "data validation", "data tests", "great expectations", "dbt tests", "data quality dimensions"]
category: quality
short: "How well data is fit for use, measured by checks such as completeness, validity, uniqueness, freshness, accuracy and consistency."
related: ["data-contract", "data-observability", "lineage"]
---
## In plain words (Junior)
Cooking with bad ingredients gives a bad meal, however good the recipe is. Data quality is checking the ingredients: are any missing, spoiled, duplicated or old? If checked at the door, problems are caught before they reach the customers (reports and models).

## How it works (Mid)
Common dimensions: completeness (no unexpected nulls/missing rows), validity (types, ranges, formats, enums), uniqueness (no duplicates on keys), consistency (same fact agrees across tables, referential integrity), accuracy (matches reality; hardest to test), and timeliness/freshness (arrived on time). Checks are implemented as tests in the pipeline: dbt tests, Great Expectations, Soda, Deequ, plus row-count and volume anomaly checks and reconciliation against sources.

```
# dbt tests (schema.yml)
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: customer_sk
        tests: [{relationships: {to: ref('dim_customer'), field: customer_sk}}]
      - name: status
        tests: [{accepted_values: {values: ['NEW','PAID','CANCELLED']}}]
    tests:
      - dbt_utils.recency: {datepart: hour, field: loaded_at, interval: 6}   # freshness
```

Where to place them: at ingestion (schema/contract validation, quarantine bad rows), between layers (bronze to silver), and before publishing (blocking gates in the DAG so bad data isn't exposed). Decide per check: fail the pipeline (block), warn, or quarantine rows.

## Senior angle & interview tips
Quality at scale needs ownership and severity: not every failing check should page someone or block a pipeline. Use thresholds and anomaly detection (volume, null rate, distribution drift) for the unknown unknowns, not just static rules. Write-audit-publish pattern: write to a staging branch/table, audit, then atomically publish (Iceberg branches, partition swap). Metrics: pass rate, time-to-detect, time-to-resolve, incident count by source. Root causes are usually upstream changes, so shift left with data contracts. Quality tests also guard against silently wrong joins (fan-out), the most common correctness bug.

Interview question: "How do you ensure data quality in a pipeline?" Model answer: define expectations with owners (uniqueness, not-null, referential integrity, freshness, volume), test them at ingestion and between layers, use write-audit-publish so failures block exposure, alert with severity, and track quality metrics, while contracts prevent upstream breakage.
