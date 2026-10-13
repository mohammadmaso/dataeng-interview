---
term: "Data lineage"
aliases: ["lineage", "column-level lineage"]
category: quality
short: "A record of where data came from and how it was transformed on its way to a table, report or model."
related: ["data-contract", "dag", "data-mesh"]
---
## In plain words (Junior)
When you buy food you can look at the label and see where it was made and what went into it. Data lineage is that label for data. If a dashboard number looks wrong, lineage lets you trace it back step by step to the source table or job that caused the problem, and also tells you who would be affected if you change something.

## How it works (Mid)
Lineage is a graph: nodes are datasets, jobs and columns; edges are "produced from" relationships. It answers two questions: upstream (where did this come from, for debugging and audit) and downstream (what breaks if I change it, for impact analysis).

```
raw.orders ──► stg_orders ──► fct_orders ──► revenue_dashboard
raw.users  ──► stg_users  ──► dim_users  ──┘

-- column-level: fct_orders.net_amount
--   = stg_orders.amount - stg_orders.discount
```

It can be captured by parsing SQL (dbt manifest, SQL parsers), from orchestration metadata, or at runtime by emitting events (OpenLineage from Airflow, Spark, Flink to a catalog such as DataHub or Marquez). Column-level lineage is more useful but harder than table-level.

## Senior angle & interview tips
Lineage underpins incident triage, schema-change impact analysis, GDPR/PII tracking and deprecation of unused tables. Weak spots: dynamic SQL, UDFs and Python transforms that parser-based tools cannot see; stale lineage when captured only at design time; and cross-system gaps (e.g. data leaving to a spreadsheet). Runtime-emitted lineage is more accurate than static parsing but needs instrumentation everywhere.

Interview question: "A key revenue metric changed overnight; how do you debug it?" Model answer: use lineage to walk upstream from the metric, compare each hop for row counts and schema/logic changes (recent deploys, late data), and find the first hop where it diverges; then use downstream lineage to notify consumers.
