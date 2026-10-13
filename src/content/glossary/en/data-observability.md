---
term: "Data observability"
aliases: ["data monitoring", "freshness monitoring", "anomaly detection", "pipeline monitoring"]
category: quality
short: "Continuously monitoring the health of data and pipelines (freshness, volume, schema, distribution, lineage) to detect and diagnose issues early."
related: ["data-quality", "lineage", "sla-slo"]
---
## In plain words (Junior)
A car dashboard shows speed, fuel and engine warnings so you notice trouble before you break down. Data observability is the dashboard for data: it tells you the moment a table is late, suddenly half the usual size, or has a column that changed, before a stakeholder finds a wrong number.

## How it works (Mid)
Tests (data quality) check known rules; observability watches system behavior to catch unknown problems and shortens diagnosis. Signals: pipeline run status and durations, table freshness, row counts, schema changes, null/duplicate rates, value distribution shifts, query and cost metrics, and lineage for impact analysis. Implemented via metadata/metric collection (warehouse information schema, query logs, OpenLineage events), time-series baselines with anomaly detection, and alerting integrated with on-call tools.

```
Five pillars (commonly cited):
  freshness     - did the table update when expected?        (max(updated_at) vs now)
  volume        - is the row count within the usual range?   (z-score vs last 4 weeks)
  schema        - did columns/types change?
  distribution  - null rate, uniqueness, value ranges drift?
  lineage       - what upstream caused it, what downstream is affected?

-- freshness check
SELECT now() - max(loaded_at) AS staleness FROM mart.orders;   -- alert if > SLO
```

Tools include open-source stacks (dbt artifacts, Great Expectations/Soda, Elementary, OpenLineage + Marquez/DataHub, Prometheus/Grafana for pipeline metrics) and commercial platforms. The goal is lower mean-time-to-detect and mean-time-to-resolve.

## Senior angle & interview tips
Alert quality matters: noisy anomaly alerts get ignored, so tune seasonality (weekday patterns), tier tables by criticality, route alerts to owners, and define SLOs per data product (freshness within 30 min, 99% completeness). Combine observability with lineage to jump from symptom to root cause quickly, and with runbooks for common failures. Distinguish pipeline health (the job ran) from data health (the output is right): a green DAG can hide wrong data. Cost observability (expensive queries, storage growth) is increasingly part of it.

Interview question: "A dashboard is wrong and nobody noticed for a week; how do you prevent that?" Model answer: freshness, volume and distribution monitors with owners and severity on critical tables, row-level tests at key joins, lineage to find the root cause quickly, and SLOs so detection happens before consumers see it.
