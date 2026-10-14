---
term: "Data anomaly detection"
aliases: ["volume anomaly","outlier detection","monitoring"]
category: quality
short: "Automatically spotting unusual values, volumes or patterns in data."
related: ["data-observability","data-quality","data-freshness"]
---
## In plain words (Junior)
A smoke alarm for data: it learns what normal looks like and warns you when today’s numbers look odd, like half the usual rows.

## How it works (Mid)
Checks compare metrics (row count, null rate, distinct count, value distribution, freshness) against history using thresholds, seasonality-aware baselines (day of week), or statistical/ML models. Alerts route to owners with context from lineage.

## Senior angle & interview tips
Tune for alert fatigue: too sensitive means ignored alerts. Handle seasonality, holidays, and backfills that legitimately change volume. Pair statistical detection with explicit rules for known invariants (not-null, uniqueness, referential integrity).

Interview question: "Daily row count dropped 60%; walk through diagnosis." Answer: check upstream freshness, ingestion errors, schema or filter changes, then lineage to find the first bad table.
