---
term: "Data freshness"
aliases: ["staleness","data latency","timeliness"]
category: quality
short: "How recent the data in a table or dashboard is compared with the real world."
related: ["sla-slo","data-observability","data-quality"]
---
## In plain words (Junior)
Like checking the date on milk: how old is the data you are looking at right now?

## How it works (Mid)
Freshness is measured as now minus the latest event or load timestamp. Monitor it per table with expected cadence and alert on breaches. Distinguish event-time freshness (how recent the latest event) from load-time freshness (when we last ran).

## Senior angle & interview tips
A table can be "fresh" by load time while all its rows are old if the upstream stalled, so check event time too. Tie freshness SLOs to consumers’ needs, not an arbitrary minute target, and propagate upstream delays in lineage.

Interview question: "The pipeline succeeded but the dashboard is stale. Why?" Answer: load ran on empty or old source data; check source freshness and row counts.
