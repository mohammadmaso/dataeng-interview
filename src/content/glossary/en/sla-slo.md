---
term: "SLA, SLO and SLI for data"
aliases: ["sla", "slo", "sli", "service level objective", "error budget", "data freshness sla"]
category: orchestration
short: "SLI is a measured indicator, SLO the internal target, SLA the external commitment with consequences; for data: freshness, completeness, latency."
related: ["data-observability", "data-quality", "backfill"]
---
## In plain words (Junior)
A pizza place promises "delivery in 30 minutes". The time each order actually takes is what you measure (SLI). The kitchen's internal goal, say 25 minutes for 95% of orders, is the SLO. The promise to customers, with a refund if broken, is the SLA. Data platforms use the same idea for data arriving on time and correct.

## How it works (Mid)
For data products pick a few indicators that reflect consumer experience: freshness (age of newest data), completeness (share of expected rows/partitions present), correctness (quality test pass rate), latency (end-to-end event to availability), availability of the serving layer. Define an SLO per indicator with a window, track an error budget, and set an SLA (usually looser than the SLO) only where there is a real commitment.

```
SLI (what we measure):  fraction of daily runs where mart.orders is updated by 07:00 UTC
SLO (our target):       99% of days over a rolling 30 days    -> error budget = 1% (~0.3 days)
SLA (promise):          "Dashboard data ready by 08:00; else credit / escalation"  (looser than SLO)

# Airflow: task-level SLA / deadline alert
default_args = {"sla": timedelta(hours=2)}      # alert if not finished 2h after schedule
```

Implementation: measure at the data (max timestamp, row count vs expected) rather than only the job status, alert on SLO burn rate, and publish SLOs in the catalog/contract with an owner and on-call route.

## Senior angle & interview tips
Use SLOs to make trade-offs explicit: a tight freshness target needs streaming, redundancy and on-call, a loose one allows cheap batch. Error budgets guide when to stop feature work and fix reliability. Avoid vanity SLOs ("job success 99.9%") that ignore wrong or late data. Dependencies chain: your SLO can't be tighter than your slowest upstream, so negotiate upstream SLOs through contracts. For batch DAGs, define deadlines (data ready by X) and monitor critical path; use priority and pools to protect important DAGs. Postmortems feed back into SLOs and tests.

Interview question: "How do you define and meet a freshness SLO for a daily revenue table?" Model answer: define SLI as the time the partition for date D lands vs 07:00, set an SLO like 99% of days, monitor the critical path with a deadline alert, build in retries and backfill runbooks, and negotiate upstream deadlines via contracts.
