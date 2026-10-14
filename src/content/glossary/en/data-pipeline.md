---
term: "Data pipeline"
aliases: ["pipeline","data flow"]
category: orchestration
short: "A series of automated steps that move and transform data from sources to destinations."
related: ["etl-elt","dag","idempotency","data-quality"]
---
## In plain words (Junior)
A conveyor belt: raw items enter at one end, get cleaned and shaped at stations along the way, and arrive ready to use at the other end.

## How it works (Mid)
A pipeline ingests from sources (databases, APIs, events), transforms (clean, join, aggregate) and loads to targets (warehouse, lake, serving store). It is orchestrated as a DAG, monitored, and made reliable via retries, idempotency, backfills, tests and alerting.

## Senior angle & interview tips
Judge a pipeline by correctness, freshness, cost, recoverability and observability, not just whether it runs. Design for failure: partial runs, duplicates, schema changes, late data. Treat pipelines as software: version control, CI, environments, code review.

Interview question: "What makes a pipeline production-ready?" Answer: idempotent reruns, data-quality checks, alerting on freshness/volume, lineage and documented ownership.
