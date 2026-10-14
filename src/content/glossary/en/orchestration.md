---
term: "Workflow orchestration"
aliases: ["orchestrator","airflow","dagster","prefect","scheduler"]
category: orchestration
short: "Scheduling and coordinating tasks and their dependencies, with retries and monitoring."
related: ["dag","data-pipeline","backfill","retry"]
---
## In plain words (Junior)
A conductor telling each musician when to start so the whole piece plays in order, and noticing if someone misses a cue.

## How it works (Mid)
Orchestrators (Airflow, Dagster, Prefect) define tasks as a DAG, run them on schedules or triggers, enforce dependencies, retry failures, pass parameters, expose state and logs, and support backfills. They coordinate work; heavy computation should run in Spark, the warehouse or containers, not inside the orchestrator.

## Senior angle & interview tips
Keep tasks idempotent and parameterized by logical date, avoid heavy work in the scheduler process, use sensors or datasets for event-driven triggers, set SLAs and alerts, and limit concurrency/pools. Asset-based orchestration (Dagster) models data outputs rather than only tasks.

Interview question: "Task-centric vs asset-centric orchestration?" Answer: task-centric schedules steps; asset-centric declares data products and derives what must run, giving better lineage.
