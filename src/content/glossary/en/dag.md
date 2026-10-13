---
term: "DAG (directed acyclic graph)"
aliases: ["dag", "dags", "directed acyclic graph", "workflow graph"]
category: orchestration
short: "A graph of tasks with directed dependencies and no cycles, defining a valid execution order for pipelines."
related: ["backfill", "idempotency", "etl-elt"]
---
## In plain words (Junior)
A recipe has steps that depend on each other: you must chop vegetables before frying them, but you can boil water at the same time. A DAG is such a recipe as a diagram: boxes (tasks) with arrows (dependencies) that never loop back. That lets a scheduler run independent steps together and dependent steps in the right order.

## How it works (Mid)
Nodes are tasks, edges say "B runs after A". Acyclic matters because a cycle would mean tasks wait on each other forever. The scheduler computes a topological order and runs tasks whose upstreams have succeeded, in parallel where possible. Orchestrators (Airflow, Dagster, Prefect) define pipelines as DAGs and add scheduling, retries, SLAs, backfills and observability.

```
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def sales_pipeline():
    @task
    def extract(): ...
    @task
    def transform(raw): ...
    @task
    def load(clean): ...
    load(transform(extract()))      # extract -> transform -> load

sales_pipeline()
```

The same idea appears in Spark (the logical plan and stage DAG of a job), dbt (model dependency graph from `ref()`), and Flink (job graph). In Airflow, each DAG run is bound to a logical date/data interval, and each task instance has a state such as queued, running, success, failed, up_for_retry.

## Senior angle & interview tips
Design guidance: keep tasks small, idempotent and parameterized by logical date; do not put heavy logic in the scheduler's top-level DAG parsing code; use sensors/deferrable operators or asset-aware scheduling instead of busy polling; limit concurrency and use pools for shared resources; avoid passing large data via XCom (pass references instead). A DAG cannot express loops or dynamic fan-out natively, so use dynamic task mapping. Event-driven or data-aware scheduling (datasets/assets) avoids brittle time-based coupling between DAGs.

Interview question: "How do you make an Airflow DAG reliable?" Model answer: idempotent, small tasks keyed by data interval; retries with backoff and timeouts; alerts and SLAs; data quality checks as tasks; backfill-safe design without depends_on_past surprises; and explicit resource pools.
