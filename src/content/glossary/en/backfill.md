---
term: "Backfill"
aliases: ["backfilling", "backfills", "reprocessing"]
category: batch
short: "Re-running a pipeline over historical time ranges to fill gaps or correct data after a bug or logic change."
related: ["idempotency", "dag", "late-arriving-data", "partition"]
---
## In plain words (Junior)
Suppose you forgot to record the temperature for two weeks, then find the old logs. You go back and fill in the missing days in your notebook. A backfill is the same for data: the pipeline did not run, ran wrongly, or the logic changed, so you re-run it for past dates to fix the history.

## How it works (Mid)
A backfill replays a pipeline for a set of logical dates (daily partitions) or an offset range (streams). It works well only if the pipeline is idempotent and parameterized on the logical date, so each run rewrites exactly its own slice.

```
# Airflow: rerun a date range for one DAG
airflow dags backfill -s 2024-01-01 -e 2024-01-31 \
    --reset-dagruns my_daily_dag

# Or clear task instances so the scheduler reruns them
airflow tasks clear my_daily_dag -s 2024-01-01 -e 2024-01-31
```

Practical concerns: process in chunks to limit load, limit concurrency (`max_active_runs`) so you do not starve production runs, check upstream data still exists (retention), and decide whether downstream tables should be recomputed too. For streams, a backfill is usually a separate batch job or a replay from Kafka/lake history, kept apart from the live job.

## Senior angle & interview tips
Backfills are where bad design shows: pipelines that read `now()`, depend on the previous day's output (`depends_on_past`), or mutate dimensions without versioning produce different history when rerun. Point-in-time correctness matters: backfilling a fact table against today's dimension values silently rewrites history unless SCD2 is used. Large backfills can overload sources, trigger duplicate alerts, and make dashboards flicker, so write to a shadow table and swap atomically (Iceberg branch / table rename).

Interview question: "A bug corrupted three months of data; how do you fix it?" Model answer: fix the logic, backfill by partition with idempotent overwrites into a staging or branch table, validate counts and checksums against the old data, then swap and notify downstream consumers.
