---
term: "Feature store"
aliases: ["feature stores", "online store", "offline store", "feast", "point-in-time join"]
category: ml
short: "A system that stores, serves and reuses ML features consistently for both training (offline) and low-latency inference (online)."
related: ["training-serving-skew", "backfill", "event-time"]
---
## In plain words (Junior)
A feature store is a shared pantry of prepared ingredients (features) for the data science kitchens. Instead of each model team recooking "customer's average spend over 30 days" in its own way, there's one tested definition used both when training the model and when serving live predictions.

## How it works (Mid)
Features are computed by batch and streaming pipelines from raw events. The offline store keeps full history (lake or warehouse) to build training datasets; the online store keeps the latest value per entity in a low-latency key-value store for real-time inference. A registry holds feature definitions, owners, and metadata, with materialization jobs syncing offline to online.

```
        batch/stream pipelines
                 │  (one feature definition)
        ┌────────┴─────────┐
   OFFLINE store       ONLINE store
 (Parquet/warehouse)   (Redis/DynamoDB)
 history for training   latest value, ms lookups for inference

-- Point-in-time correct training set: for each (entity, label_ts) take the feature
-- value as of label_ts, never later  (ASOF join)
SELECT l.*, f.avg_spend_30d FROM labels l
ASOF JOIN features f ON l.user_id = f.user_id AND f.ts <= l.label_ts;
```

The key capability is point-in-time correctness: when building training data, each example must use feature values as they were at the label's timestamp, otherwise future information leaks into training (label leakage) and the model looks great offline but fails in production. Feature stores also provide reuse, lineage, monitoring, and backfills.

## Senior angle & interview tips
Data-engineering challenges: exactly the same transformation logic for batch and stream (avoid reimplementing in two codebases, which causes training-serving skew), late-arriving events and event-time correctness, backfilling features for new definitions, TTLs and freshness in the online store, entity key design, and cost of serving at scale. Not every team needs one: for a few models, a well-organized warehouse plus a simple key-value export may suffice. Alternatives include computing features at request time (on-demand features) or using the warehouse as the offline store with a thin online cache.

Interview question: "What problem does a feature store solve?" Model answer: it guarantees consistent, reusable features between training and serving with point-in-time-correct training data, avoiding leakage and training-serving skew, and gives low-latency online access plus monitoring and lineage.
