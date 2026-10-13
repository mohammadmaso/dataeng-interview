---
term: "Training-serving skew"
aliases: ["train-serve skew", "data drift", "concept drift", "label leakage", "data leakage"]
category: ml
short: "A mismatch between the data or feature logic used to train a model and what it sees in production, silently degrading predictions."
related: ["feature-store", "data-quality", "event-time"]
---
## In plain words (Junior)
A student practices with a calculator that rounds in one way, then takes the exam with a calculator that rounds in another. Their answers will be slightly wrong without them knowing why. A model has the same problem when the numbers it learned from are prepared differently from the numbers it gets in real use.

## How it works (Mid)
Skew sources: duplicated feature logic in two systems, time-travel leakage during training dataset creation, different null/default/unit handling, stale features in the online store, and data processing changes (library versions, schema changes). Related but different: drift, where production data changes over time even if pipelines are consistent, and concept drift, where the target relationship changes so a stable feature distribution still gives worse predictions.

```
Typical causes
1. Different code paths:   training in SQL/Spark, serving in Python/Java -> subtle logic differences
2. Time leakage:           training feature uses data after the label time (not point-in-time)
3. Missing/late features:  online store lag, nulls handled differently, defaults differ
4. Data drift:             input distribution shifts after deployment (new product, season)
5. Concept drift:          relationship between input and label changes (fraud tactics change)

Detect: compare feature distributions (PSI, KS test, mean/null rate) training vs live; log served features
```

Mitigations: a single feature definition shared by training and serving (feature store, shared transformation library), point-in-time joins, logging the exact features used at serving time and training on those logs, and monitoring distributions with alerts.

## Senior angle & interview tips
Monitoring needs both input and output checks: feature distribution drift (PSI, KS, null rate), prediction distribution, and delayed ground-truth metrics when labels arrive. Data engineers are often the root cause owners: upstream schema changes, late data, and backfills that changed history without retraining. "Log and replay" is a powerful pattern: record serving features, join with later labels, and use them for training, guaranteeing identical logic. Beware of label leakage via aggregates computed over windows including the label event, and of training on data selected by the previous model (feedback loops).

Interview question: "A model performs well offline and poorly in production; what do you check?" Model answer: first leakage and training-serving skew: compare logged serving features with the offline ones for the same entities, verify point-in-time correctness and null/default handling, check online feature freshness, then investigate data drift and label delay.
