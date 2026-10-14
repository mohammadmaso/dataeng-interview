---
term: "MLOps"
aliases: ["ml pipeline","model deployment"]
category: ml
short: "Practices for reliably building, deploying, monitoring and retraining ML models."
related: ["feature-store","training-serving-skew","data-drift"]
---
## In plain words (Junior)
DevOps for machine learning: making sure a model that works in a notebook keeps working in production.

## How it works (Mid)
Covers data and feature pipelines, experiment tracking, model registry and versioning, CI/CD for training and serving, deployment patterns (batch, online, shadow, canary), and monitoring for drift and performance.

## Senior angle & interview tips
Most ML failures are data problems: skew, stale features, label leakage. Reproducibility needs versioned data, code and config. Data engineers own much of it: feature pipelines, point-in-time-correct training sets, and SLAs.

Interview question: "What is point-in-time correctness?" Answer: training features must be computed only from data available at each label’s timestamp, avoiding future leakage.
