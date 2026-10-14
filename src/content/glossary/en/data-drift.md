---
term: "Data drift"
aliases: ["concept drift","model drift","distribution shift"]
category: ml
short: "The statistical properties of incoming data change over time, degrading models or reports."
related: ["training-serving-skew","feature-store","data-observability"]
---
## In plain words (Junior)
A weather model trained on summer data starts getting winter inputs and its predictions go wrong.

## How it works (Mid)
Data drift: input distributions change. Concept drift: the relationship between inputs and the target changes. Detect with statistical tests (PSI, KS test, KL divergence) on feature distributions versus a reference window, plus monitoring of prediction quality.

## Senior angle & interview tips
Drift can come from a real-world shift or from a pipeline bug (unit change, new null default). Triage by checking upstream schema/logic changes before retraining. Set alert thresholds and retraining triggers; version the reference data.

Interview question: "Model accuracy dropped but code did not change." Answer: check data drift and upstream pipeline changes first, then label delay and concept drift.
