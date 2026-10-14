---
term: "Data Vault"
aliases: ["data vault 2.0","hub link satellite"]
category: modeling
short: "A modeling method with hubs, links and satellites that tracks business keys, relationships and history."
related: ["star-schema","normalization","scd"]
---
## In plain words (Junior)
Three kinds of boxes: hubs list the important things (customers), links record how things connect (customer buys product), and satellites keep the changing details with timestamps.

## How it works (Mid)
Hubs hold unique business keys; links hold relationships between hubs; satellites hold descriptive attributes with load date, so full history is kept and sources can be added without remodeling. It is typically the integration (raw vault) layer, with star-schema marts built on top.

## Senior angle & interview tips
Strengths: auditability, parallel loads, easy source onboarding. Costs: many tables, join-heavy queries, steep learning curve, usually needs automation or generators. Often overkill for small teams.

Interview question: "Why add a Data Vault before the marts?" Answer: it decouples ingestion from business modeling and preserves full history of every source.
