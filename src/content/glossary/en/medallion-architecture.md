---
term: "Medallion architecture"
aliases: ["bronze silver gold", "bronze/silver/gold", "multi-hop architecture"]
category: lakehouse
short: "A layered lakehouse design: bronze (raw), silver (cleaned, conformed), gold (business-ready aggregates)."
related: ["lakehouse", "data-lake", "data-warehouse", "schema-evolution"]
---
## In plain words (Junior)
Think of refining gold: you mine raw ore, then wash and sort it, then shape it into jewelry. Each stage is cleaner and more valuable. Medallion architecture treats data the same way: keep the raw data safe, build a cleaned version from it, then make polished tables ready for reports.

## How it works (Mid)
Bronze keeps data as received (often append-only, plus ingestion metadata like source, load time, batch id), so you can always replay. Silver applies schema enforcement, type casting, deduplication, null handling, and conforms entities and keys; it is the reliable, queryable "single source of truth" layer. Gold holds business-level models: star schemas, aggregates, feature tables and metrics tailored to consumers.

```
sources ─► BRONZE ─────────► SILVER ─────────► GOLD ─► BI / ML
           raw, append-only   cleaned, typed,    aggregates,
           as-received,       deduped, joined,   marts, metrics,
           + load metadata    conformed keys     serving tables
```

Each hop is a pipeline (batch or streaming) writing to table-format storage (Delta, Iceberg, Hudi) so layers can be rebuilt incrementally and idempotently. Quality checks and contracts typically gate bronze to silver and silver to gold.

## Senior angle & interview tips
The value is separation of concerns and reprocessability: if silver logic changes, rebuild from bronze rather than re-ingest. Costs: storage duplication, latency added per hop, and layer sprawl; "medallion" is a convention, not a standard, and teams differ on what belongs where. Common mistakes: transforming in bronze (loses raw fidelity), turning silver into a dumping ground without ownership, and gold tables that duplicate logic across teams. Security and PII handling usually need to start at bronze or even before. It maps to older ideas like staging, ODS and data marts.

Interview question: "Why not load straight into the final tables?" Model answer: raw data retention makes pipelines replayable and auditable, isolates source quirks from business logic, and lets you fix bugs by reprocessing from bronze instead of going back to the source.
