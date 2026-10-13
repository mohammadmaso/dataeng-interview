---
term: "Data contract"
aliases: ["data contracts"]
category: quality
short: "An explicit, versioned agreement between a data producer and its consumers on schema, semantics, quality and SLAs."
related: ["lineage", "schema-evolution", "data-mesh"]
---
## In plain words (Junior)
When you rent an apartment you sign a contract so both sides know what to expect. A data contract is the same between the team that produces data and the teams that use it. It says what fields exist, what they mean, how fresh the data will be and how changes will be announced, so a producer cannot silently break everyone downstream.

## How it works (Mid)
A data contract is machine-readable metadata, usually YAML or a schema registry definition, covering: schema and types, semantics (units, enums, PII flags), quality rules (not null, uniqueness, ranges), SLAs (freshness, completeness), ownership, and a compatibility policy for evolution.

```
dataset: orders
version: 2.1.0
owner: team-checkout
schema:
  - {name: order_id, type: string, required: true, unique: true}
  - {name: amount,   type: decimal(12,2), required: true, min: 0}
  - {name: status,   type: string, enum: [NEW, PAID, CANCELLED]}
sla: {freshness: 15m, availability: 99.9%}
compat: backward
```

It is enforced in CI (breaking-change checks on producer PRs), at ingestion (validate against the schema, send violations to a dead letter queue) and in monitoring (freshness and quality tests). Tools include Confluent Schema Registry with Avro/Protobuf compatibility modes, dbt model contracts, and Great Expectations or Soda checks.

## Senior angle & interview tips
The point is shifting data quality left: the producing team owns the interface, as with an API. Without it, the typical failure is an application team renaming a column and breaking dashboards at 3 a.m. Trade-offs: contracts add process and need real ownership; too strict slows product teams, too loose provides nothing. Define compatibility (backward, forward, full) and a deprecation window. Contracts pair naturally with data mesh and with CDC pipelines, where table schemas leak internal DB design.

Interview question: "How do you stop upstream schema changes from breaking pipelines?" Model answer: versioned contracts in a registry, compatibility checks in the producer's CI, validation at ingestion with a DLQ, and named owners with an agreed deprecation period.
