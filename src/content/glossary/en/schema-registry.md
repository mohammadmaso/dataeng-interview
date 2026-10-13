---
term: "Schema registry"
aliases: ["confluent schema registry", "avro schema registry", "glue schema registry", "schema subject"]
category: ingestion
short: "A central service storing versioned schemas for messages and enforcing compatibility rules between producers and consumers."
related: ["schema-evolution", "data-contract", "parquet"]
---
## In plain words (Junior)
A schema registry is like a shared dictionary kept at the front desk. Instead of every message carrying its full definition, it carries only a short reference number, and anyone who needs to understand it looks the definition up. The front desk also refuses new definitions that would confuse existing readers.

## How it works (Mid)
Messages in Kafka are bytes, so producers and consumers need to agree on structure. Binary formats (Avro, Protobuf, JSON Schema) are compact when the schema isn't embedded. The registry assigns each schema a global id and groups versions under a subject (usually per topic). Producers include the id; consumers fetch the schema by id and cache it. Reader and writer schemas can differ and are resolved using compatibility rules.

```
Message on the wire: [magic byte][schema id (4 bytes)][avro/protobuf payload]

producer: serialize(record) -> register/lookup schema -> get id -> send id + bytes
consumer: read id -> fetch schema (cached) -> deserialize with writer's schema

Subject: orders-value, versions v1, v2, v3 ...  compatibility: BACKWARD | FORWARD | FULL | NONE
POST /subjects/orders-value/versions   -> rejected (409) if it breaks the compatibility mode
```

The registry enforces the compatibility mode when a new version is registered, providing a safe evolution gate: add fields with defaults, don't change types or remove required fields. It can run in CI to block breaking producer changes before deployment.

## Senior angle & interview tips
It is infrastructure that becomes critical: if the registry is down, new producers can't register schemas (consumers use caches), so run it highly available. Choose Avro vs Protobuf vs JSON Schema based on tooling and evolution rules (Protobuf's field numbers, Avro's defaults and unions). Subject naming strategy (topic-name vs record-name) affects multiple event types per topic. Compatibility `TRANSITIVE` modes check all prior versions, which matters for long-lived topics with replays. The registry enforces structure, not semantics; pair it with data contracts for meaning, ownership and quality. It also helps downstream sinks (Kafka Connect to Iceberg) map schema automatically.

Interview question: "How do you prevent a producer deploy from breaking consumers?" Model answer: a schema registry with backward (or full) compatibility enforced in the producer's CI/build, so incompatible schemas can't register; roll consumers first for backward-compatible changes, and use a deprecation window for removals.
