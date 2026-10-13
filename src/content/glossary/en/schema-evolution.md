---
term: "Schema evolution"
aliases: ["schema change", "schema drift", "backward compatible", "forward compatible"]
category: lakehouse
short: "Changing a dataset's schema over time (add, drop, rename, retype columns) without breaking existing readers or data."
related: ["data-contract", "parquet", "iceberg"]
---
## In plain words (Junior)
A form that people fill out will inevitably need new questions later. If you add a field, old filled forms should still be readable, and the new form should still work for older readers. Schema evolution is the discipline of changing the shape of data in ways that do not break the people and programs already using it.

## How it works (Mid)
Compatibility definitions (Avro/Schema Registry): backward means new readers can read old data (e.g. adding a field with a default, removing an optional field); forward means old readers can read new data; full means both. Safe changes are generally adding optional or defaulted fields and widening types; unsafe ones are removing required fields, renaming, narrowing types, or changing semantics.

```
-- Safe, backward compatible: add optional column
ALTER TABLE orders ADD COLUMN coupon_code STRING;

-- Risky: rename / retype / drop
ALTER TABLE orders RENAME COLUMN amt TO amount;       -- fine in Iceberg (by column id)
ALTER TABLE orders ALTER COLUMN qty TYPE BIGINT;      -- widening int->bigint usually OK

-- Avro compatibility modes: BACKWARD, FORWARD, FULL (+ _TRANSITIVE)
```

How formats handle it differs: Parquet resolves columns by name (or position, depending on the engine), so rename can silently break; Iceberg tracks column ids so renames and reorders are safe; Delta uses column mapping; Avro resolves reader vs writer schemas; Protobuf relies on stable field numbers. Table formats store schema history so old snapshots still read correctly.

## Senior angle & interview tips
Most production incidents come from semantic changes the type system cannot catch (a field changing from cents to dollars), so contracts and tests are needed beyond syntactic compatibility. Streaming needs more care since old and new producers coexist: use a registry, set compatibility mode per subject, and roll out consumers before producers for backward changes. Ingestion strategy options: fail on drift, evolve automatically, or route unexpected fields to a variant/JSON column or DLQ. Always keep a deprecation window instead of in-place breaking changes; the expand-migrate-contract pattern works well.

Interview question: "A source adds a column and our pipeline breaks; how do you prevent it?" Model answer: explicit schemas with tolerant readers for additive changes, a registry with compatibility checks in the producer's CI, alerting on drift, and a quarantine path for incompatible records rather than failing the whole job.
