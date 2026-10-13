---
order: 155
tid: "pattern-schema-evolution"
track: "14"
section: "14B"
num: "P11"
title: "Schema Evolution in Pipelines"
type: "pattern"
level: "advanced"
priority: "P1"
est_hours: 2
summary: "Absorbing source schema changes without breaking the pipeline: registry, contracts, flexible columns, and alerting."
covers: []
interview_qs: []
extras: []
prerequisites: ["schema-registry"]
related: ["schema-evolution", "governance-contracts-mesh", "dlq-bad-records", "data-quality", "lakehouse-table-formats"]
---

## 🎯 TL;DR

> **Schema evolution pattern:** the source **will** change its schema. Design the pipeline so that most changes are absorbed automatically, dangerous changes are detected and stopped early, and the rest are handled through an agreed contract, not a surprise at 3 a.m.

- Classify changes first: **additive** (new optional field), **widening** (`int` → `bigint`), **rename**, **removal**, and **meaning change** (same name, different value). The last one is the most dangerous, because nothing fails.
- Use a **registry and compatibility rules** for event schemas, a **landing zone** that keeps the raw payload, and a **typed curated layer** that only accepts what the contract allows.
- Alert on **drift**. A pipeline that quietly drops a new column is still broken.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A shared shipping form between two companies. If one company adds a new box, the other can ignore it, as long as nothing else moves. If one company renames "amount" to "amount_cents", the other company's system reads a wrong number and keeps going. The second kind of change is the one you must catch.

Imagine the orders service publishes this JSON event on day 1:

```json
{"order_id": 101, "user_id": 7, "amount": 250}
```

On day 10 the team adds a currency and changes the amount unit:

```json
{"order_id": 102, "user_id": 8, "amount_cents": 25000, "currency": "USD"}
```

A naive pipeline reads `amount` and gets `NULL` for every new row. Revenue drops by 100 percent for the new rows, and no error appears. A better pipeline knows the old and new names, converts both to the same unit, and records any unknown field instead of dropping it.

## 🔧 How it really works (Mid)

### Classify the change

| Change | Example | Risk for consumers |
|---|---|---|
| Add optional field | new `currency` | Low: old readers ignore it |
| Widen type | `int` → `bigint` | Low, if the engine promotes safely |
| Narrow type | `bigint` → `int` | High: values can overflow or be cut |
| Rename | `amount` → `amount_cents` | High: the old name becomes `NULL` |
| Remove field | drop `user_id` | High: joins and keys break |
| Change meaning | `amount` now in cents | Highest: nothing fails, numbers are wrong |

### Layer 1: registry and compatibility

For event streams, a schema registry stores each version of the schema, and producers and consumers check it. Avro, Protobuf and JSON Schema are typical formats. The registry enforces a **compatibility mode** on each subject (topic):

- **Backward compatible:** new readers can read data written with the old schema. Typical rule: you may add optional fields with defaults and delete fields.
- **Forward compatible:** old readers can read data written with the new schema. Typical rule: you may add fields and delete optional fields.
- **Full:** both directions, so changes must be safe on both sides.

A producer that tries to register an incompatible schema gets an error before any bad event is published. That is the cheapest possible failure.

### Layer 2: landing zone keeps the raw payload

Store the original payload next to the typed columns. A simple raw table with the full JSON (or a variant type, where the engine supports it) means that a new field can be extracted later, and nothing is lost while the team debates the mapping.

```sql
-- Staging: map known fields, convert units, keep everything else
SELECT
  CAST(payload->>'order_id' AS BIGINT) AS order_id,
  CAST(payload->>'user_id'  AS BIGINT) AS user_id,
  -- v1 sent "amount" in units, v2 sends "amount_cents"
  COALESCE(CAST(payload->>'amount_cents' AS BIGINT),
           CAST(payload->>'amount' AS BIGINT) * 100) AS amount_cents,
  COALESCE(payload->>'currency', 'USD') AS currency,  -- assumed default, agree with the producer
  payload                                  AS _raw     -- keep the full event
FROM landing_orders;
```

The `COALESCE` is the **mapping layer**. It is explicit: the code says what the old and new versions mean. The `_raw` column is the safety net.

### Layer 3: curated layer with a strict contract

The curated table has a fixed schema. Loads fail or quarantine a record when a required field is missing or the type is wrong. Unknown fields are not added silently. A new field enters the curated layer only through a reviewed change to the mapping. This is a deliberate choice: the landing zone is flexible, the curated layer is strict.

### Table formats

Table formats handle some changes for you. Apache Iceberg tracks columns by an internal ID, so a rename does not break old files, and it supports safe type promotions such as `int` → `long`. Parquet readers in some engines match columns by position rather than name, so check your engine before you rename in place. Treat the exact list of supported promotions as something to verify in the docs for your version.

## 🧠 Senior depth & trade-offs

- **Compatibility is about the upgrade order.** A "backward compatible" change lets you upgrade consumers first, then producers. A "forward compatible" change lets you upgrade producers first. Choose the mode that matches how your teams deploy. For a rename, neither mode helps: use an **expand and contract** migration.
- **Expand and contract.** Step 1: producers write both `amount` and `amount_cents`. Step 2: consumers switch to `amount_cents`. Step 3: after a deprecation window (weeks, not days), producers stop writing `amount`. Each step is reversible until the last one. The mapping layer above is the consumer side of step 2.
- **Versioned topics.** For a breaking change that cannot be migrated gradually, publish `orders.v2` next to `orders.v1`, run both for a window, and migrate consumers with a deadline. It costs double storage and double processing for that window, which is acceptable for a bounded period.
- **Silent meaning changes need data checks.** No schema registry can detect that `amount` changed units. Add checks on distributions and null rates per column, for example "average amount changed by more than 50 percent day over day", and alert on them. See [[data-quality]].
- **Engine behaviour.** Schema merge on read (for example a `mergeSchema` option in Spark) can be expensive, because the engine must read the footers of many files to build the union of columns. Mixed types across files (a column that was `int` in old files and `string` in new ones) can fail or produce surprising casts. Check the plan and the file list, not only the success status.
- **Stream consumers.** Avro readers resolve the writer's schema against their own reader schema. A field missing in the writer and without a default in the reader fails the job. Test the schema resolution in CI with real samples.
- **When not to use this pattern.** If you own both the producer and the consumer and they deploy together, a simple coordinated change with a migration note is cheaper than a registry plus landing zone. Do not build a full evolution framework for a table that changes once a year.

## 🧪 Try it

A producer sends these two versions of a payload:

```json
{"id": 1, "amount": 12}
{"id": 2, "amount_cents": 1200, "currency": "EUR"}
```

1. What does a pipeline that only reads `amount` return for `id = 2`?
2. Write the mapping that returns `amount_cents` for both rows, with `currency` defaulting to `'EUR'` when missing.
3. The producer later renames `id` to `event_id`. Which of the change types from the table is this, and which migration would you use?

<details>
<summary>Your guess?</summary>

1. `NULL`, with no error. The pipeline silently loses the amount for every new event.
2. `COALESCE(CAST(payload->>'amount_cents' AS BIGINT), CAST(payload->>'amount' AS BIGINT) * 100)` for `amount_cents`, and `COALESCE(payload->>'currency', 'EUR')` for `currency`. Row 1 gives 1200, row 2 gives 1200.
3. It is a **rename** (high risk). Use expand and contract: write both `id` and `event_id` for a while, move consumers to `event_id`, then stop writing `id` after the deprecation window.

</details>

## 💬 Interview corner

**Typical question:** "The source team renamed a field overnight and our daily load failed. What do you do, and what do you change so it does not happen again?"

- **Junior answer:** "I would fix the column name in the SQL and rerun the job."
- **Mid answer:** "I would fix the mapping, rerun the failed window, and add a schema check to the pipeline so it fails earlier."
- **Senior answer:** "First I restore the service: the landing zone kept the raw payload, so I can re-map and backfill without asking the source for data. Then I find out why the change was not coordinated, and I add a compatibility check in the registry or CI so incompatible changes are rejected at the producer. In the pipeline I add drift alerts and a quarantine for unknown or missing fields, and I agree on an expand-and-contract process for future renames."

> **🎤 Say this in the interview:** "I keep a raw landing zone and a strict curated layer. Compatible changes are absorbed by an explicit mapping, incompatible ones are blocked at the registry or in CI, and meaning changes are caught by data checks. Renames go through expand and contract, so no consumer breaks in one step."

## ⚠️ Common mistakes

- **Ignoring unknown fields** silently. New data is lost, and nobody notices until a report is wrong.
- **Treating a rename as an addition.** The old column becomes `NULL`, and the job still succeeds.
- **Auto-accepting every change** into the curated layer. Schema drift then leaks into every downstream model.
- **Trusting the registry for meaning.** Compatibility checks look at types and names, not at units.
- **No raw copy.** After a bad mapping you cannot recover the lost fields without asking the source.
- **Upgrading consumers and producers in the wrong order** for the compatibility mode you chose.

## ✅ Check yourself (quiz)

1. A producer adds an optional `coupon_code` field. Which change class is it, and what should the curated layer do?
   - A) Breaking; stop the pipeline
   - B) Additive; the curated layer keeps its contract, the raw payload keeps the field, and a reviewed mapping can add it later
   - C) Rename; create a new table
   - D) Meaning change; alert the business

   <details>
   <summary>Answer</summary>

   **B** — an optional new field is additive. Old consumers ignore it, so there is no need to stop. It stays in the raw payload until someone agrees to add it to the typed layer.

   </details>

2. The source changes `amount` from `int` to `bigint`. Which problem is the least likely?
   - A) Old values are cut off
   - B) Old files and new files have different types that an engine must reconcile
   - C) Values silently change meaning
   - D) The registry needs a new version

   <details>
   <summary>Answer</summary>

   **A** — widening keeps all values. The concern is about mixed types across files (B) and about the registry (D). Silent meaning change (C) is a different kind of change, not caused by widening.

   </details>

3. *(Trade-off)* Consumers deploy weekly, producers deploy daily. Which compatibility mode lets producers ship a new optional field without waiting for consumers?
   - A) Backward compatible, because new readers read old data
   - B) Forward compatible, because old readers can read data written with the new schema
   - C) Both are equivalent here
   - D) Neither; consumers must always upgrade first

   <details>
   <summary>Answer</summary>

   **B** — forward compatibility means old readers (the weekly consumers) can read data written with the new schema, so the producer can ship first. Backward compatibility is the opposite direction of the upgrade order.

   </details>

4. Why keep a `_raw` payload column in the landing zone, even when the typed columns cover today's fields?
   - A) It is required by Parquet
   - B) It allows re-mapping and backfilling after a mapping bug or a new field, without asking the source to resend
   - C) It makes queries faster
   - D) It replaces the curated layer

   <details>
   <summary>Answer</summary>

   **B** — the raw payload is the recovery path. A bad mapping or a new field can be fixed by reprocessing the landing zone, which is much cheaper than requesting history from the source.

   </details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Additive change | New optional field; old consumers ignore it |
| Rename | Old name becomes `NULL` silently; a high-risk change |
| Meaning change | Same name, different unit or semantics; no schema tool catches it |
| Backward compatible | New readers can read old data (upgrade consumers first) |
| Forward compatible | Old readers can read new data (upgrade producers first) |
| Expand and contract | Add the new field, migrate consumers, then remove the old one |
| Landing zone | Raw, flexible copy of the payload that allows re-mapping |
| Schema drift | Incoming schema differs from the registered or expected one; alert on it |

## 🔗 Go further

- [[schema-evolution]] for the general theory and format-level rules.
- [[schema-registry]] for how registries store versions and enforce compatibility.
- [[governance-contracts-mesh]] for data contracts between producer and consumer teams.
- [[dlq-bad-records]] for quarantining records that do not match the contract.
- [[lakehouse-table-formats]] for how Iceberg and Delta handle column changes.
