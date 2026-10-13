---
order: 91
tid: "schema-registry"
track: "8"
section: null
num: "8.7"
title: "Schema Registry & Serialization"
type: "technology"
level: "advanced"
priority: "P1"
est_hours: 2
summary: "Schema Registry with Avro or Protobuf, compatibility modes, and protecting consumers. Solving \"a producer dropped a field and the pipeline broke.\""
covers: ["Confluent vs Apicurio vs Karapace","subject naming strategies"]
interview_qs: []
extras: []
prerequisites: ["kafka-producers"]
related: ["schema-evolution", "pattern-schema-evolution", "kafka-connect-ops", "data-formats", "governance-contracts-mesh"]
---

## 🎯 TL;DR

> A **schema registry** is a service that stores every version of the schema for your messages and checks each new version against a **compatibility rule**. It stops a producer from breaking consumers before the bad data is ever written.

- Messages on a Kafka topic are bytes. Avro and Protobuf turn them into typed records, and the schema says how to read the bytes.
- Each message carries only a small **schema id**. The registry maps that id to the full schema.
- The **compatibility mode** (BACKWARD, FORWARD, FULL, NONE and their `_TRANSITIVE` forms) decides which schema changes are allowed.
- Most "the pipeline broke after a deploy" incidents are schema changes that a registry would have rejected.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A registry is like a **legal contract template office**. Every time a team changes a contract form, the office checks the new version against the old one. If old readers would no longer understand the new form, the office refuses to file it.

Imagine a producer that sends this Avro record to topic `orders`:

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "long"}
  ]
}
```

Without a registry, the producer writes the schema in its code and hopes every consumer has the same copy. A deployment changes it, drops `amount`, and a consumer crashes at 3 a.m. With a registry, the producer registers the schema first. The registry checks the change and rejects it if consumers would break.

Message bytes on the wire look like this:

```text
[ magic byte = 0 ][ schema id = 4 bytes ][ Avro-encoded payload ]
```

The consumer reads the schema id, fetches the schema (and caches it), and decodes the payload. The message does not carry the whole schema, so it stays small.

> **⚠️ Watch out:** The registry does not check your data. It checks the **schema change**. A field can be type-compatible and still have a wrong meaning. Schema review is still needed.

## 🔧 How it really works (Mid)

### The moving parts

- **Subject:** a named list of schema versions. Under the default naming strategy the subject is `<topic>-value` (and `<topic>-key` for keys). So topic `orders` has subject `orders-value`, with versions 1, 2, 3, and so on.
- **Serializer / deserializer:** client libraries (for example, `KafkaAvroSerializer` in the Confluent clients) register or look up the schema, add the schema id to the bytes, and reverse that on read.
- **Schema id:** global. The same schema gets the same id even across subjects, which is why messages can stay small.

Clients point to the registry with `schema.registry.url` (or the equivalent setting in your client library).

### Subject naming strategies

| Strategy | Subject name | Use when |
|---|---|---|
| TopicNameStrategy (default) | `<topic>-value` | One record type per topic |
| RecordNameStrategy | `<fully.qualified.RecordName>` | Several record types share one topic |
| TopicRecordNameStrategy | `<topic>-<RecordName>` | Several types per topic, but scoped per topic |

Mixing types in one topic with the default strategy fails in confusing ways, because the subject can only hold one "family" of schemas. Pick the strategy when you create the topic, not later.

### Compatibility modes

The compatibility mode is set per subject (or globally). The registry compares the new version with the previous version(s).

| Mode | Rule | Safe change for a new version |
|---|---|---|
| BACKWARD (default in Confluent) | New schema can read data written with the old one | Delete fields; add optional fields with a default |
| FORWARD | Old schema can read data written with the new one | Add fields; delete fields that had a default |
| FULL | Both directions at once | Only changes that are safe both ways, such as adding a field with a default |
| NONE | No check | Anything; this is how incidents happen |
| `*_TRANSITIVE` | Checks against **all** previous versions, not just the last one | Use it when old data may sit in topics for a long time |

For Avro, "add a field" means the new field needs a `default`. Without a default, old data cannot be read with the new schema, so the registry rejects it under BACKWARD. Renaming a field is not compatible unless you add an alias.

For Protobuf, compatibility comes from **field numbers**. Never reuse a number of a removed field, and mark retired numbers as `reserved`. New fields should be `optional` or have default values in the proto3 sense.

### A worked example

Version 1 of `orders-value` is the schema above. Version 2 adds a field with a default:

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "currency", "type": "string", "default": "IRR"}
  ]
}
```

Under BACKWARD this is accepted: a new consumer reading old messages fills `currency` with `IRR`. Version 3 removes `amount`:

- Under BACKWARD, the registry accepts removing a field, because the new reader simply ignores old data it does not need. But downstream code that **expects** `amount` now breaks. Compatibility is not the same as semantic safety.
- Under FORWARD, the registry would reject it if the old consumers have no default for `amount`, because old readers could not read new data.

This is why the choice of mode is a team decision, not a default you forget.

### Rejected change, end to end

The registry answers with an HTTP error. The producer fails before writing any record. That is the win: the bad schema never reaches the topic.

## 🧠 Senior depth & trade-offs

**Choosing the mode.** Use BACKWARD_TRANSITIVE or FULL_TRANSITIVE for topics that feed long-retention data (a lake, Iceberg tables, or a topic with long retention). Your consumers may be on very old versions, and a transitive check protects every one of them. Use plain BACKWARD for short-lived internal topics where the team controls all readers.

**Ordering of deploys.** Mode decides which side must be upgraded first:

- BACKWARD: upgrade consumers first, then producers.
- FORWARD: upgrade producers first, then consumers.
- FULL: any order, but the change set is smaller.

Write this in your change process. Most outages come from doing the deploy in the wrong order even with a valid schema.

**Registry as a dependency.** Every producer writes through it, and every consumer reads from it on cache misses. Run it highly available (several nodes behind a load balancer), back the storage with a replicated Kafka topic (the default for Confluent's `_schemas` topic), and set client caches so a short registry outage does not stop traffic that already has its schema ids cached.

**Confluent vs Apicurio vs Karapace.**

- **Confluent Schema Registry:** the reference implementation. Largest ecosystem support, licensed under the Confluent Community License for the server, so check the license for your use.
- **Apicurio Registry:** open source, supports Avro, Protobuf, JSON Schema and OpenAPI/AsyncAPI. Different REST API details, so client compatibility needs testing.
- **Karapace:** open source, aims at Confluent-compatible REST and wire behavior. Verify the exact endpoints and modes you need.

Whatever you choose, test the serializer your clients use against it, because the wire format and the REST API are what must match.

**Schema evolution beyond compatibility.** A registry checks structure. It cannot detect:

- a unit change (`amount` moves from rials to tomans),
- a changed meaning of a status code,
- a field whose values are now nullable in practice.

Put those in a data contract and in tests. See [[schema-evolution]] and [[data-contract]] for the wider picture.

**Failure modes.**

- Producers using `NONE` mode, usually because someone "temporarily" disabled checks.
- Clients that hard-code a schema and bypass the registry, so the registry never sees the change.
- Cache misses during a registry outage causing a flood of requests.
- Subject names that drift between teams, so two subjects hold two versions of the "same" data.

**What to monitor.** Registration failures and their error codes, the rate of new schema versions per subject (a sudden spike usually means a bug), compatibility-check failures in CI, registry latency and the number of schema lookups per second.

## 🧪 Try it

Run the registry locally (for example with a Confluent or Karapace container) and try the REST API. Commands use the Confluent-style endpoints; check your server's docs for exact paths.

```bash
REG=http://localhost:8081
SUBJ=orders-value

# 1. Set compatibility for this subject
curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' $REG/config/$SUBJ

# 2. Register version 1 (schema is a JSON string inside the body)
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"long\"}]}"}' \
  $REG/subjects/$SUBJ/versions

# 3. Ask whether a candidate version (drops amount) is compatible with the latest
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"}]}"}' \
  $REG/compatibility/subjects/$SUBJ/versions/latest
```

Questions to answer after running it:

1. Does the check in step 3 pass under BACKWARD? Under FORWARD?
2. Change the compatibility to `FORWARD` and rerun step 3. What changes, and why?
3. Which team would you tell first about this change, and in what order would you deploy?

> **🎤 Say this in the interview:** "I put the schema in a registry and choose a compatibility mode per topic. For a lake-bound topic I use the transitive form so old consumers stay safe. The registry stops incompatible changes in CI and at registration. I still review meaning changes, because the registry only checks structure, and I document the deploy order that the chosen mode requires."

## 💬 Interview corner

**Q: A producer removed a field and the pipeline broke. What do you do?**
- **Junior:** Restore the field, or update the consumer to stop reading it. Then ask for a registry check before the next deploy.
- **Mid:** Check the subject's compatibility mode. If it was NONE or the change was allowed under BACKWARD but the consumer relied on the field, add a contract test and fix the deploy order.
- **Senior:** Treat this as an incident with a process fix. Set a transitive compatibility mode, add registry checks in CI, write the deploy order into the runbook, and add a data contract for semantic fields. Then check the damage: find the messages that were written with the broken shape and plan a backfill if the downstream tables are wrong.

**Q: BACKWARD or FORWARD?**
- **Mid:** BACKWARD protects new readers from old data. FORWARD protects old readers from new data. The mode decides the safe deploy order.
- **Senior:** It depends on what you can upgrade first, and on data that stays in long-retention storage. For lakes, use the transitive variant.

**Model answer (senior):** I use a schema registry with Avro or Protobuf so the producer cannot publish an incompatible schema. I set the compatibility mode per subject, use transitive modes for long-retention data, and write the matching deploy order. The registry checks structure, so I still use data contracts and tests for meaning changes.

## ⚠️ Common mistakes

- **Leaving compatibility at NONE** "just for this release". It stays that way.
- **Adding a field without a default** in Avro. Under BACKWARD this is rejected, and under a transitive mode it is rejected too.
- **Reusing a Protobuf field number** after deleting the field. Old and new data now disagree about what the number means.
- **Assuming compatibility means correctness.** A type-compatible change can still change meaning.
- **Ignoring subject naming.** The default strategy breaks when one topic carries several record types.
- **Deploying in the wrong order** for the chosen mode, even when the schema is valid.
- **Bypassing the registry** by hard-coding schemas in consumers.

## ✅ Check yourself (quiz)

1. Which change is accepted by BACKWARD compatibility in Avro?
   - A) Adding a field without a default
   - B) Adding a field with a default
   - C) Changing `long` to `string`
   - D) Renaming `amount` to `total` without an alias

   <details><summary>Answer</summary>

   **B** — The new reader fills the new field from its default when reading old data. A is rejected because old data has no value for it. C and D change types or names without a way to read old data.

   </details>

2. What does the message carry on the wire, besides the payload?
   - A) The full schema
   - B) A magic byte and a schema id
   - C) A timestamp and a checksum of the schema
   - D) Nothing; the consumer guesses the schema

   <details><summary>Answer</summary>

   **B** — The format is a magic byte, a 4-byte schema id and then the encoded payload. The consumer fetches the schema by id and caches it.

   </details>

3. **Scenario:** A team wants to delete `amount` from a topic that feeds a 2-year lake table. Under which mode is the change more likely to be caught, and which mode would you choose?
   - A) NONE, because it allows everything
   - B) FORWARD_TRANSITIVE or FULL_TRANSITIVE, because old readers across all versions must still work
   - C) BACKWARD without transitive, because it checks only the last version
   - D) Any mode works equally well

   <details><summary>Answer</summary>

   **B** — Transitive modes compare against all previous versions, so data written years ago stays readable. Plain BACKWARD only checks the last version, which can miss a chain of individually safe changes that together break old data.

   </details>

4. **Senior trade-off:** Why does compatibility not protect you from every incident?
   - A) Because the registry does not store schemas
   - B) Because it checks structure, not meaning such as units or status codes
   - C) Because Kafka ignores schema ids
   - D) Because serializers always ignore compatibility

   <details><summary>Answer</summary>

   **B** — Structural compatibility does not catch a unit change or a changed meaning of a value. Those need data contracts, tests and review.

   </details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Subject (default name) | `<topic>-value`, holding a list of schema versions |
| Wire format prefix | Magic byte plus 4-byte schema id, then the encoded payload |
| BACKWARD | New schema can read old data |
| FORWARD | Old schema can read new data |
| FULL | Both directions |
| `*_TRANSITIVE` | Check against all versions, not only the previous one |
| Avro add-field rule | New field needs a default to stay compatible |
| Protobuf rule | Never reuse a removed field number; mark it `reserved` |
| TopicRecordNameStrategy | Subject `<topic>-<RecordName>`, for several record types per topic |

## 🔗 Go further

- [[schema-evolution]] — the general problem and the patterns behind it
- [[pattern-schema-evolution]] — a design pattern for changing schemas safely
- [[kafka-connect-ops]] — registry-aware converters in Kafka Connect
- [[data-formats]] — Avro, Protobuf and Parquet compared
- Your registry's documentation for the exact compatibility endpoints and modes of your version
