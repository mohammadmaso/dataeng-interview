---
term: "Apache Avro"
aliases: ["avro","avro format"]
category: storage
short: "A compact row-based binary format with an embedded schema, popular in Kafka."
related: ["schema-registry","schema-evolution","row-oriented-storage","parquet"]
---
## In plain words (Junior)
A shipping box with a packing list taped to it: the data is packed tightly, and the schema tells anyone how to unpack it.

## How it works (Mid)
Avro stores records in binary using a JSON-defined schema. Files embed the schema; Kafka messages usually carry a schema id resolved via a schema registry. It supports rich schema evolution with reader and writer schemas, defaults and aliases.

## Senior angle & interview tips
Prefer Avro (or Protobuf) for streaming events and Parquet/ORC for analytic storage. Compatibility rules (backward, forward, full) must be enforced in CI via the registry. Adding a field without a default breaks old readers.

Interview question: "Avro vs Parquet?" Answer: Avro is row-oriented and ideal for write-heavy, record-at-a-time streaming; Parquet is columnar and ideal for scan-heavy analytics.
