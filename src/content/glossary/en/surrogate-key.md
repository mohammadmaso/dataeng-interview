---
term: "Surrogate key"
aliases: ["surrogate keys", "synthetic key"]
category: modeling
short: "A system-generated, meaningless identifier for a dimension row, independent of the source system's business key."
related: ["dimension-table", "scd", "fact-table", "star-schema"]
---
## In plain words (Junior)
A school gives every student a new ID number that has nothing to do with their name or birthday. If the student changes their name, the ID stays valid. A surrogate key is that kind of ID in a data warehouse table: a made-up number that identifies a row, so the warehouse does not depend on the messy IDs of other systems.

## How it works (Mid)
In dimensional modeling, each dimension row gets a surrogate key (integer sequence or deterministic hash) used as the join key from fact tables. The original source ID stays as a natural/business key attribute.

```
CREATE TABLE dim_customer (
  customer_sk   BIGINT PRIMARY KEY,   -- surrogate
  customer_id   VARCHAR,              -- business/natural key
  name          VARCHAR,
  valid_from    DATE,
  valid_to      DATE,
  is_current    BOOLEAN
);
-- fact rows reference customer_sk, not customer_id
```

Why use it: it decouples the warehouse from source key changes or collisions between systems; it enables SCD Type 2 (several versions of one customer, each with its own key); it makes joins on narrow integers cheap; and it allows special rows such as `-1 = Unknown` for late-arriving or missing dimensions. Hash-based keys (e.g. of business key plus version) are popular in distributed pipelines because they can be generated in parallel without a central sequence.

## Senior angle & interview tips
Trade-offs: sequences need coordination and make parallel/backfill loads harder (keys must be stable across reruns; use deterministic hashes or lookup tables), and fact loading needs a key lookup step. Hash collisions are theoretically possible, so use a wide hash. Do not expose surrogate keys as business identifiers. In lakehouse/ELT stacks (dbt) `generate_surrogate_key` builds hashes from natural keys. Many columnar systems make wide natural keys cheap enough that some teams skip surrogates for non-SCD dimensions.

Interview question: "Why not join facts directly on the source ID?" Model answer: source IDs can change, collide across systems, and cannot represent historical versions; a surrogate key gives stable, versioned, compact joins and a place for unknown members.
