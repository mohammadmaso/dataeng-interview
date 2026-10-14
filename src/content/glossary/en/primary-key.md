---
term: "Primary key"
aliases: ["pk"]
category: sql
short: "A column or set of columns that uniquely identifies each row."
related: ["surrogate-key","database-index","foreign-key"]
---
## In plain words (Junior)
Like a national ID number: no two people share it and everyone has one, so it points to exactly one person.

## How it works (Mid)
A primary key enforces uniqueness and not-null, and usually creates a unique index. It can be natural (email, ISBN) or surrogate (generated id), single or composite. Foreign keys in other tables reference it.

## Senior angle & interview tips
In warehouses (Snowflake, BigQuery) primary keys are often informational only and not enforced, so uniqueness must be tested in the pipeline. Random UUID keys hurt B-tree locality; time-ordered ids (UUIDv7, ULID) insert more efficiently. Choose immutable keys.

Interview question: "Natural or surrogate key?" Answer: surrogate when natural values can change or be reused; natural when stable and meaningful.
