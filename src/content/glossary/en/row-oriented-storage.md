---
term: "Row-oriented storage"
aliases: ["row store","row-based storage","row format"]
category: storage
short: "Storing all fields of a record together, row after row."
related: ["columnar-storage","oltp","parquet"]
---
## In plain words (Junior)
Like an index card per customer with every detail on it: great for reading or updating one customer, wasteful for adding up one column across everyone.

## How it works (Mid)
Whole rows are laid out contiguously on disk, so reading or writing a single record touches few pages. Typical for OLTP databases (Postgres, MySQL) and formats like Avro and CSV. Poor for analytics that scan a few columns across millions of rows.

## Senior angle & interview tips
Choose row stores for point lookups, frequent small writes and transactional integrity; choose columnar for scans and aggregates. Hybrid designs (HTAP, row-group columnar files) exist. Avro is row-based and preferred for streaming records because whole events are written and read together.

Interview question: "Why not run analytics on the OLTP row store?" Answer: scans read whole rows, compete with transactions, and lack columnar compression and vectorization.
