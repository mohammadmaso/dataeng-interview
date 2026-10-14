---
term: "Foreign key"
aliases: ["fk","referential integrity"]
category: sql
short: "A column that references the primary key of another table to link records."
related: ["primary-key","normalization","dimension-table"]
---
## In plain words (Junior)
The customer id written on an order slip: it points to the one customer card that holds the details.

## How it works (Mid)
A foreign key constraint ensures the referenced row exists (referential integrity) and can cascade deletes or updates. It enables joins between tables and documents relationships.

## Senior angle & interview tips
OLTP databases enforce them; analytical warehouses and distributed stores often do not, for write speed, so orphan rows must be caught with tests (relationships tests in dbt). Index foreign key columns or deletes and joins on the parent get slow. Late-arriving dimensions create orphan facts, handled with placeholder (inferred) members.

Interview question: "Why do warehouses skip FK enforcement?" Answer: bulk loads would be slowed by checks and the pipeline already controls integrity.
