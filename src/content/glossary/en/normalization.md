---
term: "Normalization and denormalization"
aliases: ["normalize", "denormalize", "denormalization", "3nf", "third normal form", "one big table"]
category: modeling
short: "Normalization removes redundancy by splitting data into related tables (good for OLTP); denormalization duplicates data to speed reads (good for analytics)."
related: ["oltp", "star-schema", "cardinality"]
---
## In plain words (Junior)
Keeping a customer's address in one place and referring to it from every order means a move needs only one edit (normalized). Copying the address onto every order makes looking things up easier, but an address change must be fixed in many places (denormalized). It's a trade between easy updates and easy reading.

## How it works (Mid)
Normal forms: 1NF (atomic values, no repeating groups), 2NF (no partial dependency on part of a composite key), 3NF (no transitive dependencies: non-key attributes depend only on the key). Normalized schemas avoid update/insert/delete anomalies and save space; they fit OLTP with many small writes, at the cost of joins for reads.

```
-- Normalized (3NF): each fact stored once
customers(customer_id PK, name, city_id FK)   cities(city_id PK, city, country)
orders(order_id PK, customer_id FK, ordered_at)  order_items(order_id, product_id, qty, price)

-- Denormalized (analytics): one wide table, no joins at query time
fct_orders_wide(order_id, ordered_at, customer_name, city, country, product_name, qty, price)
-- Pro: fewer joins, columnar compression hides repetition. Con: updates must touch many rows.
```

Analytics inverts the trade-off: data is written in batches, read heavily and rarely updated, so denormalized dimensional models (star schema) or wide tables reduce joins, and columnar compression and dictionary encoding make repetition cheap. Document and NoSQL stores denormalize by default (embedding), pushing consistency to the application.

## Senior angle & interview tips
Denormalization costs: update anomalies (change propagation), larger storage, and the risk of inconsistent copies, so it should be derived from a normalized or authoritative source by pipelines, not hand-maintained. For history, prefer SCD2 over repeating mutable attributes. "One big table" speeds BI and removes join bugs but hides grain and can explode in width; keep it as a downstream layer (gold), built from modeled dimensions. Distributed engines favor denormalizing or broadcasting small dimensions because large shuffled joins are expensive. A good answer says where each style belongs in the stack rather than picking one.

Interview question: "Normalized or denormalized for an analytics warehouse?" Model answer: denormalized dimensional models for the serving layer to minimize joins and give simple SQL, built from a normalized/raw source of truth by idempotent pipelines, with SCD2 for history and tests to keep copies consistent.
