---
term: "One big table (OBT)"
aliases: ["obt","wide denormalized table"]
category: modeling
short: "A single wide, fully denormalized table pre-joining facts and dimensions."
related: ["star-schema","denormalization","snowflake-schema"]
---
## In plain words (Junior)
One huge spreadsheet with everything about each sale on its own row, so analysts never need to join anything.

## How it works (Mid)
Facts and dimension attributes are pre-joined into one table, often with nested columns. Queries are simple and fast on columnar engines, and BI tools need no join logic. Rebuilding is expensive when dimensions change.

## Senior angle & interview tips
Best as a derived serving table on top of a star schema, not as the only model. Watch grain consistency, SCD handling (point-in-time attributes get baked in), and column explosion. Governance is harder because metric logic lives in many places.

Interview question: "OBT vs star?" Answer: OBT optimizes read simplicity; star keeps reusable conformed dimensions and cleaner history.
