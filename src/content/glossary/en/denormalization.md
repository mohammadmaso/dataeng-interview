---
term: "Denormalization"
aliases: ["denormalized","wide table"]
category: modeling
short: "Deliberately duplicating data to avoid joins and speed up reads."
related: ["normalization","one-big-table","star-schema"]
---
## In plain words (Junior)
Writing the customer name directly on every order instead of looking it up each time: faster to read, but changing a name means fixing many places.

## How it works (Mid)
Denormalized models embed related attributes (customer, product) in one wide row. Reads need fewer joins and scan less metadata; writes and updates become heavier, and consistency must be maintained by the pipeline. Common in analytics layers, document stores and star-schema dimensions.

## Senior angle & interview tips
Denormalize at the serving layer, keep the normalized or raw layer as the source of truth, and rebuild derived tables from it. In columnar warehouses, repeated values compress well, so storage cost is small. The risk is stale copies when dimensions change.

Interview question: "When is denormalization right?" Answer: read-heavy analytics where join cost dominates and data is rebuilt from a normalized source.
