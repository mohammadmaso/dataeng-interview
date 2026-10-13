---
term: "Fact table"
aliases: ["fact tables", "facts", "measures"]
category: modeling
short: "A dimensional-model table holding numeric measurements of a business event at a declared grain, with keys to dimensions."
related: ["grain", "star-schema", "dimension-table", "surrogate-key"]
---
## In plain words (Junior)
A fact table is the diary of what happened: each line records one event, like a sale, with numbers such as quantity and price, plus pointers to the descriptive cards (who, what, when). It usually grows very large, because new events keep being added.

## How it works (Mid)
A fact table stores measures (numeric values you aggregate) and foreign keys to dimensions, at a precisely declared grain. It is the largest table in the model, typically append-mostly, and is queried with joins to dimensions followed by `GROUP BY`.

```
Types (Kimball):
- Transaction fact:        one row per event (order line, click)
- Periodic snapshot:       one row per entity per period (account balance per day)
- Accumulating snapshot:   one row per process instance, updated as it progresses
- Factless fact:          events/coverage with no numeric measure (attendance)

Measure additivity: additive (revenue) | semi-additive (balance, not across time) | non-additive (ratios)
```

Additivity matters: revenue can be summed across any dimension; balances are semi-additive (sum across accounts, not across time); ratios must be recomputed from additive numerator and denominator instead of averaged. Store numerators and denominators, not percentages. Dates are keys to a date dimension; missing dimension values point to an `Unknown` member, not NULL, to preserve inner-join correctness.

## Senior angle & interview tips
Practical points: load order (dimensions before facts), handling late-arriving facts and dimensions, idempotent loads by partition or merge on the natural key, partitioning by date, and pre-aggregated rollups for dashboards. Common bugs: grain mixing, fan-out joins that double count, nulls in foreign keys that drop rows in inner joins, and averaging averages. Events captured in streams may need deduplication before load. Big facts are usually the join's large side, with dimensions broadcast.

Interview question: "Transaction vs snapshot fact table?" Model answer: a transaction fact records each event once and is the finest grain; a periodic snapshot records state at regular intervals (like daily inventory) and makes trends cheap to query even when no event occurred; an accumulating snapshot tracks a process with milestones and is updated in place.
