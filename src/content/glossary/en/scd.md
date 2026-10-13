---
term: "Slowly changing dimension (SCD)"
aliases: ["scd", "scd2", "scd type 2", "slowly changing dimensions", "type 2 dimension"]
category: modeling
short: "Techniques for handling dimension attributes that change over time, e.g. overwrite (Type 1) or keep versions (Type 2)."
related: ["surrogate-key", "dimension-table", "late-arriving-data", "cdc"]
---
## In plain words (Junior)
Your friend moves to a new city. In your address book you can cross out the old address and write the new one (you lose the history), or add a new line and keep the old one with dates. Slowly changing dimensions are these choices for warehouse tables: when a customer's details change, do you forget the old values or keep the history?

## How it works (Mid)
Main types:

- Type 0: never change.
- Type 1: overwrite, no history. Simple, but old reports change retroactively.
- Type 2: add a new row with a new surrogate key plus `valid_from`, `valid_to`, `is_current`. Facts keep pointing at the version that was true when they happened.
- Type 3: keep previous value in an extra column (limited history).
- Type 6: hybrid of 1, 2 and 3.

```
-- SCD2: customer moved from Tehran to Shiraz on 2024-03-01
customer_sk | customer_id | city   | valid_from | valid_to   | is_current
101         | C7          | Tehran | 2020-01-01 | 2024-02-29 | false
245         | C7          | Shiraz | 2024-03-01 | 9999-12-31 | true
```

Type 2 loading: detect changes by comparing a hash of tracked attributes, close the current row, insert the new row. Join facts to the dimension either through the surrogate key stored at load time or with a time-range join `fact.date BETWEEN valid_from AND valid_to`.

## Senior angle & interview tips
Type 2 is the default for attributes that matter historically, but it grows tables, complicates joins and requires deterministic keys for backfills. Pitfalls: overlapping or gapped validity ranges, late-arriving changes that require re-versioning, tracking too many volatile attributes (row explosion; split them into a mini-dimension), and `MERGE` logic done wrong. CDC streams or snapshot diffs can drive SCD2 and some table formats and dbt snapshots automate it.

Interview question: "How do you report sales by the customer's region at the time of sale?" Model answer: SCD2 dimension with validity ranges; the fact stores the surrogate key of the version current at load time, or we do a range join on the event date.
