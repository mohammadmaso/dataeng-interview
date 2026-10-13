---
term: "Grain"
aliases: ["granularity", "fact grain", "level of detail"]
category: modeling
short: "The precise meaning of one row in a fact table, such as one line item per order; the first decision in dimensional design."
related: ["fact-table", "dimension-table", "star-schema"]
---
## In plain words (Junior)
Imagine a spreadsheet of purchases. One version has a row for each receipt; another has a row for each item on each receipt. Same shop, different level of detail. The grain is the sentence that says exactly what one row represents. Writing it down first prevents confusion about what you are counting and adding up.

## How it works (Mid)
In Kimball-style modeling the process is: choose the business process, declare the grain, then pick dimensions and facts. The grain must be stated in business terms ("one row per order line item per day"), and every column in the fact table must be true at that grain; dimensions attached must be single-valued at that grain.

```
-- Grain: one row per order line item
CREATE TABLE fct_order_items (
  order_id BIGINT, line_no INT,           -- together define the grain
  product_sk BIGINT, customer_sk BIGINT, date_sk INT,
  quantity INT, net_amount DECIMAL(12,2),
  PRIMARY KEY (order_id, line_no)
);
```

Prefer the finest atomic grain you can afford, since you can aggregate up but never drill down. Mixing grains in one table (header-level discount repeated on every line item) breaks sums: `SUM(order_discount)` double counts.

## Senior angle & interview tips
Most "wrong numbers" bugs trace to grain: a join that changes row multiplicity (fan-out) and silently inflates sums. Check the grain before and after each join and test that the declared key is unique. Header-level measures need allocation to line level or a separate fact table. Aggregate tables and materialized views should declare their own coarser grain. Periodic snapshot, transaction and accumulating-snapshot facts have different grain semantics.

Interview question: "Revenue doubled after adding a join; why?" Model answer: the joined table was at a finer grain, so each order row was duplicated (fan-out); I'd aggregate the many side to the original grain first or join on the full grain key, and add a uniqueness test.
