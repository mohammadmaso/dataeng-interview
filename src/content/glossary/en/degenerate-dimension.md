---
term: "Degenerate dimension"
aliases: ["degenerate dim"]
category: modeling
short: "A dimension value stored directly in the fact table, with no dimension table of its own."
related: ["fact-table","dimension-table","grain"]
---
## In plain words (Junior)
An order number printed on each line item: it groups rows together but has no extra details worth a separate table.

## How it works (Mid)
Identifiers such as order number, invoice number or transaction id live as columns in the fact table. They are useful for grouping line items and drill-through, but have no other attributes, so a separate dimension would be empty.

## Senior angle & interview tips
Common in transaction-grain facts. If attributes later appear (order status, channel), promote it to a real dimension. Don’t forget it is high-cardinality, so avoid using it as a partition key.

Interview question: "Where do you store order_id in a star schema?" Answer: as a degenerate dimension on the order-line fact.
