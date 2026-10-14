---
term: "Factless fact table"
aliases: ["factless fact","coverage table"]
category: modeling
short: "A fact table that records events or coverage with no numeric measures."
related: ["fact-table","dimension-table","grain"]
---
## In plain words (Junior)
An attendance sheet: it records that a student was in a class on a day, with no amounts attached.

## How it works (Mid)
It contains only dimension keys. Event type records that something happened (student attended class); coverage type records what could happen (products on promotion), so you can find what did not happen by anti-joining with the actual facts.

## Senior angle & interview tips
Counting rows gives the measure. The coverage pattern answers "which promoted products had no sales" which a normal fact cannot.

Interview question: "How do you report products on promotion that never sold?" Answer: anti-join the promotion coverage factless table to sales.
