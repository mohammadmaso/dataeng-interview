---
term: "Semantic layer"
aliases: ["metrics layer","metric store","headless bi"]
category: modeling
short: "A shared layer that defines business metrics and dimensions once for every tool to use."
related: ["star-schema","data-catalog","semi-additive-measure"]
---
## In plain words (Junior)
A company dictionary where "revenue" and "active user" have one official definition, so every dashboard shows the same number.

## How it works (Mid)
Metrics (revenue = sum of paid order amounts minus refunds), dimensions, joins and filters are declared in code or config (dbt Semantic Layer, Cube, LookML). BI tools, notebooks and APIs query the layer, which generates correct SQL.

## Senior angle & interview tips
It solves metric drift and duplicate logic across dashboards, and encodes aggregation rules for semi-additive measures. Costs: another system to govern, performance tuning via pre-aggregations, and tool lock-in questions.

Interview question: "Two dashboards show different revenue. How do you prevent it?" Answer: define the metric once in a semantic layer and make all consumers query it.
