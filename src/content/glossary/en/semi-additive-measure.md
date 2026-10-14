---
term: "Semi-additive measure"
aliases: ["additive measure","non-additive measure","snapshot measure"]
category: modeling
short: "A measure that can be summed across some dimensions but not across time."
related: ["fact-table","grain","window-function"]
---
## In plain words (Junior)
Bank balances: adding balances of different accounts on one day makes sense, but adding one account’s balance across 30 days does not.

## How it works (Mid)
Additive measures (revenue) sum across all dimensions. Semi-additive (balance, inventory) sum across all but time, where you take last, average or min/max. Non-additive (ratios, percentages) are never summed; store numerator and denominator and compute the ratio after aggregating.

## Senior angle & interview tips
Periodic snapshot facts commonly carry semi-additive measures. Wrong aggregation here produces silently inflated numbers, a frequent BI bug. Define the aggregation rule in the semantic layer, not in each dashboard.

Interview question: "Average of daily conversion rates vs overall rate?" Answer: sum conversions and visits first, then divide; averaging ratios is wrong.
