---
term: "SQL join types"
aliases: ["inner join","left join","outer join","full outer join","cross join","join"]
category: sql
short: "Ways to combine rows of two tables: inner, left, right, full outer, cross, semi and anti."
related: ["sort-merge-join","broadcast-join","cardinality"]
---
## In plain words (Junior)
Matching two guest lists. Inner keeps only names on both; left keeps everyone from the first list and fills blanks for non-matches; full keeps everyone from either.

## How it works (Mid)
INNER returns matches; LEFT/RIGHT keep all rows of one side with NULLs for missing matches; FULL OUTER keeps both; CROSS is the Cartesian product; SEMI (EXISTS) filters one side by presence in the other without duplicating; ANTI (NOT EXISTS) keeps rows without a match. Physical algorithms: hash, sort-merge, nested loop, broadcast.

## Senior angle & interview tips
The classic bugs: join fan-out (duplicating rows when the key is not unique on one side), filters in WHERE on the nullable side turning a LEFT join into an inner one, and NULL keys never matching. Always know the grain of both inputs and validate row counts.

Interview question: "Why did the revenue double after a join?" Answer: the right side had several rows per key (fan-out); aggregate first or join at the correct grain.
