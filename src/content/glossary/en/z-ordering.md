---
term: "Z-ordering"
aliases: ["z-order","space-filling curve","clustering"]
category: lakehouse
short: "Sorting data along a space-filling curve so multiple columns cluster together in files."
related: ["partition","predicate-pushdown","small-files-problem","delta-lake"]
---
## In plain words (Junior)
Shelving books so that items close in both author and year end up on nearby shelves, letting you skip most shelves for either search.

## How it works (Mid)
Z-order interleaves the bits of several column values, so rows with similar values on all of them land in the same files. Min/max statistics then skip many files for filters on any of those columns. Delta Lake OPTIMIZE ... ZORDER BY and Iceberg sort/z-order rewrite files this way. Liquid clustering is the incremental successor.

## Senior angle & interview tips
Works best with 2-4 columns of moderate-high cardinality; benefits fall as columns increase. It rewrites files, so it costs compute and must be scheduled. It complements, not replaces, partitioning on low-cardinality columns like date.

Interview question: "Filters hit user_id and event_type unpredictably; how do you speed them?" Answer: partition by date, Z-order within by user_id, event_type.
