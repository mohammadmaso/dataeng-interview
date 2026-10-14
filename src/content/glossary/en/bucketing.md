---
term: "Bucketing"
aliases: ["hash bucketing","clustering by hash"]
category: batch
short: "Pre-grouping rows into a fixed number of hash buckets by a column to avoid later shuffles."
related: ["partition","sort-merge-join","shuffle","data-skew"]
---
## In plain words (Junior)
Pre-sorting mail into a fixed set of numbered boxes by customer id, so two stacks sorted the same way can be matched box to box.

## How it works (Mid)
Rows are hashed on a key into N buckets stored as separate files, optionally sorted within each bucket. Two tables bucketed the same way on the join key (same N) can be joined without a shuffle. Spark, Hive and Trino support it in different flavors.

## Senior angle & interview tips
Bucket count is fixed at write time and mismatched counts or types disable the optimization. Cost is write-time complexity. Unlike partitioning (directories by value), bucketing suits high-cardinality keys.

Interview question: "How do you speed up a repeated join of two huge tables?" Answer: bucket and sort both by join key, so the join needs no shuffle.
