---
term: "Sort-merge join"
aliases: ["sort merge join", "merge join", "hash join", "shuffle hash join", "nested loop join"]
category: batch
short: "A join that sorts (and shuffles) both inputs on the join key, then merges them in one pass; scales to two large tables."
related: ["shuffle", "broadcast-join", "data-skew"]
---
## In plain words (Junior)
Two sorted lists of names, say attendees and paid-ups, can be compared by walking down both at once: if names match you record it, otherwise you advance the one that is behind. No need to search the whole second list for each name. That's a merge join, after first sorting the lists.

## How it works (Mid)
Algorithms: nested loop (compare every pair, fine for tiny inputs or with an index), hash join (build a hash table on the smaller side, probe with the other, fast but memory-bound), sort-merge (sort both on the key, then merge; handles data larger than memory since sorting can spill). In distributed engines, sort-merge first hash-partitions both sides by key (shuffle) so equal keys land together, then sorts within each partition and merges.

```
-- Spark physical plan
SortMergeJoin [user_id], [user_id], Inner
:- Sort [user_id ASC]
:  +- Exchange hashpartitioning(user_id, 200)      <- shuffle left
+- Sort [user_id ASC]
   +- Exchange hashpartitioning(user_id, 200)      <- shuffle right

-- Merge step: two sorted cursors advance together, emitting matches
```

It's Spark's default for large-large equi-joins. Cost: shuffle + sort of both sides; benefit: robust to memory limits and produces sorted output. If data is already bucketed and sorted by the join key in the same way, the shuffle and sort can be skipped.

## Senior angle & interview tips
Choosing between strategies: broadcast hash join if one side is small; shuffle hash join when one side is substantially smaller but too big to broadcast and fits per-partition in memory; sort-merge for two large sides or non-equi range joins (with extra work). Skew breaks sort-merge since a hot key lands in one partition (see data skew, AQE skew join). Bucketing both tables by the join key avoids repeated shuffles for recurring joins. Non-equi joins (inequality, `BETWEEN`) can fall back to broadcast nested loop or cartesian products, which are dangerous.

Interview question: "How does Spark join two 1 TB tables?" Model answer: sort-merge join: both sides shuffled by the key into the same number of partitions, sorted per partition, then merged; I'd check skew, partition count, and consider bucketing if the join recurs.
