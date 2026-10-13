---
term: "Shuffle"
aliases: ["shuffles", "shuffling", "exchange"]
category: batch
short: "Redistributing data across the network between nodes so rows with the same key end up together, e.g. for joins and group-bys."
related: ["data-skew", "broadcast-join", "partition", "predicate-pushdown"]
---
## In plain words (Junior)
Imagine a class where every student holds cards of different colors, and the teacher asks everyone to gather with others holding the same color. Everyone walks around the room and swaps cards. That crowd movement is a shuffle: data moves between machines so related rows end up in one place. It is often the most expensive part of a job.

## How it works (Mid)
Operations like `groupBy`, `join`, `distinct`, `orderBy` and `repartition` need all rows of the same key on the same node. Each task writes its output to local disk split by target partition (map side); downstream tasks fetch their pieces over the network (reduce side). This involves serialization, disk I/O, network transfer and possible spilling, which is why it dominates job time.

```
df.groupBy("country").sum("amount")        # shuffle: hash by country
df1.join(df2, "user_id")                   # shuffle both sides (sort-merge)
df.repartition(200, "user_id")             # explicit shuffle
df.coalesce(10)                            # narrow: no full shuffle
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

Ways to reduce it: filter and project early, pre-aggregate (map-side combine), broadcast small tables, bucket/co-partition data you join often, and tune partition counts (too few means big, spilling partitions; too many means scheduling overhead and tiny files).

## Senior angle & interview tips
Reading the plan for `Exchange` nodes (Spark) or `Exchange`/`Redistribute` steps (Trino, StarRocks) is the fastest way to find shuffles. Shuffle cost scales with data volume, not row count, so wide rows and un-pruned columns hurt. Failure modes: fetch failures from lost executors, skew (see data skew), small shuffle partitions causing millions of tiny blocks, and external shuffle service or disk pressure. AQE can coalesce small post-shuffle partitions automatically.

Interview question: "Why is a join slow?" Model answer: it usually triggers a shuffle of both sides; I check the plan, filter early, broadcast the smaller side if it fits memory, and verify partition counts and skew.
