---
term: "Broadcast join"
aliases: ["broadcast hash join", "map-side join", "replicated join", "broadcast"]
category: batch
short: "A join where the small table is copied to every worker so the large table never needs to be shuffled."
related: ["shuffle", "data-skew", "predicate-pushdown"]
---
## In plain words (Junior)
Suppose you must match 10 million customer cards against a short list of 20 VIP names. Instead of moving all the cards to one place, you give each clerk a copy of the short list and they check their own cards. Broadcasting means sending the small table to everyone so the big one stays where it is.

## How it works (Mid)
Normal distributed joins shuffle both sides by join key. In a broadcast hash join, the small side is collected, built into an in-memory hash table and sent to every executor; each task streams its partition of the large table and probes the hash table locally. The big side needs no shuffle and skew on it does not matter.

```
from pyspark.sql.functions import broadcast
fact.join(broadcast(dim_country), "country_id")

-- SQL hint
SELECT /*+ BROADCAST(d) */ * FROM fact f JOIN dim_country d ON f.country_id = d.id;

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)
```

Spark chooses it automatically when a side's estimated size is below the threshold, and adaptive execution can convert a sort-merge join into a broadcast at runtime when actual size is small. Works for inner, left (broadcast right) joins; not for full outer. Trino/StarRocks have broadcast vs partitioned distribution choices as well.

## Senior angle & interview tips
Risks: the small side is not actually small (stale statistics or filters applied later), causing driver or executor OOM or a slow collect; broadcasting a table repeatedly in a loop; and too many executors multiplying network cost. The driver typically collects the table first, so memory there matters. Alternatives when both are large: shuffle hash or sort-merge join, bucketing both sides, or salting for skew. Dimension tables in star schemas are the classic broadcast candidates.

Interview question: "When would you avoid a broadcast join?" Model answer: when the build side is large or its size estimate is unreliable, since memory pressure and network copy to every executor outweigh the saved shuffle; then I'd use sort-merge with proper partitioning, bucketing or runtime filters.
