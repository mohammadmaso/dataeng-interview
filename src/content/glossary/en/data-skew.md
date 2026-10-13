---
term: "Data skew"
aliases: ["skew", "skewed data", "data skewness"]
category: batch
short: "Uneven distribution of data across partitions or keys, so a few tasks do most of the work and slow the whole job."
related: ["shuffle", "hot-key", "salting", "broadcast-join", "partition"]
---
## In plain words (Junior)
Picture a supermarket with ten checkout lanes where one lane gets almost all the customers. The store is not busy overall, yet everyone waits for that one lane. Data skew is the same: work is split by key, but a few keys have huge amounts of data, so one worker is overloaded while others sit idle.

## How it works (Mid)
In distributed engines, rows are hashed by key to partitions (for `GROUP BY`, `JOIN`, `repartition`). If one key (e.g. `NULL`, a default user id, a celebrity account) holds a big share of rows, its partition becomes a straggler. Symptoms in the Spark UI: one task running far longer than the median, huge shuffle read for one task, spills or OOM in one executor.

```
-- Find the heavy keys
SELECT user_id, COUNT(*) c FROM events
GROUP BY user_id ORDER BY c DESC LIMIT 20;

# Spark 3: let AQE split skewed shuffle partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

Mitigations: AQE skew-join handling, broadcast the small side, salting hot keys, filtering or isolating `NULL`/default keys, pre-aggregating before the shuffle, choosing a better partitioning key, or splitting the hot keys into a separate path.

## Senior angle & interview tips
Skew is a property of the data, not the cluster, so adding executors does not help; the longest task decides the wall-clock time. Check whether it is key skew (few heavy keys) or partition-size skew from file layout. Salting multiplies the small side and adds complexity; AQE is cheaper but only handles shuffle joins it can split. Skew also hits Kafka (hot partitions from a bad key), Flink keyed state, and sharded databases.

Interview question: "A Spark join job has 199 tasks done and one running for an hour. What do you do?" Model answer: confirm skew via task metrics and key counts, then enable AQE skew join or broadcast the small side; if a few hot keys remain, salt them or process them separately and union the results.
