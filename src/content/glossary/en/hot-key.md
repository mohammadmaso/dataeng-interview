---
term: "Hot key"
aliases: ["hot keys", "hot partition", "hot shard", "hotspot", "celebrity key"]
category: distributed
short: "A key that receives a disproportionate share of traffic or data, overloading the single partition or node that owns it."
related: ["data-skew", "salting", "sharding", "partition"]
---
## In plain words (Junior)
If a hundred thousand fans all try to message one famous person, the line to that person's inbox is jammed while everyone else's inbox is empty. A hot key is the same: one key (a user, product, or date) gets so much traffic that the one machine responsible for it can't keep up.

## How it works (Mid)
Systems assign keys to partitions by hash or range, assuming even load. If access is skewed (Zipf-like), the owner of a popular key saturates CPU, memory or I/O while others idle. It shows up in many places: Kafka partitions and consumer lag, Spark/Flink keyed operators, Cassandra/DynamoDB partitions (throttling), Redis single-key contention, and time-range keys where the newest range takes all writes.

```
-- Kafka: all events of a celebrity share one partition
partition = hash("user:celebrity") % 12    -- one partition, one consumer thread

-- Mitigation: split the key
key = "user:celebrity#" + (random() % 16)  -- 16 sub-keys, merge downstream

-- Detect
SELECT key, COUNT(*) c FROM events GROUP BY key ORDER BY c DESC LIMIT 10;
```

Mitigations: split the key into sub-keys (salting) and merge results; replicate hot reads (caching, read replicas); batch/aggregate writes before they hit the key (local pre-aggregation, counters sharded across sub-keys); choose a composite partition key (user + time bucket); and handle the top-N heavy keys separately.

## Senior angle & interview tips
Detect first: per-key frequency sampling, per-partition metrics, and tail latency by partition. Distinguish read-hot from write-hot as fixes differ. Splitting a key loses single-key ordering and requires a merge step, so apply it only to known heavy keys (dynamic salting) rather than everywhere. For Kafka, a key-less or round-robin producer avoids hot partitions when ordering by key isn't needed. DynamoDB-style adaptive capacity helps but not forever. Design the key for access patterns before data grows.

Interview question: "A single Cassandra partition is overloaded; how do you fix the data model?" Model answer: add a bucket (time or hash suffix) to the partition key to bound partition size and spread load, and accept scatter-gather reads across buckets for that entity.
