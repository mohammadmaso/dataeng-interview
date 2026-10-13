---
term: "Sharding"
aliases: ["shard", "shards", "horizontal partitioning", "sharded"]
category: distributed
short: "Splitting a dataset horizontally across multiple machines by a shard key so each node holds and serves only a subset."
related: ["partition", "replication", "hot-key"]
---
## In plain words (Junior)
A single cashier cannot serve a stadium crowd. You open many counters and assign people to them, say by the first letter of the last name. Sharding does the same for data: no single machine holds everything, and requests go to the machine responsible for that key.

## How it works (Mid)
A shard is a partition of the data placed on its own node (or replica set). A router or the client library maps a key to its shard. Strategies: hash (even distribution, no range scans), range (ordered, supports range queries, hotspot risk), directory/lookup table (flexible, extra hop), and consistent hashing (minimal movement when nodes are added).

```
-- hash sharding
shard = hash(user_id) % N          -- simple, but N change reshuffles everything
shard = consistent_hash(user_id)   -- ring: adding node moves ~1/N of keys

-- range sharding
[a-g] -> shard1, [h-p] -> shard2, [q-z] -> shard3   -- range scans, hotspot risk

-- ClickHouse: Distributed table over shards with a sharding key
ENGINE = Distributed(cluster, db, events_local, cityHash64(user_id))
```

Sharding scales storage and write throughput horizontally, where replication scales read capacity and availability; production systems combine them (each shard has replicas). Used in Cassandra, MongoDB, Elasticsearch, ClickHouse clusters, Vitess, and Kafka (partitions).

## Senior angle & interview tips
Hard parts: choosing the shard key (high cardinality, even access, matches query patterns); hot shards and celebrity keys; cross-shard queries and joins (scatter-gather, higher tail latency); cross-shard transactions (2PC or sagas, or avoid them); and resharding without downtime (virtual nodes/many small logical shards help). A bad key is expensive to change later. Prefer co-locating data that is queried together (same shard key for related tables) so joins stay local.

Interview question: "How do you shard a user-events table?" Model answer: shard by user_id hash so a user's data is co-located and load is even; time-based range keys would hammer the newest shard. Use consistent hashing or many virtual shards for resharding, replicate each shard, and accept scatter-gather for global queries.
