---
term: "Tombstone"
aliases: ["tombstones", "delete marker", "soft delete marker"]
category: storage
short: "A marker record that represents a deletion, so deletes can propagate through immutable files, logs and replicas."
related: ["lsm-tree", "compaction", "cdc", "upsert"]
---
## In plain words (Junior)
In a shared notebook where nobody may erase anything, to delete an entry you write "entry 42 is cancelled" next to it. Readers see the note and ignore the entry, and later a cleanup can physically remove both. That note is a tombstone.

## How it works (Mid)
Immutable, append-only structures (LSM SSTables, Kafka logs, Parquet files, replicated stores) cannot erase a record in place. A delete is written as a new record, the tombstone, that shadows older versions of the key. Reads that find the tombstone treat the key as absent; compaction later drops the tombstone and the shadowed data once it's safe.

```
# Kafka (log-compacted topic): null value = tombstone for the key
producer.send("users", key=b"user-42", value=None)
# After delete.retention.ms, compaction removes the key entirely.

# Cassandra: DELETE writes a tombstone, removed after gc_grace_seconds
DELETE FROM users WHERE id = 42;
```

"Safe" matters in replicated systems: if a tombstone is purged before every replica saw it, a replica that missed the delete can resurrect ("zombie") the old value. Cassandra therefore keeps tombstones for `gc_grace_seconds` and requires repair within that window; Kafka keeps them for `delete.retention.ms` so consumers can observe the delete. Table formats use delete files (Iceberg position/equality deletes) the same way.

## Senior angle & interview tips
Operational issues: tombstone buildup makes reads slow (scanning many dead cells; Cassandra warns and fails queries over thresholds), queue-like workloads on Cassandra are an anti-pattern, TTL expirations also create tombstones, and wide partitions with many deletes are dangerous. In CDC pipelines, a delete event must be mapped to a downstream delete or tombstone; forgetting it leaves ghost rows. For GDPR, logical deletion isn't enough: the underlying data must eventually be physically removed by compaction or rewrite.

Interview question: "Why can't Cassandra just delete the row?" Model answer: SSTables are immutable and data is replicated, so deletes are written as tombstones that shadow older versions; they're retained for gc_grace so that all replicas learn about them, otherwise deleted data can come back after a repair.
