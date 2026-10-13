---
term: "Log compaction (Kafka)"
aliases: ["compacted topic", "log compacted topic", "cleanup.policy=compact", "changelog topic"]
category: streaming
short: "A Kafka retention mode that keeps at least the latest record for every key, turning a topic into a durable changelog or table snapshot."
related: ["tombstone", "compaction", "consumer-group"]
---
## In plain words (Junior)
A phone's contact list keeps only the latest number for each friend, not every number they ever had. A compacted Kafka topic works the same: instead of expiring old messages by time, it keeps the most recent message for each key, so you can always rebuild the current state.

## How it works (Mid)
With `cleanup.policy=delete`, segments are removed after a time or size limit. With `compact`, a background cleaner removes older records of the same key from closed segments, retaining the latest value; a record with a null value is a tombstone that eventually deletes the key. Offsets don't change and ordering is preserved; the active segment isn't compacted.

```
kafka-topics.sh --create --topic user-profiles --partitions 6 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.5 \
  --config delete.retention.ms=86400000

offset: 0   1   2   3   4   5   6
key:    A   B   A   C   B   A   C        before: A@0 B@1 A@2 C@3 B@4 A@5 C@6
value:  v1  v1  v2  v1  v2  v3  null     after:  B@4 A@5 (C@6 tombstone, later removed)
```

Use cases: changelog of a database table (CDC to a compacted topic), Kafka Streams state-store changelogs, consumer offsets (`__consumer_offsets`), and reference/config data a new consumer can read from the beginning to rebuild a full snapshot (KTable).

## Senior angle & interview tips
Compaction is not immediate or guaranteed to remove duplicates: readers can still see multiple records for a key until cleaning runs, so consumers must still apply last-write-wins. Tombstones live for `delete.retention.ms` so slow consumers can observe deletes. Messages need keys (null-keyed records can't be compacted). Compaction needs the whole key space's latest values retained, so topic size scales with distinct keys, not event count. It doesn't give history; use `compact,delete` to also bound by time. Bootstrapping a consumer from a large compacted topic can take time.

Interview question: "How do you let a new service rebuild the current customer table from Kafka?" Model answer: publish customer changes keyed by id to a compacted topic with tombstones for deletes; the new service reads from the earliest offset to the end, applying last-write-wins per key, then continues streaming updates.
