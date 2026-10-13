---
term: "Offset (Kafka)"
aliases: ["offsets", "committed offset", "log end offset", "offset commit"]
category: streaming
short: "A record's sequential position within a partition; consumers track and commit offsets to resume where they left off."
related: ["consumer-group", "consumer-lag", "exactly-once", "partition"]
---
## In plain words (Junior)
An offset is like a bookmark in a very long book: it records the page where you stopped. Each reader (consumer group) keeps its own bookmark, so after a break it continues from the right page instead of the beginning.

## How it works (Mid)
Each partition is an append-only log; every record gets a monotonically increasing offset. Offsets are unique only within a partition (the full address is topic, partition, offset). A consumer group stores its committed offset per partition in `__consumer_offsets` (or externally). On restart or rebalance it resumes from the committed offset, so the commit timing defines the delivery semantics.

```
partition 0:  [0][1][2][3][4][5][6][7]
                          ^committed=4   ^ current position=6   ^ log end=8
                          next fetch after restart: starts at 4 (re-reads 4,5 => at-least-once)

consumer.poll() -> process(records) -> consumer.commitSync()   # commit AFTER processing
enable.auto.commit=false
auto.offset.reset=earliest    # used only when no committed offset exists
```

Commit before processing gives at-most-once (crash loses records); commit after processing gives at-least-once (crash causes reprocessing); committing offsets atomically with output (Kafka transactions, or storing offsets in the sink's database) gives exactly-once effects. Consumers can also seek to arbitrary offsets or timestamps to replay.

## Senior angle & interview tips
Common pitfalls: `auto.offset.reset=latest` silently skipping data for a new group; committing offsets for records not yet fully processed (async processing with auto-commit); expired offsets when a group is idle longer than `offsets.retention.minutes`; and offsets not being comparable across partitions or topics (so there's no global order). Log truncation by retention can make a stored offset out of range. Offset gaps exist due to transactions markers and compaction, so don't assume contiguity or use offset difference as exact message count.

Interview question: "How do you reprocess the last 6 hours of a topic?" Model answer: stop the consumer group, reset its offsets by timestamp (`kafka-consumer-groups --reset-offsets --to-datetime`) or use a new group id, then restart; the sink must be idempotent so replays don't duplicate.
