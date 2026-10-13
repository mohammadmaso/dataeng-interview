---
term: "Consumer group"
aliases: ["consumer groups", "kafka consumer group"]
category: streaming
short: "A set of consumers that share a topic's partitions so each partition is read by exactly one member of the group at a time."
related: ["consumer-lag", "partition", "backpressure"]
---
## In plain words (Junior)
Think of a pizza shop with several delivery drivers. Orders are grouped by neighborhood, and each neighborhood is handled by only one driver at a time. If a driver leaves or a new one joins, neighborhoods are redistributed. A consumer group works like that: members of the group share the work of reading a topic.

## How it works (Mid)
All consumers with the same `group.id` form a group. Kafka assigns each partition to exactly one member, so maximum parallelism equals the number of partitions (extra consumers idle). The group stores its committed offsets per partition, so after restart it resumes where it stopped. Different groups read the same topic independently, each with its own offsets, which is how several services consume the same events.

```
group.id=billing-service          # same id = same group
enable.auto.commit=false          # commit after processing
partition.assignment.strategy=CooperativeStickyAssignor
session.timeout.ms=45000
max.poll.interval.ms=300000
```

When members join, leave or crash (missed heartbeats or `max.poll.interval.ms` exceeded), the group coordinator triggers a rebalance and reassigns partitions. Ordering is guaranteed per partition only.

## Senior angle & interview tips
Rebalances pause consumption; frequent ones (slow processing exceeding poll interval, flapping pods, autoscaling) cause lag spikes. Mitigate with cooperative rebalancing, static membership (`group.instance.id`) and tuning timeouts. Offset commit strategy sets the delivery guarantee: commit after processing for at-least-once. Processing must be idempotent because a rebalance can re-deliver. Key-based ordering needs the key to map to a stable partition, and adding partitions changes that mapping.

Interview question: "You have 12 partitions and 20 consumers in a group; what happens?" Model answer: only 12 consumers get partitions and 8 idle; to scale beyond that you need more partitions, though that changes key distribution and ordering assumptions.
