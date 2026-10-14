---
term: "Rebalancing"
aliases: ["consumer rebalance","rebalance"]
category: distributed
short: "Redistributing partitions or shards among workers when membership changes."
related: ["consumer-group","partition","consumer-lag"]
---
## In plain words (Junior)
When a new cashier joins or one goes home, the queue lines are reassigned so work stays fair. During the shuffle, service pauses briefly.

## How it works (Mid)
In Kafka, joins, leaves or crashes in a consumer group trigger a rebalance that reassigns partitions; classic "stop the world" rebalancing pauses all consumers, while cooperative/incremental rebalancing moves only affected partitions. Distributed stores rebalance data when nodes are added.

## Senior angle & interview tips
Frequent rebalances (flapping from slow processing past max.poll.interval, bad session timeouts, rolling deploys) cause lag spikes. Mitigate with static membership, cooperative protocol, and keeping poll loops short. Data rebalancing moves bytes and needs throttling.

Interview question: "Consumer lag spikes after each deploy. Why?" Answer: rolling restarts trigger rebalances; use static group membership and cooperative rebalancing.
