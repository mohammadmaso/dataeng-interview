---
term: "Gossip protocol"
aliases: ["epidemic protocol","gossip"]
category: distributed
short: "Nodes periodically exchange state with random peers so information spreads like a rumor."
related: ["eventual-consistency","heartbeat","replication"]
---
## In plain words (Junior)
Everyone tells a few random friends what they know; soon the whole town knows without a central announcer.

## How it works (Mid)
Each round, a node picks random peers and shares membership and state, merging what it learns. Information spreads in O(log N) rounds, with no single coordinator. Used by Cassandra, Consul and Dynamo-style systems for membership and failure detection.

## Senior angle & interview tips
It is scalable and fault tolerant but only eventually consistent, and generates constant background traffic. Convergence time and message size must be tuned for large clusters.

Interview question: "How does Cassandra learn which nodes are up?" Answer: gossip with a phi-accrual failure detector.
