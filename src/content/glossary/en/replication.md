---
term: "Replication"
aliases: ["replica", "replicas", "replicated", "leader-follower", "primary-replica", "read replica"]
category: distributed
short: "Keeping copies of data on multiple nodes for fault tolerance and read scalability, synchronously or asynchronously."
related: ["quorum", "cap-theorem", "sharding", "wal"]
---
## In plain words (Junior)
Keeping a spare copy of an important document in another building means a fire in one place does not destroy it, and two people can read it at the same time. Replication copies data to several machines so the system survives failures and more readers can be served.

## How it works (Mid)
The most common topology is single-leader: writes go to the leader, which streams its log (WAL/binlog) to followers that apply it in order. Followers serve reads and take over on failure. Synchronous replication waits for a follower acknowledgement (durability, higher latency); asynchronous returns immediately (fast, but replication lag and possible loss of recent commits on failover); semi-sync waits for at least one.

```
Single-leader:   clients write -> LEADER --(log stream)--> FOLLOWER 1, FOLLOWER 2
                 reads: leader (fresh) or followers (maybe stale)
Multi-leader:    each region has a leader; conflicts need resolution (LWW, CRDT)
Leaderless:      write to N nodes, succeed with W acks; read R nodes (quorum)

sync:  commit waits for replica ack  -> no loss on failover, higher latency
async: commit returns immediately    -> replication lag, may lose last writes
```

Alternatives: multi-leader (multi-region writes, conflict resolution needed) and leaderless with quorums. Kafka replicates each partition with a leader and an in-sync replica set (ISR); HDFS replicates blocks; ClickHouse uses ReplicatedMergeTree coordinated via Keeper.

## Senior angle & interview tips
Problems to know: replication lag causes stale reads (read-your-writes violations; fix with sticky reads to leader, or lag-aware routing), split brain after failover without fencing, data loss on async failover, and replica divergence. Failover needs reliable failure detection and leader election (consensus) plus fencing of the old leader. Replication does not replace backups: a bad `DELETE` or corruption replicates instantly. Replication provides availability and read scale, sharding provides write and storage scale.

Interview question: "Replica reads show old data right after a write; why and what do you do?" Model answer: asynchronous replication lag; route the user's reads to the leader for a short window or until the replica's position passes the write's log position, or use synchronous/quorum reads when strict freshness is required.
