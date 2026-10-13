---
term: "Quorum"
aliases: ["quorums", "majority quorum", "read quorum", "write quorum"]
category: distributed
short: "The minimum number of replicas that must agree on a read or write; a majority quorum ensures any two quorums overlap."
related: ["replication", "cap-theorem", "two-phase-commit"]
---
## In plain words (Junior)
A committee decides by majority vote. If 3 of 5 members agree, the decision stands, even if two members are absent. Any two majorities share at least one member, so two groups can never make contradicting decisions. A quorum is that rule applied to replicated data.

## How it works (Mid)
With `N` replicas, a quorum is usually a majority, `floor(N/2)+1`. In leaderless systems (Dynamo, Cassandra) you tune `W` and `R`; `R + W > N` guarantees the read set intersects the latest write set, so at least one replica has the newest value (resolved by timestamps or version vectors). In consensus systems (Raft, Paxos, ZooKeeper/ZAB, Kafka KRaft) a leader needs acknowledgements from a majority before committing, and a new leader election needs a majority vote.

```
N = replicas, W = write acks required, R = replicas read
Rule:  R + W > N   -> read set overlaps the latest write set

N=3, W=2, R=2  -> tolerates 1 node down, reads see latest committed write
N=3, W=3, R=1  -> fast reads, writes fail if any node is down
N=3, W=1, R=1  -> fast, but can read stale data (eventual consistency)

Cassandra: CONSISTENCY QUORUM = floor(N/2)+1
```

Majority quorums tolerate `f` failures with `2f+1` nodes: 3 nodes tolerate 1, 5 tolerate 2. That is why clusters have odd sizes; adding a fourth node to three adds cost without more fault tolerance.

## Senior angle & interview tips
Quorum choices trade availability and latency against consistency. Overlap alone is not linearizability: sloppy quorums with hinted handoff, concurrent writes, clock skew and partial failed writes can still expose stale or conflicting data, so quorum reads need read repair and anti-entropy. Network partitions: the minority side cannot form a quorum and stops accepting writes (CP behavior). Kafka's `acks=all` with `min.insync.replicas` is an ISR-based variant: writes need enough in-sync replicas, not a pure majority.

Interview question: "Why 3 or 5 nodes for ZooKeeper or etcd?" Model answer: a majority quorum is needed to commit, so 3 nodes tolerate one failure and 5 tolerate two; even sizes add cost but not extra fault tolerance, and larger clusters slow writes.
