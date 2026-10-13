---
term: "Consensus (Raft / Paxos)"
aliases: ["raft", "paxos", "leader election", "zab", "distributed consensus"]
category: distributed
short: "Protocols that let a cluster agree on a single ordered sequence of decisions despite node failures, using elected leaders and majority quorums."
related: ["quorum", "replication", "cap-theorem"]
---
## In plain words (Junior)
A group of friends in a chat must decide the one restaurant for tonight, even if some people drop off and come back. They pick a coordinator, who proposes a choice; once a majority agree, that choice is final and the coordinator must not contradict it later. Consensus algorithms formalize how computers do this reliably.

## How it works (Mid)
Consensus lets replicas agree on a value or an ordered log (replicated state machine) so every node applies the same commands in the same order. Raft decomposes it into leader election, log replication, and safety. A leader accepts writes and replicates log entries; an entry is committed once stored on a majority; a new election needs a majority vote and the elected leader is guaranteed to have all committed entries.

```
Raft (simplified):
  roles: follower -> candidate -> leader     (term numbers increase on each election)
  election: follower times out, requests votes; majority wins, becomes leader
  replication: client -> leader appends to its log -> sends AppendEntries to followers
  commit: once a MAJORITY has the entry, leader marks it committed and applies it
  safety: a candidate with a less up-to-date log cannot win
Used by: etcd, Consul, Kafka KRaft, CockroachDB, TiKV | Paxos/ZAB: Spanner, ZooKeeper
```

Consensus tolerates `f` crash failures with `2f+1` nodes, and it needs the majority quorum to be reachable, so a partitioned minority can't make progress (CP behavior). It's the foundation of metadata stores and coordination: etcd, ZooKeeper, Kafka's KRaft controller, Raft groups per shard in CockroachDB or TiKV.

## Senior angle & interview tips
Consensus is expensive and not meant for bulk data paths: writes go through one leader and need majority round trips, so systems use it for metadata, locks, leader election and configuration, and use other replication (ISR, quorum) for data. Failure modes: leader churn from flaky networks or GC pauses (tune election timeouts), slow disks (fsync) on a majority stalling commits, and split brain if you bypass the protocol (fencing tokens are needed for external resources). Byzantine faults aren't covered (needs BFT protocols).

Interview question: "Why does ZooKeeper/etcd need an odd number of nodes?" Model answer: progress requires a majority; with 3 nodes you tolerate 1 failure, with 4 you still tolerate 1, so even sizes add cost without extra fault tolerance; and a network split can leave at most one side with a majority.
