---
term: "CAP theorem"
aliases: ["cap", "pacelc", "brewer's theorem"]
category: distributed
short: "During a network partition a replicated system must choose between consistency (linearizability) and availability; it cannot have both."
related: ["quorum", "replication", "sharding"]
---
## In plain words (Junior)
Two branches of a bank lose their phone line to each other. A customer asks one branch for their balance. The branch can refuse until it can verify with the other (consistent but unavailable), or answer with what it knows, which may be stale (available but maybe inconsistent). CAP says that during such a split you cannot have both.

## How it works (Mid)
The formal statement: in an asynchronous network with partitions, no system can provide both linearizable consistency (every read sees the latest write, as if one copy) and availability (every non-failing node answers every request). Partition tolerance is not optional in distributed systems, so "pick two of three" is misleading: the choice is C vs A only while a partition exists.

```
Partitions WILL happen, so the real choice is during a partition:

CP: refuse/lose availability on the minority side to stay consistent
    (etcd, ZooKeeper, HBase, Spanner-like, Kafka with min.insync)
AP: keep answering on both sides, reconcile later -> stale/conflicting reads
    (Cassandra at low consistency, Dynamo-style, DNS)

PACELC: if Partition -> A or C; Else (normal) -> Latency or Consistency
```

Note that C here means linearizability, not the C in ACID. Many systems are tunable (Cassandra consistency levels). PACELC extends CAP: even without partitions there is a trade-off between latency and consistency, since synchronous replication costs round trips.

## Senior angle & interview tips
Common interview mistakes: calling a single-node database "CA" and treating CAP as a permanent label on a product. Real systems sit on a spectrum per operation and configuration. Think in terms of what happens on the minority side of a partition, what staleness is acceptable (eventual, read-your-writes, monotonic reads), and how conflicts are resolved (LWW, CRDTs, vector clocks, application merge). For data engineering: a metadata store, orchestration lock or leader election wants CP; a metrics ingestion path or a cache may prefer AP.

Interview question: "Is MongoDB / Cassandra CP or AP?" Model answer: it depends on configuration; with majority writes and reads it behaves CP-ish (minority side rejects), with low consistency levels it favors availability and may return stale data, so I describe behavior during a partition rather than a label.
