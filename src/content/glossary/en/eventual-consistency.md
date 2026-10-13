---
term: "Eventual consistency"
aliases: ["eventually consistent", "base", "read-your-writes", "strong consistency", "linearizability"]
category: distributed
short: "If no new writes occur, all replicas converge to the same value eventually, but reads may be stale in the meantime."
related: ["cap-theorem", "replication", "quorum"]
---
## In plain words (Junior)
You post a photo, then refresh on another phone and it isn't there yet; a minute later it shows up. Different servers learn about the change at slightly different times but everyone ends up agreeing. That's eventual consistency, and it's the price paid for speed and availability.

## How it works (Mid)
With asynchronous replication, a write lands on one replica first and propagates later. During that window, reads from other replicas return old values. "Eventually" means convergence is guaranteed only if updates stop and the system keeps delivering them (via anti-entropy, read repair, gossip, or log shipping). Conflicts from concurrent writes are resolved by last-write-wins timestamps, version vectors, or CRDTs.

```
Spectrum (strong -> weak):
linearizable        every read sees the latest write (single-copy illusion)
sequential          all see the same order of operations
causal              cause precedes effect everywhere
read-your-writes    you see your own writes
monotonic reads     never see data go backwards in time
eventual            replicas converge if writes stop
```

Stronger session guarantees can be layered on: read-your-writes, monotonic reads, and causal consistency are often enough for user-facing apps. Linearizability (strong consistency) needs coordination (quorum/consensus), costing latency and availability during partitions.

## Senior angle & interview tips
Eventual consistency is a spectrum, not one thing; say which guarantee you need. LWW with clock skew can silently drop writes; CRDTs and merge functions are safer for counters and sets. For data pipelines, consumers of a lake table or replicated store read an eventually consistent view: dashboards may briefly mismatch, and join of two tables updated at different times can be inconsistent. S3 now provides strong read-after-write consistency, but listing-based assumptions in other stores may not hold. Design idempotent, commutative updates so ordering doesn't matter.

Interview question: "A user updates a profile and sees old data; how do you fix it without strong consistency everywhere?" Model answer: provide read-your-writes by routing that user's reads to the leader (or a replica caught up to their write's version) for a short window, keeping eventual consistency for everyone else.
