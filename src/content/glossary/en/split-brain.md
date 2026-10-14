---
term: "Split brain"
aliases: ["network partition","partitioned cluster"]
category: distributed
short: "A failure where a cluster splits and both halves think they are in charge."
related: ["leader-election","quorum","cap-theorem"]
---
## In plain words (Junior)
Two halves of a team lose contact and each elects its own boss; both start giving conflicting orders.

## How it works (Mid)
A network partition separates nodes; without a majority rule, each side may elect a leader and accept writes, producing divergent data that is hard to merge. Quorum-based systems let only the majority side continue.

## Senior angle & interview tips
Prevention: odd number of nodes, majority quorum, fencing, and STONITH in HA clusters. Detection and reconciliation (vector clocks, CRDTs, last-write-wins with caveats) are needed for AP systems.

Interview question: "Why odd node counts?" Answer: an even split can leave no majority; odd counts guarantee one side can reach quorum.
