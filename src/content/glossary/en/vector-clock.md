---
term: "Vector clock"
aliases: ["logical clock","lamport clock","happens-before"]
category: distributed
short: "A logical timestamp structure that detects whether events are causally ordered or concurrent."
related: ["eventual-consistency","event-time","replication"]
---
## In plain words (Junior)
Each person keeps a count of what they know about everyone else’s actions, so you can tell if one event could have influenced another or they happened independently.

## How it works (Mid)
Each node keeps a vector of counters, one per node, increments its own on events, and merges on message receipt. Comparing two vectors tells whether A happened before B, after, or they are concurrent. Lamport clocks use a single counter and give ordering consistent with causality but cannot detect concurrency.

## Senior angle & interview tips
Physical clocks drift, so wall-clock ordering can silently lose writes (last-write-wins). Vector clocks (Dynamo-style) expose conflicts for the application to merge; they grow with participants. Hybrid logical clocks and CRDTs are practical alternatives.

Interview question: "Why not just use timestamps?" Answer: clock skew makes timestamps unreliable for causality; logical clocks capture happens-before.
