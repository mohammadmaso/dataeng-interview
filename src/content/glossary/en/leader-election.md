---
term: "Leader election"
aliases: ["leader follower","primary election"]
category: distributed
short: "Choosing one node to coordinate or accept writes, and replacing it if it fails."
related: ["consensus","replication","quorum"]
---
## In plain words (Junior)
A group picks one captain so decisions are not made twice; if the captain falls ill, they vote for a new one.

## How it works (Mid)
Nodes use a consensus protocol (Raft, Paxos, ZooKeeper/etcd locks) to agree on a single leader, usually with terms or epochs. Followers replicate from the leader; failure is detected via heartbeats and timeouts, triggering a new election.

## Senior angle & interview tips
Danger: two leaders at once (split brain) if timeouts and fencing are wrong. Use fencing tokens so a stale leader cannot write. Elections cause brief unavailability, and timeouts trade detection speed against false positives.

Interview question: "How do you prevent two leaders writing?" Answer: majority quorum plus epoch numbers and fencing tokens.
