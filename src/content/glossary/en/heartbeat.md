---
term: "Heartbeat"
aliases: ["health check","failure detector","keepalive"]
category: distributed
short: "A periodic signal a node sends to prove it is still alive."
related: ["leader-election","consensus","split-brain"]
---
## In plain words (Junior)
Like a pulse: if it stops for too long, others assume the node is down.

## How it works (Mid)
Nodes send heartbeats at intervals; a monitor or peers declare failure after a timeout of missed beats. Used by Kafka brokers and consumers (session timeout), Raft leaders, YARN and Kubernetes.

## Senior angle & interview tips
Failure detection is probabilistic in asynchronous networks: a slow node looks dead. Short timeouts mean fast failover but false alarms (and rebalances); long ones mean slow recovery. Phi-accrual detectors adapt to variance.

Interview question: "Why does a GC pause trigger a Kafka rebalance?" Answer: the consumer missed heartbeats beyond session timeout and was declared dead.
