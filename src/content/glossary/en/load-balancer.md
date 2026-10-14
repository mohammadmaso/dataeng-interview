---
term: "Load balancer"
aliases: ["load balancing","reverse proxy"]
category: infra
short: "A component that spreads incoming requests across multiple servers."
related: ["consistent-hashing","sharding","replication"]
---
## In plain words (Junior)
A host at a restaurant sending each new party to a table with free seats, so no waiter is overloaded.

## How it works (Mid)
Strategies: round robin, least connections, hash-based (sticky by key), weighted. Layer 4 balances TCP; layer 7 understands HTTP and can route by path or header. Health checks remove failing servers.

## Senior angle & interview tips
The balancer itself must not be a single point of failure (redundant pairs, DNS or anycast). Consistent hashing reduces reshuffling when servers change. For stateful backends (caches, shards), prefer key-based routing.

Interview question: "L4 vs L7?" Answer: L4 is faster and protocol-agnostic; L7 enables content-based routing, TLS termination and retries at higher cost.
