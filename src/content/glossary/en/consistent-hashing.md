---
term: "Consistent hashing"
aliases: ["hash ring", "virtual nodes", "vnodes", "rendezvous hashing"]
category: distributed
short: "A hashing scheme where adding or removing a node moves only about 1/N of the keys, instead of reshuffling almost everything."
related: ["sharding", "partition", "hot-key"]
---
## In plain words (Junior)
Suppose books are assigned to shelves by a formula that depends on the number of shelves. Add one shelf and the formula changes for nearly every book, so you re-shelve the whole library. Consistent hashing uses a smarter arrangement where adding a shelf only moves a small portion of the books.

## How it works (Mid)
Keys and nodes are hashed onto the same circular space. A key belongs to the next node clockwise. When a node joins, it takes over only part of one neighbor's range; when it leaves, its keys go to the next node. Virtual nodes (each physical node appears at many ring positions) smooth out imbalance and let heterogeneous machines take proportional shares.

```
Naive: node = hash(key) % N      -> N from 3 to 4 moves ~75% of keys

Ring: place nodes (and many virtual nodes each) at hash(node_id) on a 0..2^32 ring.
      key goes to the first node clockwise from hash(key).
      add a node -> it takes over only the arc just before it (~1/N of keys)

  vnodes: each physical node owns e.g. 128-256 small arcs -> even load, smooth rebalancing
```

Used in Dynamo-style stores (Cassandra, Riak), caches (memcached clients, CDN request routing), load balancers, and Kafka-like sharding of consumers. Replication places copies on the next R nodes clockwise. Rendezvous (HRW) hashing and jump hash are alternatives with similar minimal-movement properties.

## Senior angle & interview tips
Trade-offs: needs a membership/ring metadata view that all clients agree on, and data must be streamed to the new node (rebalancing load, hinted handoff). Pure hashing ignores load, so hot keys still hot-spot. Range partitioning (HBase, Spanner, Cockroach) trades hash evenness for range scans and dynamic splits. Kafka doesn't use consistent hashing: key to partition is `hash % partitions`, which is why increasing partitions breaks key locality.

Interview question: "Why consistent hashing instead of hash mod N for a cache cluster?" Model answer: with mod N, resizing remaps most keys, causing a mass cache miss storm; consistent hashing remaps only ~1/N keys, and virtual nodes keep load even.
