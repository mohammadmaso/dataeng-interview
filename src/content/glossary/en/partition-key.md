---
term: "Partition key"
aliases: ["shard key","message key","hash key"]
category: distributed
short: "The field whose value decides which partition a record goes to."
related: ["partition","sharding","hot-key","data-skew"]
---
## In plain words (Junior)
Like sorting mail into bins by zip code: the key decides the bin, so all letters for one zip code end up together.

## How it works (Mid)
The system hashes (or ranges) the key to choose a partition. In Kafka, same key means same partition means preserved order for that key. In databases and warehouses, the key controls data placement, parallelism and which queries are cheap.

## Senior angle & interview tips
A good key has high cardinality, even distribution and matches the dominant access pattern. Bad keys cause hot partitions (celebrity user, null keys) and skew. Changing the key later means repartitioning all data.

Interview question: "How do you pick a Kafka key for user events?" Answer: user id for per-user ordering, with salting or sub-keys if a few users dominate traffic.
