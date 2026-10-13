---
term: "Partition"
aliases: ["partitions", "partitioning", "partitioned"]
category: distributed
short: "A subset of a dataset split by a key or range, enabling parallelism, pruning and independent management."
related: ["sharding", "data-skew", "consumer-group", "small-files-problem"]
---
## In plain words (Junior)
Imagine a library that organizes books into shelves by first letter. If you want a book starting with M, you go straight to the M shelf instead of scanning the whole library, and several librarians can work on different shelves at once. Partitioning splits big data the same way so work can be skipped or done in parallel.

## How it works (Mid)
The word is used at three levels, which interviews often mix up:

- Table/storage partitioning: directory or segment per value (date, region) so queries with a filter on that column prune others. Choose a column of moderate cardinality; too fine causes many small files.
- Processing partitions: chunks of data a Spark/Flink task handles; count controls parallelism and memory per task.
- Streaming partitions: Kafka topic partitions are the unit of ordering, parallelism and consumer assignment.

```
-- Table partitioning (pruning)
CREATE TABLE events (...) PARTITIONED BY (dt DATE);
SELECT count(*) FROM events WHERE dt = '2024-05-01'; -- reads 1 partition

# Kafka: key decides partition (ordering per key only)
partition = hash(key) % num_partitions

# Spark
df.repartition(200, "user_id")
```

Partitioning by hash spreads load evenly but loses range locality; range partitioning keeps order and supports range scans but risks hotspots (time-based keys hammer the latest partition).

## Senior angle & interview tips
A good partition key matches the dominant filter and keeps partitions of similar, reasonably large size. Mistakes: partitioning by a high-cardinality column like `user_id` (millions of tiny partitions), skew, partitioning on a column nobody filters, and forgetting that Kafka partition count cannot easily be reduced and changing it breaks key-to-partition mapping. Modern table formats offer hidden partitioning and partition evolution (Iceberg) so queries need not reference partition columns.

Interview question: "How do you pick a partition scheme for an events table?" Model answer: partition by event date (or hour for huge volume) as most queries filter by time, keep files near target size, sort within partitions by a second common filter, and avoid high-cardinality keys.
