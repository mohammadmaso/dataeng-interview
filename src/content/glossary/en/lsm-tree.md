---
term: "LSM-tree (log-structured merge-tree)"
aliases: ["lsm", "lsm tree", "lsm-trees", "log-structured merge tree", "memtable", "sstable"]
category: storage
short: "A write-optimized structure that buffers writes in memory, flushes sorted immutable files, and merges them in the background."
related: ["wal", "compaction", "bloom-filter"]
---
## In plain words (Junior)
Imagine scribbling new notes on a sticky pad on your desk (fast), and every so often sorting the full pad into a folder and merging folders so they stay tidy. Writes are quick because you never search the archive to insert. An LSM-tree does this: it absorbs writes in memory and reorganizes them later.

## How it works (Mid)
Writes go to a WAL and an in-memory sorted structure (memtable). When full, the memtable is flushed as an immutable sorted file (SSTable). Deletes are tombstones, updates are new versions. A background compaction merges files, removes overwritten values and tombstones, and keeps the number of files bounded.

```
write -> WAL (durability)
      -> memtable (sorted, in RAM)
memtable full -> flush -> SSTable L0 (immutable, sorted)
background compaction merges L0 -> L1 -> L2 ...
read -> memtable -> bloom filter -> SSTables newest to oldest
```

Reads must check the memtable and possibly many SSTables, so they use Bloom filters, sparse indexes and block caches to skip files. Compaction styles: size-tiered (lower write amplification, more space and read amplification) vs leveled (better reads and space, higher write amplification). Used by Cassandra, RocksDB, HBase, LevelDB, and ClickHouse MergeTree (similar idea with parts).

## Senior angle & interview tips
LSM vs B-tree: LSM turns random writes into sequential ones and wins on write-heavy workloads; B-trees give more predictable reads and in-place updates. The RUM trade-off applies: you cannot minimize read, update and memory overhead simultaneously. Operational issues: compaction stalls and write throttling, space amplification during compaction, tombstone buildup hurting reads (Cassandra tombstone warnings), and tuning compaction to workload.

Interview question: "Why is Cassandra fast at writes?" Model answer: writes are an append to the commit log plus an in-memory insert, with no read-before-write; sorted immutable files are produced by flush and merged by compaction, at the cost of read and write amplification handled by Bloom filters and compaction strategy.
