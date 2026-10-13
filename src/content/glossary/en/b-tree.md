---
term: "B-tree"
aliases: ["b-trees", "b+tree", "b+ tree", "btree index"]
category: storage
short: "A balanced, wide, sorted tree on disk pages that gives O(log n) point lookups, range scans and in-place updates; the default database index."
related: ["database-index", "lsm-tree", "oltp"]
---
## In plain words (Junior)
Think of the index in a thick book: it tells you the right chapter, then the right page, in a few steps, rather than reading every page. A B-tree is such an index kept sorted on disk, making searches fast and letting you also read "everything between 40 and 60".

## How it works (Mid)
Data is organized in fixed-size pages (e.g. 8 KB). Each internal page holds sorted keys and child pointers; leaf pages hold keys and values (or row pointers). Lookups traverse root to leaf, one page read per level; with hundreds of keys per page, depth is 3-4 even for billions of rows. B+trees keep values only in leaves and link leaves, making range scans sequential. Inserts split full pages; updates modify pages in place, protected by a WAL.

```
                 [ 30 | 60 ]                    <- root page
               /      |      \
      [10|20]     [40|50]     [70|80|90]        <- internal pages (keys + child pointers)
       / | \       / | \        / | | \
     leaf pages: sorted keys + row pointers (B+tree: leaves linked for range scans)

Fanout ~ hundreds => 3-4 levels address billions of keys; each level = one page read.
```

Properties: predictable read latency, efficient range queries and ordered iteration, in-place updates, and good performance for mixed read/write OLTP. It's the index in PostgreSQL, MySQL InnoDB, SQL Server, Oracle and many file systems.

## Senior angle & interview tips
Weaknesses: write amplification (modify whole page for small change), random I/O on inserts with random keys (UUIDv4 primary keys cause page splits and cache misses; time-ordered IDs help), fragmentation requiring vacuum/reindex, and poor suitability for very high write throughput compared with LSM-trees. Composite index column order matters (leftmost prefix rule), and covering indexes avoid table lookups. Columnar OLAP engines mostly don't use B-trees; they use sparse indexes, zone maps and sort order.

Interview question: "B-tree vs LSM-tree?" Model answer: B-trees update in place with predictable reads and good range scans but incur random writes and write amplification; LSM-trees buffer writes and merge sorted files, giving higher write throughput and better compression at the cost of read amplification and compaction overhead.
