---
term: "Columnar storage"
aliases: ["column-oriented", "column store", "columnar", "columnar database", "column-oriented storage"]
category: storage
short: "Storing each column's values contiguously rather than row by row, which suits analytics: read fewer bytes and compress better."
related: ["parquet", "olap", "predicate-pushdown"]
---
## In plain words (Junior)
A phone book lists name, address and number together for each person (row-oriented). A columnar store would put all numbers in one list, all names in another. If you only want to add up all the numbers, you read just that one list and ignore the rest, so questions about a few columns are much faster.

## How it works (Mid)
Analytic queries typically touch few columns of very wide tables but many rows. In a column store, each column is stored in its own contiguous block, so a query reads only the needed columns. Same-type neighbors compress extremely well (dictionary, run-length, delta encoding), and engines process data in vectorized batches that use CPU caches and SIMD efficiently.

```
Row store:     [id1,name1,amt1] [id2,name2,amt2] [id3,name3,amt3]
Column store:  ids:[id1,id2,id3]  names:[name1,name2,name3]  amts:[amt1,amt2,amt3]

SELECT SUM(amt) FROM sales;   -- column store reads only the amts block
SELECT * FROM sales WHERE id=2; -- row store reads one contiguous record
```

Examples: Parquet/ORC files, ClickHouse, StarRocks, Redshift, BigQuery, DuckDB, Snowflake. Row stores (PostgreSQL, MySQL) win for OLTP: fetching or updating one whole record touches one place.

## Senior angle & interview tips
The costs: reconstructing full rows requires reading many column files, single-row inserts/updates are expensive (hence buffering, LSM-style parts, delta stores, immutable files plus merge), and point lookups need indexes or sparse primary indexes. Sorting order is the big lever in ClickHouse and in clustered Parquet: data sorted by common filter columns compresses better and enables data skipping. Late materialization (filter on a few columns first, fetch other columns for matching rows only) is a key optimization.

Interview question: "Why is `SELECT *` bad on a column store?" Model answer: it forces reading and decoding every column, eliminating the main benefit; select only needed columns so the engine can skip the rest and read far fewer bytes.
