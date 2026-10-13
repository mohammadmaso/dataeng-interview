---
term: "Parquet"
aliases: ["apache parquet", "parquet file", "parquet files"]
category: storage
short: "An open columnar file format with compression, encodings and min/max statistics, the default for analytic data lakes."
related: ["columnar-storage", "predicate-pushdown", "compaction", "schema-evolution"]
---
## In plain words (Junior)
Think of a table printed on paper. Instead of storing it row by row, Parquet stores each column separately, like keeping all prices on one sheet and all names on another. If a question only needs prices, you read only that sheet. Same-type values packed together also shrink nicely when compressed.

## How it works (Mid)
A Parquet file is split into row groups; inside each, every column is stored as a column chunk divided into pages. Values are encoded (dictionary, run-length, delta) and then compressed (Snappy, Zstd). The footer holds the schema and per-column-chunk statistics.

```
File
 ├─ Row group 1 (~128 MB)
 │   ├─ Column chunk: user_id  -> pages (dictionary/RLE, compressed)
 │   ├─ Column chunk: amount   -> pages
 │   └─ stats per chunk/page: min, max, null_count
 ├─ Row group 2 ...
 └─ Footer: schema, row-group offsets, statistics
```

Readers use projection pushdown (read only needed columns) and predicate pushdown (skip row groups or pages whose min/max cannot match the filter). It supports nested data via the Dremel repetition/definition level encoding. Parquet files are immutable; updates mean rewriting files, which table formats (Iceberg, Delta, Hudi) manage with metadata and delete files.

## Senior angle & interview tips
Practical tuning: aim for files of roughly 128 MB to 1 GB and row groups that match your reader parallelism; sort or cluster by common filter columns so min/max stats are selective; avoid very high-cardinality dictionary bloat; avoid tiny files. Caveats: poor for point lookups and frequent updates, small appends cause file proliferation, and schema evolution depends on column resolution (by name vs position) which differs between engines. ORC is a close alternative; Arrow is the in-memory counterpart.

Interview question: "Why Parquet over CSV for analytics?" Model answer: columnar layout lets you read only needed columns, encoding and compression cut size and I/O, statistics enable data skipping, and the embedded schema avoids parsing ambiguity.
