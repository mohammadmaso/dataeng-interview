---
term: "Dictionary encoding"
aliases: ["dictionary compression","rle","run-length encoding"]
category: storage
short: "Replacing repeated values with small integer codes plus a lookup table."
related: ["data-compression","columnar-storage","parquet","cardinality"]
---
## In plain words (Junior)
Instead of writing "United Kingdom" a million times, write 7 and keep a legend saying 7 means United Kingdom.

## How it works (Mid)
A column chunk stores a dictionary of distinct values and then integer ids for each row, which are further packed (bit-packing, run-length encoding). Works best on low-cardinality columns. Parquet falls back to plain encoding when the dictionary grows too large. Filters can even run on the dictionary first.

## Senior angle & interview tips
High-cardinality columns (ids, timestamps) gain little and may waste the dictionary page. Sorting data before writing creates long runs that RLE compresses dramatically, a cheap optimization.

Interview question: "How do you shrink a Parquet table by 40% without changing data?" Answer: sort by low-cardinality columns before writing and use Zstd.
