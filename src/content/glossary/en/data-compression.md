---
term: "Data compression"
aliases: ["compression","snappy","zstd","gzip"]
category: storage
short: "Shrinking data on disk and over the network, trading CPU for space and I/O."
related: ["parquet","columnar-storage","dictionary-encoding"]
---
## In plain words (Junior)
Vacuum-packing clothes: they take less room in the suitcase, but you spend a bit of effort packing and unpacking.

## How it works (Mid)
Codecs differ: Snappy and LZ4 are fast with modest ratios, Zstd offers a tunable balance, Gzip compresses well but is slower. Columnar files compress per column chunk, so similar values give high ratios. Splittability matters: plain gzip text files cannot be split across tasks, while Parquet with Snappy remains splittable at row-group level.

## Senior angle & interview tips
Default to Zstd or Snappy for hot data, stronger codecs for cold archives. Compression often speeds queries because I/O dominates. Combine with encoding (dictionary, RLE, delta).

Interview question: "Why can a gzip CSV hurt Spark performance?" Answer: it is not splittable, so one task reads the whole file.
