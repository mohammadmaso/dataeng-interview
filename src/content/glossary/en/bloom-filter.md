---
term: "Bloom filter"
aliases: ["bloom filters", "bloom"]
category: storage
short: "A compact probabilistic set that answers \"definitely not present\" or \"possibly present\", with false positives but no false negatives."
related: ["lsm-tree", "parquet", "hyperloglog"]
---
## In plain words (Junior)
Picture a bouncer with a very small notebook that cannot list every guest but can say for sure "this person is definitely not invited", or "might be invited, go check the full list". Sometimes he lets in someone who should have been checked, but he never wrongly rejects a real guest. A Bloom filter is that quick, cheap pre-check.

## How it works (Mid)
A Bloom filter is a bit array of size `m` with `k` hash functions. To add an element, set `k` bits; to query, check the same bits. If any is 0 the element was never added; if all are 1 it was probably added (other elements may have set those bits). It cannot delete (counting Bloom filters or cuckoo filters can) and its false-positive rate grows as it fills; size it from expected `n` and target rate.

```
m bits, k hash functions
add("alice"):   set bits h1(x), h2(x), ..., hk(x)
query("bob"):   if any of those k bits is 0 -> definitely NOT in set
                if all are 1             -> maybe in set

false-positive rate ~ (1 - e^(-kn/m))^k
```

Typical uses: LSM stores (skip SSTables that cannot contain a key), Parquet/ORC column Bloom filters for point lookups, ClickHouse skip indexes, join runtime filters (prune probe rows early), cache admission, and deduplication pre-checks.

## Senior angle & interview tips
Value comes from avoiding expensive lookups on the negative path: most reads of absent keys never touch disk. Trade-offs: memory vs false-positive rate (about 10 bits per element gives roughly 1%), no exact membership, no enumeration or deletion, and useless for range predicates. Bloom filters are only effective for equality filters on high-cardinality columns where min/max statistics fail. For streaming dedup, a Bloom filter can drop duplicates but may wrongly drop a new event (false positive), so it is unsuitable where loss is unacceptable.

Interview question: "How does Cassandra avoid checking every SSTable on a read?" Model answer: each SSTable has a Bloom filter; if it says the key is absent, the file is skipped, so only files that might contain it are read, and false positives only cost an unneeded disk read.
