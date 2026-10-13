---
term: "HyperLogLog"
aliases: ["hll", "hyperloglog++", "approximate distinct count", "approx_count_distinct"]
category: general
short: "A probabilistic sketch that estimates the number of distinct elements using a few KB of memory with about 1-2% error."
related: ["bloom-filter"]
---
## In plain words (Junior)
Imagine counting how many different people walked through a big stadium gate, without writing down names. A clever trick: watch the rarest thing, like the longest run of zeros in everyone's random ticket numbers. The rarer the extreme you saw, the more people you must have seen. HyperLogLog estimates unique counts with a tiny memory budget this way.

## How it works (Mid)
Each element is hashed; the hash is split into a bucket index and a remainder. Each bucket stores the maximum number of leading zeros seen. Rare long runs happen only after many distinct items, so a harmonic mean of the bucket values gives an estimate of the cardinality. With `m` buckets the standard error is about `1.04 / sqrt(m)`; 16 KB of buckets gives a few tenths of a percent error.

```
-- Approximate distinct users per day
SELECT dt, approx_count_distinct(user_id) FROM events GROUP BY dt;   -- Spark
SELECT uniqHLL12(user_id) FROM events;                                -- ClickHouse
SELECT approx_distinct(user_id) FROM events;                          -- Trino

-- Mergeable sketches: store per-day HLL, union for weekly uniques
```

Key property: sketches are mergeable (take bucket-wise max). You can store a sketch per day or per segment and combine them to get distinct counts over any range without reprocessing raw data. Exact distinct needs memory proportional to the number of unique values and a full shuffle, which is why dashboards and OLAP engines use sketches.

## Senior angle & interview tips
Know when approximation is acceptable: unique visitors, cardinality of keys, query planning stats. Not for billing, reconciliation or small sets where exact is cheap. Error is relative, so small counts are accurate too (sparse encoding). HLL supports union but not intersection or deletion directly (inclusion-exclusion gives poor error); theta sketches support set operations. Different engines use different precision and hash functions, so sketches are not interchangeable across systems. Related sketch families: Count-Min (frequencies), t-digest (quantiles), Bloom filter (membership).

Interview question: "Daily active users over 90 days, fast and cheap?" Model answer: store one HLL sketch per day in a rollup table and merge them at query time, accepting about 1% error, instead of counting distinct over raw events.
