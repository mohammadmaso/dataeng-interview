---
term: "Salting"
aliases: ["key salting", "salted key", "salt"]
category: batch
short: "Adding a random suffix to a skewed key so its rows spread across several partitions, then recombining the results."
related: ["data-skew", "hot-key", "shuffle"]
---
## In plain words (Junior)
If one checkout line is far too long, you split the customers of that line into several smaller lines by handing out random numbered tickets, and then combine the totals at the end. Salting splits one overloaded key into many smaller sub-keys so several workers share the load.

## How it works (Mid)
For a skewed aggregation: append a random salt `0..N-1` to the key, aggregate by `(key, salt)` to produce partial results spread over N partitions, then aggregate again by the original key to merge. This is a two-stage aggregation and works for decomposable functions (sum, count, min, max); averages need sum and count; exact distinct needs care.

```
from pyspark.sql import functions as F
N = 16
# 1) skewed side: add random salt
big  = big.withColumn("salt", (F.rand() * N).cast("int"))
# 2) other side: replicate each row N times with every salt value
small = small.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)])))
# 3) join on (key, salt)
joined = big.join(small, ["key", "salt"])

# Aggregation: two-stage
partial = df.withColumn("salt", (F.rand()*N).cast("int")).groupBy("key","salt").sum("v")
final   = partial.groupBy("key").sum("sum(v)")
```

For a skewed join: salt the large skewed side randomly and replicate the other side across all salt values so every salted key still finds its match. The cost is N-fold replication of the smaller side, so salt only the heavy keys (filter hot keys, salt those, and join the rest normally), or let Spark AQE do skew-join splitting automatically.

## Senior angle & interview tips
Choose N from the key's skew factor, not blindly; too large wastes shuffle and memory, too small leaves stragglers. Salting adds code complexity and breaks any assumption of same-key co-location, so ordering-dependent logic (window functions, dedup by latest) cannot simply be salted. Prefer, in order: remove the skew at the source (filter nulls, fix default keys), broadcast the small side, enable AQE, then salt. The concept also applies to databases and Kafka (key suffixes to spread hot partitions).

Interview question: "Explain salting with an example." Model answer: if 60% of events have user_id = NULL, I add a random suffix to the key, pre-aggregate per (key, salt) across many tasks, and then aggregate per original key; for joins I replicate the other side across salts, ideally only for the known hot keys.
