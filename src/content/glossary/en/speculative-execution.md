---
term: "Speculative execution"
aliases: ["speculation","backup tasks"]
category: batch
short: "Running a duplicate of a slow task elsewhere and taking whichever finishes first."
related: ["data-skew","shuffle","dag"]
---
## In plain words (Junior)
If one runner in a relay is slow, send a second runner on the same leg and use whoever arrives first.

## How it works (Mid)
The scheduler notices tasks running much slower than the median of their stage and launches copies on other executors; the first to finish wins and the other is killed. Helps against bad nodes and transient slowness. In Spark: spark.speculation.

## Senior angle & interview tips
It does not fix data skew (the copy is equally slow) and can duplicate side effects, so tasks must be idempotent and sinks must tolerate retries. Wastes resources when mis-tuned.

Interview question: "One straggler task delays the stage. Speculation or salting?" Answer: if the machine is slow use speculation; if the partition is huge, fix the skew.
