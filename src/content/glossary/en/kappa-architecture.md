---
term: "Kappa architecture"
aliases: ["kappa"]
category: streaming
short: "One streaming pipeline over a replayable log; reprocess by replaying."
related: ["lambda-architecture","stream-processing","log-compaction"]
---
## In plain words (Junior)
Instead of two kitchens, use one that can also re-cook everything from the saved recipe book (the log) whenever the recipe changes.

## How it works (Mid)
All data flows through a durable, replayable log (Kafka). A single stream job computes the views. To change logic or fix a bug, deploy the new job version reading from the start (or a chosen offset), build a new output, then switch consumers over and retire the old one.

## Senior angle & interview tips
Requires long log retention or tiered storage and enough throughput to replay quickly. Hard when computations need data not in the log or very large historical state. Pair with a lakehouse table as the long-term log for cheap replay.

Interview question: "How do you fix a bug in a Kappa pipeline?" Answer: run the corrected job in parallel from an earlier offset into a new output, validate, cut over, delete the old.
