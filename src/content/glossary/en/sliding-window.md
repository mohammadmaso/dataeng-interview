---
term: "Sliding window"
aliases: ["hopping window","sliding windows"]
category: streaming
short: "Overlapping fixed-size time windows that advance by a smaller step."
related: ["tumbling-window","session-window","watermark"]
---
## In plain words (Junior)
A "last 10 minutes" view that refreshes every minute: each new view overlaps the previous one, like a moving magnifying glass over a timeline.

## How it works (Mid)
Defined by size and slide (for example size 10 min, slide 1 min). Each event belongs to size/slide windows. Compared with tumbling windows (slide = size, no overlap), sliding windows give smoother metrics but multiply state and output volume.

## Senior angle & interview tips
Cost grows with size/slide ratio; pick the coarsest slide the product accepts. Alternatives: maintain incremental aggregates with panes (sub-windows) and combine them. Flink calls these "sliding", Spark "sliding" with windowDuration and slideDuration, others say "hopping".

Interview question: "Rolling 5-minute error rate, updated every 30s?" Answer: sliding window size 5m slide 30s; each event lands in 10 windows.
