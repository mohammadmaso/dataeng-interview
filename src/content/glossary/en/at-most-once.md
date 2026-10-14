---
term: "At-most-once delivery"
aliases: ["at most once"]
category: streaming
short: "Each message is delivered zero or one times; no duplicates but loss is possible."
related: ["at-least-once","exactly-once","offset"]
---
## In plain words (Junior)
You shout a message once and never repeat it. If the other person did not hear, it is gone, but nobody hears it twice.

## How it works (Mid)
The consumer commits its offset before processing, or the producer does not retry. A crash after commit but before processing drops the message. Lowest latency and overhead.

## Senior angle & interview tips
Acceptable for metrics, logs sampling, or telemetry where an occasional drop is fine. Wrong for money or state changes. Know it as the third point of the delivery triangle.

Interview question: "When is at-most-once a reasonable choice?" Answer: high-volume, low-value, idempotent-by-nature signals where freshness matters more than completeness.
