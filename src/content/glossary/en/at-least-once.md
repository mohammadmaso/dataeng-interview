---
term: "At-least-once delivery"
aliases: ["at least once","alo"]
category: streaming
short: "Every message is delivered one or more times; duplicates are possible, loss is not."
related: ["exactly-once","at-most-once","idempotency","retry"]
---
## In plain words (Junior)
You keep re-sending a letter until you get a receipt. It will surely arrive, but the receiver might get two copies if the receipt was lost.

## How it works (Mid)
The sender retries until acknowledged; the consumer commits its offset only after processing. A crash between processing and commit causes reprocessing. It is the default practical guarantee in Kafka consumers and most queues.

## Senior angle & interview tips
Turn it into effectively-once by making processing idempotent (upsert by key, dedupe table, transactional writes). Do not commit offsets before processing unless loss is acceptable (that becomes at-most-once).

Interview question: "How do you handle duplicates under at-least-once?" Answer: idempotent sinks or dedupe using a unique event id.
