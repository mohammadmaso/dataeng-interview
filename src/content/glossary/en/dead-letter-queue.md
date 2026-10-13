---
term: "Dead letter queue (DLQ)"
aliases: ["dlq", "dead-letter queue", "dead letter topic", "poison message", "quarantine"]
category: ingestion
short: "A separate queue or table where messages that repeatedly fail processing are parked so the main pipeline keeps flowing."
related: ["schema-evolution", "idempotency", "exactly-once"]
---
## In plain words (Junior)
In a mail sorting office, a letter with an unreadable address does not stop the whole belt. It goes into a special bin for a human to check later. A dead letter queue is that bin: messages that cannot be processed are set aside so the other messages keep moving.

## How it works (Mid)
A single bad message (corrupt payload, schema violation, bug on an edge case) can block an ordered partition forever if you retry endlessly. The pattern: retry transient errors a few times with backoff, and send permanent failures to a DLQ along with context (original payload, error, source topic/partition/offset, timestamp, attempt count), then commit the offset and continue.

```
try:
    record = parse(msg.value)
    process(record)
except (ValidationError, ParseError) as e:
    producer.send("orders.dlq", key=msg.key, value=msg.value,
                  headers={"error": str(e), "src_topic": msg.topic,
                           "partition": msg.partition, "offset": msg.offset})
consumer.commit()   # move on; do not block the partition
```

Good practice: separate transient from permanent errors; keep the original bytes untouched so they can be replayed; alert on DLQ size and rate; and build a tool to inspect, fix and re-drive messages back to the main topic. Batch pipelines do the same with a quarantine table for rejected rows.

## Senior angle & interview tips
Trade-offs: skipping a message breaks strict ordering for that key and may leave downstream state inconsistent; a DLQ without monitoring is just silent data loss. A spike of DLQ messages usually signals a schema change or upstream bug, not random bad data, so pair it with data contracts and alerts. Retry topics with increasing delays (retry-5m, retry-1h) avoid blocking. Idempotent processing is needed since re-driven messages may duplicate. Kafka has no built-in DLQ for plain consumers; Kafka Connect and frameworks (Spring Kafka) provide one.

Interview question: "A malformed event is blocking a Kafka partition; what do you do?" Model answer: catch the permanent error, publish the message with error metadata to a DLQ, commit the offset, alert on DLQ volume, and later fix and replay; transient errors get bounded retries with backoff instead.
