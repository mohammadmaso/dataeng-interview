---
order: 92
tid: "kafka-connect-ops"
track: "8"
section: null
num: "8.8"
title: "Kafka Connect & Operations"
type: "technology"
level: "advanced"
priority: "P1"
est_hours: 4
summary: "Kafka Connect (source/sink, distributed workers, SMTs), MirrorMaker 2 for cross-DC replication, cluster sizing, monitoring (under-replicated partitions, lag), and Strimzi on Kubernetes."
covers: ["rack awareness","partition reassignment","quotas","Cruise Control"]
interview_qs: []
extras: []
prerequisites: ["kafka-architecture", "kafka-producers", "kafka-consumers"]
related: ["kafka-retention-compaction", "schema-registry", "dlq-bad-records", "cdc", "kubernetes-for-data", "fault-tolerance"]
---

## 🎯 TL;DR

> **Kafka Connect** moves data between Kafka and other systems with configuration instead of code. **Operating** Kafka means keeping brokers healthy, replicated, balanced and observable, and knowing which metric tells you a problem is starting.

- A **source** connector reads from an external system into Kafka. A **sink** connector writes from Kafka to an external system.
- In **distributed mode**, workers share the work. Connector config, offsets and status are stored in Kafka topics, so a failed worker's tasks move to another worker.
- **MirrorMaker 2** is Connect-based replication between clusters, used for disaster recovery and multi-region setups.
- The metrics that matter first: **under-replicated partitions**, **offline partitions**, **consumer lag**, and **request latency**.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Kafka Connect is a **courier service** with pre-built routes. You do not write the driver. You fill in a form: pick up from this warehouse (source), deliver to that store (sink). The couriers (workers) handle the routes and reassign them when one courier goes sick.

A minimal sink connector config for PostgreSQL, sent to the Connect REST API:

```json
{
  "name": "orders-to-postgres",
  "config": {
    "connector.class": "io.aiven.connect.jdbc.JdbcSinkConnector",
    "topics": "orders",
    "connection.url": "jdbc:postgresql://db:5432/shop",
    "connection.user": "loader",
    "connection.password": "${file:/secrets/db.properties:password}",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "tasks.max": "3",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "orders.dlq"
  }
}
```

Three things to notice:

- `tasks.max` is the **parallelism**. It is an upper limit; the connector decides how many tasks it actually creates.
- `errors.tolerance=all` with a dead-letter queue sends bad records to `orders.dlq` instead of stopping the connector.
- The password comes from a secret file, not from plain text in the config.

> **⚠️ Watch out:** `tasks.max=3` does not mean three times the throughput. A sink connector can only parallelise across topic partitions, and a source connector depends on how the external system can be split.

## 🔧 How it really works (Mid)

### Standalone vs distributed

- **Standalone** runs one process with config in a file. Good for development, no fault tolerance.
- **Distributed** runs many workers with the same `group.id`. Connectors are created through the REST API (port 8083 by default). Workers coordinate through three internal topics: config, offsets and status.

If a worker dies, the group rebalances and the tasks run on the survivors. Modern versions use cooperative rebalancing, which moves only the affected tasks instead of stopping everything.

### Sources, offsets and sinks

| Side | Where progress is stored | What happens on restart |
|---|---|---|
| Source | Connector-defined offsets (for example, a log position) in the offsets topic | Resume from the stored source position |
| Sink | Consumer group offsets for the connector's group (`connect-<name>`) | Resume from the committed Kafka offset |

A sink that writes to a database is **at-least-once** by default: it can write a record, crash before committing the offset, and write it again. The fix is idempotent writes, such as an upsert on the primary key. This is the same reasoning as in [[kafka-exactly-once]].

### Converters and transforms

- **Converters** (`key.converter`, `value.converter`) decide how bytes become structures. With Avro you use the registry-aware converter, which stores the schema in [[schema-registry]].
- **SMTs** (single message transforms) change one record at a time: rename a field, mask a value, route to another topic. Keep them simple. Anything that needs state or joins belongs in a stream processor.

### Error handling

With `errors.tolerance=none` (the default), one bad record stops the task. With `all` plus a dead-letter topic, the bad record is written to the DLQ with error headers, and the rest flows. See [[dlq-bad-records]] for the general pattern. A DLQ is only useful if someone reads it and has a replay plan.

### MirrorMaker 2

MirrorMaker 2 (MM2) runs on Kafka Connect. It copies topics from a source cluster to a target cluster and does three extra jobs:

- **Renames topics** with a source alias. By default a topic `orders` from cluster `eu` becomes `eu.orders` in the target, so the origin is always visible and loops are avoided.
- **Syncs consumer group offsets** so consumers can fail over to the target cluster and continue near where they stopped.
- **Writes checkpoints and heartbeats** that let you measure replication lag and check that the link is alive.

Offsets in the target are not identical to offsets in the source. MM2 translates them with offset-sync data, so failover needs the translated offsets, not the raw ones.

```bash
# Start MM2 with a properties file that describes the clusters and flows
connect-mirror-maker.sh mm2.properties
```

## 🧠 Senior depth & trade-offs

### Sizing

Sizing comes from throughput, retention and replication. A simple estimate:

```text
write throughput      = 50 MB/s
replication factor    = 3          → 150 MB/s of disk writes across the cluster
retention             = 3 days = 259200 s
raw data on disk      = 50 MB/s × 259200 s ≈ 12,960,000 MB ≈ 12.96 TB
with replication      = 12.96 TB × 3 ≈ 38.9 TB
per broker (6 brokers) ≈ 6.5 TB, plus 30–40% headroom → plan ~9 TB each
```

Then check the other limits: partitions per broker (too many partitions slow leader elections and increase file handles), network bandwidth (replication and consumers multiply the traffic), and memory for page cache. Measure before you trust any estimate.

### Rack awareness

`broker.rack` tells Kafka which failure domain each broker belongs to (rack, zone, data center). With rack-aware placement, replicas of a partition go to different racks, so losing a rack does not remove every copy. In cloud setups this is usually the availability zone. Get this right before the first production deployment, because changing it later means moving replicas.

### Partition reassignment and quotas

- **Reassignment** moves partition replicas between brokers, for example after adding brokers. It copies data over the network, so set a replication throttle and run it in small batches. Otherwise it competes with live traffic and the cluster slows down.
- **Quotas** limit producers and consumers by client id or user: bytes per second (`producer_byte_rate`, `consumer_byte_rate`) and request share (`request_percentage`). A quota is the first defence against one noisy team taking down a shared cluster.

### Cruise Control

Cruise Control is an open-source tool that watches cluster load and proposes balanced replica placements. It can add or remove brokers with minimal disruption and detect anomalies such as broker failures or disk skew. Use it when manual reassignment is too slow or error-prone. It does not replace monitoring: you still need alerts.

### Strimzi on Kubernetes

Strimzi is a Kubernetes operator that manages Kafka clusters, Connect clusters, connectors and MirrorMaker 2 as custom resources. The operator handles rolling restarts, certificate rotation and scaling. Operators are a good fit when you already run Kubernetes; see [[kubernetes-for-data]] for the general trade-offs. The risk is a team that does not understand the underlying Kafka behaviour and trusts the operator blindly.

### What to monitor

| Signal | Meaning | First action |
|---|---|---|
| Under-replicated partitions > 0 | A follower is behind or a broker is down | Find the broker; check disk, network and GC |
| Offline partitions > 0 | No leader for some partitions. Producers and consumers fail for them | Page immediately; check broker health and unclean leader election |
| Consumer lag growing | Consumers are slower than producers | Check consumer throughput, rebalances, and downstream sinks |
| Connector task `FAILED` | A task crashed on a record or connection | Read the trace in the status endpoint; restart after the cause is fixed |
| Request latency (p99) | Brokers or disks are saturated | Check disk utilisation and the request queue |

An under-replicated partition is the best early warning. A rising count across many partitions usually means one broker is struggling.

### Trade-offs and failure modes

- **Connect vs custom code.** Connect gives you retries, offset tracking and scaling for free. It costs you a runtime to operate and a connector ecosystem to vet. For one unusual system, a small consumer service may be simpler.
- **Restarts and rebalances.** Frequent rebalances stop processing. Long-running tasks that exceed session timeouts get kicked out and restarted, which causes a loop. Tune timeouts and keep poll loops short.
- **Silent sink duplication** when the target does not support upserts. Check the connector's delivery guarantees before you promise exactly-once to the business.
- **Schema drift.** A converter that cannot read a new schema turns every record into an error. Pair converters with a registry and compatibility rules.

## 🧪 Try it

Use the Connect REST API against a local cluster. The paths below are the standard ones; check your version for extra fields.

```bash
CONNECT=http://localhost:8083

# List connectors and their state
curl -s $CONNECT/connectors

# Get status: connector and each task's state
curl -s $CONNECT/connectors/orders-to-postgres/status

# Pause a connector (tasks stop, offsets are kept)
curl -s -X PUT $CONNECT/connectors/orders-to-postgres/pause

# Restart a failed task (id 0)
curl -s -X POST $CONNECT/connectors/orders-to-postgres/tasks/0/restart
```

Then describe the cluster health:

```bash
# Partitions with fewer in-sync replicas than the replication factor
kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions

# Consumer lag per group
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connect-orders-to-postgres
```

Exercise: stop one broker in a three-broker local cluster and rerun the first command. What changes, and which metric would you alert on?

> **🎤 Say this in the interview:** "I run Connect in distributed mode with tasks spread across workers, and I make sinks idempotent because Connect is at-least-once. Bad records go to a DLQ with error headers. For DR I use MirrorMaker 2 and rely on its offset translation, not raw offsets. For day-to-day health I watch under-replicated partitions, offline partitions and consumer lag, in that order."

## 💬 Interview corner

**Q: A connector shows `RUNNING` but no data arrives. What do you check?**
- **Junior:** Whether the connector and its tasks are both running. A connector can be running with a failed task.
- **Mid:** Task status, the source offset (has it advanced?), the topic's end offset, and the `topics` or filter config. Check the converter errors in the logs.
- **Senior:** Confirm whether the source is actually producing. Check the offsets topic for stuck source positions, look for silent filtering in SMTs, and verify the sink's commit path. Then fix the alert: a `RUNNING` state alone is not a health signal.

**Q: How would you fail over to a second region?**
- **Mid:** MirrorMaker 2 replicates topics and consumer offsets. Switch consumers and producers to the target cluster using the translated offsets.
- **Senior:** Rehearse it. Check replication lag before the cutover, handle duplicates at the consumers, and document the failback path. A failover you have never tested is a plan, not a capability.

**Model answer (senior):** I run distributed Connect with idempotent sinks and a DLQ for bad records. For cross-region I use MirrorMaker 2 with offset translation and test failover regularly. For operations I watch under-replicated and offline partitions first, then consumer lag, and I use rack awareness, quotas and controlled reassignment to keep the cluster balanced.

## ⚠️ Common mistakes

- **Treating `RUNNING` as healthy.** Tasks can be failed, and sources can be stuck.
- **Assuming `tasks.max` scales linearly.** Parallelism depends on partitions and the connector's design.
- **Using the default `errors.tolerance=none`** and then losing a whole pipeline for one malformed record.
- **Storing secrets in the config JSON** instead of a config provider.
- **Using raw offsets for MM2 failover** instead of the translated ones.
- **Running reassignments without throttles**, which slows live traffic.
- **Skipping rack awareness** and finding out during the first zone outage.

## ✅ Check yourself (quiz)

1. What is the main job of an under-replicated partition alert?
   - A) It tells you the topic has too many partitions
   - B) It is an early warning that a follower is behind or a broker is unhealthy
   - C) It means data has been lost
   - D) It only matters for compacted topics

   <details><summary>Answer</summary>

   **B** — The partition has fewer in-sync replicas than its replication factor. It is not data loss by itself, but it reduces your safety margin and often comes before an outage.

   </details>

2. A sink connector writes to a database and crashes after writing but before committing the offset. What happens on restart?
   - A) The record is lost
   - B) The record is written again, so the sink must be idempotent
   - C) Kafka rolls back the database write
   - D) The connector stops permanently

   <details><summary>Answer</summary>

   **B** — Delivery is at-least-once. Writing with an upsert on the key makes the duplicate harmless.

   </details>

3. **Scenario:** Your MirrorMaker 2 link is healthy, but after failover consumers in the new region reprocess a large range of data. What is the likely cause?
   - A) Compaction ran on the target
   - B) Consumers used raw source offsets instead of translated offsets, or committed late
   - C) Rack awareness moved replicas
   - D) The Connect REST API restarted

   <details><summary>Answer</summary>

   **B** — MM2 translates offsets between clusters. Using untranslated offsets, or stale group offsets, moves the consumer to a position that does not match the target log, so it replays data.

   </details>

4. **Senior trade-off:** Why use cooperative rebalancing and quotas on a shared cluster?
   - A) They increase the replication factor
   - B) They limit the blast radius of one team's change or one noisy client
   - C) They remove the need for monitoring
   - D) They make MirrorMaker unnecessary

   <details><summary>Answer</summary>

   **B** — Cooperative rebalancing avoids stopping all tasks on every change, and quotas stop one client from consuming the shared capacity. Both reduce the impact of a single mistake.

   </details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Source connector | Reads external data into Kafka |
| Sink connector | Writes Kafka data to an external system |
| `tasks.max` | Upper limit on parallel tasks; bounded by partitions or the connector's design |
| Under-replicated partitions | Partitions with fewer in-sync replicas than the replication factor |
| MirrorMaker 2 alias | Topics in the target are prefixed with the source cluster alias, such as `eu.orders` |
| `broker.rack` | Failure domain label used for rack-aware replica placement |
| Cruise Control | Tool that proposes balanced placements and detects load anomalies |
| Strimzi | Kubernetes operator for Kafka, Connect and MM2 |
| Connect REST API | Default port 8083; manages connectors, tasks and their status |

## 🔗 Go further

- [[kafka-architecture]] — brokers, replicas and the controller
- [[kafka-exactly-once]] — how to make at-least-once sinks safe
- [[schema-registry]] — converters and schema compatibility
- [[cdc]] — the most common Connect source pattern
- [[fault-tolerance]] — the general reasoning behind replicas and failover
- Kafka and Connect documentation for your version's metric names and REST endpoints
