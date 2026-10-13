---
order: 66
tid: "event-modeling"
track: "6"
section: null
num: "6.11"
title: "Event & Clickstream Modeling"
type: "concept"
level: "intermediate"
priority: "P1"
est_hours: 3
summary: "Designing event schemas: a standard envelope (event_id, event_time, user_id, properties), schema versioning, and modeling sessions and funnels."
covers: ["wide events vs narrow events","semi-structured columns (JSON/Map)","tracking plans"]
interview_qs: []
extras: []
prerequisites: ["dwh-fundamentals"]
related: ["schema-evolution", "pattern-schema-evolution", "dimensional-modeling", "data-lineage", "design-clickstream-analytics"]
---

## 🎯 TL;DR

> An **event** is an immutable fact that something happened at a point in time. Event modeling is the work of defining **what** an event looks like, **how** it is named and versioned, and **how** you turn millions of raw events into sessions, funnels and metrics.

- Every event shares one **envelope**: a fixed set of fields (`event_id`, `event_name`, `event_time`, `user_id`, ...) that every consumer can rely on.
- Business-specific details go into a flexible **properties** object (JSON or a `Map` column).
- Events are append-only. You never update a past event; you add a new version or a correction event.
- Sessions, funnels and retention are **derived** from events with window functions and conditional aggregation.

## 🌱 Start simple (Junior)

> **💡 Analogy:** An event is a receipt line from a shop. It says *who* bought, *what* was bought, *when*, and *how much*. A receipt never changes after it is printed. If the shop made a mistake, it prints a new receipt that corrects the old one.

A website sends a message every time a user does something. Here is one event, written as JSON:

```json
{
  "event_id": "2f6c1e0a-6d1b-4c1e-9a55-0b7f3c2a9d10",
  "event_name": "add_to_cart",
  "event_time": "2026-01-05T10:15:02Z",
  "user_id": "u_1024",
  "properties": {"sku": "SKU-77", "qty": 2, "price": 15.5}
}
```

Four things are doing the work here:

- `event_id` is unique. It lets you deduplicate when the same event is delivered twice.
- `event_name` says what happened. Use a verb in `snake_case`: `page_view`, `add_to_cart`, `purchase`.
- `event_time` is when it **happened** on the device or server, in UTC.
- `properties` holds the details that differ by event type.

If every event had a different shape, every query would need special code. A shared envelope lets one table hold all events.

## 🔧 How it really works (Mid)

### The envelope

A common envelope has these fields:

| Field | Purpose |
|---|---|
| `event_id` | Unique id, used for deduplication and debugging |
| `event_name` | Type of event, from a controlled list |
| `schema_version` | Version of the `properties` contract |
| `event_time` | When it happened (client or server clock) |
| `ingested_at` | When your pipeline received it |
| `user_id` / `anonymous_id` | Logged-in id, and a device id before login |
| `properties` | Event-specific fields (JSON or `Map`) |

Two timestamps are important. `event_time` is the business time, and `ingested_at` is the processing time. The gap between them is your **lateness**, and it explains why yesterday's numbers can change today. The same idea appears in [[event-time]] and [[late-arriving-data]].

### Wide vs narrow events

- **Narrow (EAV-like)**: one row per property, for example `(event_id, key, value)`. Flexible, but every query needs pivots and joins, and it is slow to scan.
- **Wide**: one row per event with many typed columns (`sku`, `qty`, `price`, `page_url`, ...). Fast to query and compress well, but adding a property means an `ALTER TABLE` on a large table.
- **Semi-structured**: keep the envelope as typed columns and store `properties` as a `JSON` or `Map` column. Common columnar stores (for example ClickHouse, BigQuery, Snowflake and Spark with Parquet) can extract a JSON path at query time or materialize it as a column later.

Most teams use the third option. They promote the few properties that many queries filter on (for example `sku`) to real columns, and leave the long tail in the map.

### Tracking plans and schema versions

A **tracking plan** is the contract between product, mobile/web developers and data engineers. It lists each `event_name`, its required properties, their types and allowed values, and an owner. When a property changes, bump `schema_version`:

```text
add_to_cart v1: sku (string), qty (int)
add_to_cart v2: sku (string), qty (int), price_cents (int)   -- price became integer cents
```

Rules that prevent most breakage:

- Adding an optional field is backwards compatible.
- Renaming or changing a type is a new version, and you run both for a while.
- Consumers read `schema_version` before parsing `properties`.

For enforcement, register schemas in a [[schema-registry]] and validate events at ingestion. Invalid events go to a [[dead-letter-queue]] instead of breaking the table.

### Sessions

A **session** is a group of events from one user with no long pause between them. The usual rule is a 30-minute inactivity gap. In SQL you do it in three steps: find the gap, mark where a new session starts, then number the sessions with a running sum.

```sql
WITH ordered AS (
  SELECT user_id, event_time, event_name,
         LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time
  FROM events
),
flagged AS (
  SELECT *,
         CASE WHEN prev_time IS NULL
                OR event_time - prev_time > INTERVAL '30 minutes'
              THEN 1 ELSE 0 END AS is_new_session
  FROM ordered
),
numbered AS (
  SELECT *,
         SUM(is_new_session) OVER (
           PARTITION BY user_id ORDER BY event_time
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
  FROM flagged
)
SELECT user_id, session_no,
       MIN(event_time) AS started_at,
       MAX(event_time) AS ended_at,
       COUNT(*)        AS events
FROM numbered
GROUP BY user_id, session_no;
```

This is the same "gaps and islands" idea used in [[window-function]] problems. The `INTERVAL` syntax is PostgreSQL-style; adapt it for your engine.

### Funnels

A **funnel** counts how many users reached each step. The simplest version uses conditional aggregation:

```sql
SELECT
  COUNT(DISTINCT CASE WHEN event_name = 'page_view'   THEN user_id END) AS viewed,
  COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_id END) AS carted,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase'    THEN user_id END) AS bought
FROM events
WHERE event_time >= TIMESTAMP '2026-01-01 00:00:00';
```

This version does not enforce order: a user who bought before viewing still counts. A strict funnel needs each step to happen **after** the previous one, which usually means a per-user `MIN(event_time)` per step and a comparison between them, or a dedicated funnel function in engines that provide one.

## 🧠 Senior depth & trade-offs

- **Idempotent ingestion.** Producers retry, so duplicates are normal. Deduplicate on `event_id` within a bounded window, or use an upsert keyed by `event_id` in a table format that supports [[upsert]]. Count duplicates as a data quality metric.
- **Event time vs processing time.** Partition the raw table by `event_time` for analytics, but keep `ingested_at` to debug pipelines. Late events arrive in old partitions, so backfills must be planned. See [[watermark]] for streaming systems.
- **Cardinality of properties.** Storing free-text values or user-generated strings as a dimension key blows up column statistics and dictionaries. Use controlled vocabularies, and keep high-cardinality values as plain payload, not as grouping columns.
- **Privacy.** Events often carry personal data. Keep identifiers pseudonymous, separate the mapping table, and apply [[data-masking]] before data reaches wide analyst access. Deletion requests must reach the raw events too.
- **Storage.** Raw events are large. Partition by day, store in columnar formats, and keep a short raw retention with longer retention for aggregated tables.
- **Streaming vs batch.** Sessions over a stream need state with a timeout (session windows in Flink or Spark Structured Streaming). Batch sessions are simpler but have a delay. Choose by how fresh the metric must be.
- **Do not overfit.** Modeling every possible property up front slows product teams. A good default is a stable envelope, a flexible map, and a small set of promoted columns that the tracking plan calls out.

## 🧪 Try it

Use this small table in PostgreSQL 13 or later (or any SQL engine that supports `LAG`, `SUM OVER` and `INTERVAL`):

```sql
CREATE TABLE events (user_id text, event_time timestamp, event_name text);
INSERT INTO events VALUES
 ('u1','2026-01-05 10:00','page_view'),
 ('u1','2026-01-05 10:10','add_to_cart'),
 ('u1','2026-01-05 12:00','page_view'),
 ('u2','2026-01-05 10:05','page_view'),
 ('u2','2026-01-05 10:20','purchase'),
 ('u3','2026-01-05 09:00','add_to_cart');
```

1. Run the sessionization query above. How many sessions does `u1` have, and why?
2. Run the funnel query. What are the counts for `viewed`, `carted` and `bought`?
3. Write a query that counts users who **bought after** viewing (strict order), using `MIN(event_time)` per event name.

<details><summary>Show solutions</summary>

**1.** `u1` has **2** sessions. The events at 10:00 and 10:10 are within 30 minutes. The event at 12:00 is 110 minutes later, so it starts session 2. `u2` has 1 session and `u3` has 1 session.

**2.** `viewed = 2` (u1, u2), `carted = 2` (u1, u3), `bought = 1` (u2).

**3.**
```sql
WITH firsts AS (
  SELECT user_id,
         MIN(CASE WHEN event_name = 'page_view' THEN event_time END) AS viewed_at,
         MIN(CASE WHEN event_name = 'purchase'  THEN event_time END) AS bought_at
  FROM events
  GROUP BY user_id
)
SELECT COUNT(*) AS bought_after_view
FROM firsts
WHERE viewed_at IS NOT NULL AND bought_at > viewed_at;
```
Result: `1` (u2 viewed at 10:05 and bought at 10:20).

</details>

## 💬 Interview corner

**Question: "How would you model clickstream events for analytics?"** Mid answer: "A single events table with a standard envelope: an event id, name, timestamp, user id, and a JSON properties column. I'd partition by day." Senior answer: "The envelope is a contract, so I'd version it with a tracking plan and a schema registry, validate at ingestion, and send bad events to a dead letter queue. I'd keep both event time and ingestion time, dedupe on `event_id`, promote only the frequently filtered properties to columns, and derive sessions and funnels in a modeled layer."

**Question: "Why does yesterday's dashboard number change today?"** Late events and deduplication. Events arrive after their event time, and retries add duplicates that are removed later. Measure lateness, and state the freeze policy for each metric.

> **🎤 Say this in the interview:** "I treat events as immutable facts with a stable envelope and a versioned payload. Flexible details go in a map, the hot ones become columns, and sessions and funnels are derived views. I always separate event time from ingestion time, because that is where most correctness problems hide."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Using the ingestion time as the event time. Dashboards then shift whenever the pipeline is slow, and a backfill silently moves events to the wrong day.

> **⚠️ Watch out:** Changing the meaning of a property without bumping `schema_version`. Old and new events then mix in one column and the metric breaks without an error.

- Creating a new event name for every small UI change. This produces hundreds of near-duplicate names; prefer one event with a property.
- Using free-text values as grouping keys. The number of distinct values grows without limit.
- Deduplicating on `user_id` instead of `event_id`. Many events per user are legitimate.
- Building sessions with a fixed clock window (for example per hour) instead of an inactivity gap. Sessions then split at arbitrary times.
- Treating a non-strict funnel as a strict one in a report.
- Storing PII in `properties` without a classification, so it leaks into every downstream table.

## ✅ Check yourself (quiz)

1. Which field tells you **when the action happened**, not when the pipeline received it?
   - A) `ingested_at`
   - B) `event_id`
   - C) `event_time`
   - D) `schema_version`

<details><summary>Answer</summary>

**C** — `event_time` is the business time of the action. `ingested_at` is the processing time, and the gap between them is lateness.

</details>

2. Which change is safe **without** a new schema version?
   - A) Renaming `qty` to `quantity`
   - B) Changing `price` from a float to a string
   - C) Adding an optional `coupon_code` property
   - D) Deleting `sku` from all `add_to_cart` events

<details><summary>Answer</summary>

**C** — adding an optional field is backwards compatible. Renaming, changing types and removing fields break existing consumers, so they need a new version.

</details>

3. Your funnel shows more purchases than page views in a segment. What is the most likely cause?
   - A) The `LAG` window is too large
   - B) The funnel is non-strict and does not enforce order, or events were duplicated
   - C) `COUNT(DISTINCT)` cannot be used on strings
   - D) Purchases are always recorded twice

<details><summary>Answer</summary>

**B** — a non-strict funnel counts a user who bought without a recorded view, and duplicated events can inflate counts if they are not deduplicated by `event_id`. Check both before blaming the query.

</details>

4. **(Senior)** Ingestion retries cause about 0.5% duplicate events. A daily revenue metric is slightly too high and changes after a backfill. What do you change, and why not just `DISTINCT` on the whole row?

   - A) Remove the `properties` column so rows are unique
   - B) Deduplicate on `event_id` (the producer-generated unique key) in the modeled layer, or use an upsert keyed by `event_id`; `DISTINCT` on the whole row fails when retries change `ingested_at`
   - C) Increase the partition count
   - D) Switch all timestamps to local time

<details><summary>Answer</summary>

**B** — the retried copies have the same `event_id` but may differ in `ingested_at` or other transport fields, so a full-row `DISTINCT` keeps both. A key-based dedup is correct and cheap, and it should run before revenue aggregation.

</details>

5. **(Senior)** Product wants a new `checkout_step` property with 40,000 distinct free-text values per day. What is the risk, and what do you propose?

   - A) No risk; columnar stores handle any cardinality
   - B) High cardinality grows dictionaries and statistics and makes grouping slow; propose a controlled list of steps, store the raw text in the payload only, and promote a small enum column
   - C) Free text cannot be stored in JSON
   - D) Replace all events with a single row per user

<details><summary>Answer</summary>

**B** — unbounded text as a grouping key is expensive and hard to report on. A controlled enum column answers the analytical question, and the raw text can stay in the payload for debugging.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What is an event? | An immutable fact about something that happened at a point in time |
| Purpose of the envelope | A fixed set of fields every consumer can rely on (id, name, time, user, version) |
| `event_time` vs `ingested_at` | Business time of the action vs pipeline receive time; the gap is lateness |
| Safe change without a new version | Adding an optional property |
| Typical session rule | New session after 30 minutes of inactivity |
| Strict vs non-strict funnel | Strict enforces step order per user; non-strict only checks that each step happened |
| Why dedupe on `event_id` | Retries repeat the same event id; full-row `DISTINCT` misses them when transport fields differ |

## 🔗 Go further

- **dwh-fundamentals** covers the warehouse context where event tables live.
- **schema-evolution** and **pattern-schema-evolution** explain how to change a contract without breaking consumers.
- **dimensional-modeling** shows how events relate to facts and dimensions in a star schema.
- **design-clickstream-analytics** applies these ideas to an end-to-end system design.
- Read the docs of your engine for its JSON and map functions; the syntax differs between warehouses.
