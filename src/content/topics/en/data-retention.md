---
order: 135
tid: "data-retention"
track: "12"
section: null
num: "12.8"
title: "Data Lifecycle & Retention"
type: "concept"
level: "intermediate"
priority: "P2"
est_hours: 1
summary: "Retention policies, hot/warm/cold tiers, archiving, TTLs, and regulatory retention requirements."
covers: []
interview_qs: []
extras: []
prerequisites: ["lakehouse-table-formats"]
related: ["data-security", "pattern-data-deletion", "design-user-deletion", "clickhouse", "kafka-retention-compaction"]
---
## 🎯 TL;DR

> **Data retention** decides **how long** each dataset is kept, **where** it lives at each age, and **when** it is deleted. The rule is set by the business and by law; the engineering is making it automatic and provable.

- Keep data in **tiers** by age and access frequency: hot (fast, expensive), warm, cold (cheap, slow).
- Use **TTLs** and lifecycle rules to delete or move data automatically, not with a manual cleanup job.
- Retention applies to **every copy**: raw files, tables, snapshots, backups, logs, caches and Kafka topics.
- A **legal hold** overrides normal deletion. Deletion must be provable, with a record of what was removed and when.
- Plan retention before data arrives. Deleting from a huge immutable lake later is far harder.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A library with limited shelves. Books people read every week stay at the front desk (hot). Older books go to the back room (warm). Very old books go to the basement archive (cold). A few are destroyed when the law says the library no longer must keep them.

Three questions for each dataset:

1. **How long must we keep it?** (business need, legal requirement)
2. **How often is it read, and how fast must it be?** (decides the storage tier)
3. **What happens at the end?** (delete, archive, or anonymize)

Example policy written in plain words:

| Dataset | Keep in hot | Keep in cold archive | Delete after |
|---|---|---|---|
| Raw clickstream events | 7 days | 90 days | 1 year |
| Orders (finance) | 13 months | 7 years | as required by finance rules |
| Application debug logs | 3 days | none | 30 days |

Notice that the rules differ per dataset. "Keep everything forever" is not a policy; it is a cost that grows every month.

## 🔧 How it really works (Mid)

### Storage tiers

| Tier | Typical storage | Access | Cost per TB |
|---|---|---|---|
| Hot | SSD or local disks in the cluster, warehouse compute | Interactive queries, dashboards | Highest |
| Warm | Object storage with a columnar table format | Batch jobs, weekly reports | Medium |
| Cold | Object storage in a cheaper class or tape/archive media | Rare audits and reprocessing | Lowest |

Moving data between tiers is a **copy plus a metadata change**. In a lakehouse, a table format such as Iceberg or Delta tracks which files belong to a table, so you can move or expire files without breaking readers, provided the metadata is updated correctly. Tier moves in an OLAP database such as ClickHouse are declared in the table definition with TTL rules that move parts to another volume.

### TTL and lifecycle rules

A **TTL** (time to live) is a rule that deletes or moves data after a certain age. Examples of where it lives:

- **Object storage:** lifecycle rules on a prefix, for example "transition to the cold class after 90 days, delete after 365 days". The provider runs them; you do not write a cron job.
- **Kafka:** `retention.ms` on a topic deletes old log segments. Compacted topics keep the latest value per key instead; see [[kafka-retention-compaction]].
- **ClickHouse:** a `TTL` clause on a table or column deletes rows or moves parts to another disk or volume when they expire.
- **Tables in a lakehouse:** you must **expire snapshots** and **remove orphan files** too. A table that only deletes rows still keeps the old files until the snapshots that reference them are gone.

```sql
-- ClickHouse: delete raw events after 90 days, move them to cold storage after 14 days.
CREATE TABLE events_raw
(
    event_time DateTime,
    user_id    UInt64,
    payload    String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, event_time)
TTL event_time + INTERVAL 14 DAY TO VOLUME 'cold',
    event_time + INTERVAL 90 DAY DELETE;
```

The `TO VOLUME` target must exist in the storage policy; check your version's documentation for the exact configuration.

### Deleting in a lakehouse

Iceberg and Delta tables are built from immutable data files plus metadata. A "delete" is a sequence:

```sql
-- Iceberg (Spark SQL): remove rows, then clean up old versions.
DELETE FROM sales.orders WHERE order_date < DATE '2019-01-01';

-- Expire snapshots so the old files are no longer referenced by time travel.
CALL system.expire_snapshots(
  table => 'sales.orders',
  older_than => TIMESTAMP '2025-01-01 00:00:00'
);

-- Remove files that no snapshot references any more.
CALL system.remove_orphan_files(table => 'sales.orders');
```

Procedure names and arguments differ between Spark, Trino and engine versions. The principle is the same: deleting rows does not free disk space until old snapshots are expired and unreferenced files are removed.

### Logs and backups

- Logs and backups are copies. A retention rule for the warehouse that ignores the backup set does not satisfy a deletion request.
- Backups usually have their own retention (for example, daily for 30 days, monthly for a year). Document both, and test restores.

## 🧠 Senior depth & trade-offs

- **Retention is a data contract.** The owner of a dataset declares its retention in the catalog and in the data contract. Consumers then know the window for backfills and re-processing. A pipeline that needs three years of history cannot depend on a dataset with a 90-day TTL.
- **Compute for deletion.** Row deletes in a columnar table rewrite or mark files. At scale, deletion jobs compete with ingestion for compute. Schedule compaction and snapshot expiry, and measure how many files and bytes each run removes.
- **Snapshots vs retention.** Time travel is useful, but every retained snapshot keeps its data alive. A common production bug is "we deleted the rows, but storage did not shrink" because snapshots still reference them.
- **Derived data.** A deleted row can survive in aggregates, feature stores, caches, search indexes and ML training sets. Retention must cover derived data or a fine-grained deletion can be incomplete. Lineage ([[data-lineage]]) tells you where the copies are.
- **Legal hold.** A hold is a flag that blocks deletion for a set of records, for example during an investigation or litigation. Holds must override TTL jobs, and the system must show the hold was honoured.
- **Cost model.** Estimate retention cost before you ship a pipeline: daily bytes × replication factor × days kept × price per tier. For example, 200 GB/day × 3 replicas × 365 days = 219 TB stored on hot disks; moving the 355 days older than 30 to cold storage changes the bill far more than any compression trick.
- **Deleting immutable history.** Event logs that are the source of truth should have a retention that matches the reprocessing window. If you keep only 7 days in Kafka, a lakehouse copy is what makes a 2-year backfill possible.
- **Monitoring.** Track oldest-record age per table, bytes per tier, snapshot count, orphan file count, and the last successful expiry job. A retention job that silently fails is a compliance incident.

> **⚠️ Watch out:** A TTL that deletes data in the warehouse does nothing to the raw landing bucket, the Kafka topic or the backup set. Retention is a property of the whole chain, not one system.

## 🧪 Try it

1. Write a retention table for four datasets in your own project: raw events, cleaned facts, aggregated daily metrics and audit logs. For each, set hot, warm, cold and delete durations and justify them in one sentence.
2. Compute the stored size for `200 GB/day` of raw events, replication factor `3`, kept `365` days, with `30` days hot and the rest cold. Show the arithmetic for each tier.
3. Explain why running `DELETE` on an Iceberg table did not reduce the bucket size, and list the three steps that do reduce it.

<details><summary>Show solutions</summary>

**1.** A typical answer: raw events hot 7 days, warm 30 days, cold 365 days, delete after 2 years (reprocessing and audit need); cleaned facts hot 13 months, delete after 7 years (finance rule); daily aggregates kept longer because they are small; audit logs immutable for the legal period. The key is that each duration has a reason.

**2.** Raw per year: 200 GB × 365 = 73,000 GB ≈ 73 TB per copy. With RF 3: 219 TB total. Hot share: 200 GB × 30 days × 3 = 18 TB. Cold share: 219 − 18 = 201 TB. Hot tier is about 8% of the bytes but the most expensive part of the bill.

**3.** `DELETE` writes new metadata and marks rows as removed, but old snapshots still point to the old files. Expire snapshots past the retention window, then remove orphan files. The bucket shrinks only after both steps, and only if no other snapshot references the files.

</details>

## 💬 Interview corner

**Question: "How would you design retention for a clickstream lake that must support two-year backfills?"**

- **Mid answer:** "Raw events go to object storage in a lifecycle policy: cold after 90 days, delete after two years. The warehouse keeps aggregated tables longer."
- **Senior answer:** "I'd separate the retention of the source of truth from the serving copies. Kafka keeps a few days for real-time consumers, the lake keeps raw files for two years in tiered storage, and the aggregates keep longer because they are small. I'd check that the backfill window matches the lake retention, expire Iceberg snapshots on a schedule, and monitor the oldest record age and the orphan file count. Deletion requests must also reach the derived tables."

> **🎤 Say this in the interview:** "Retention is a per-dataset decision with a reason, enforced by automation and monitored. I make sure the retention of the source of truth covers the longest backfill, and I count every copy, not just the main table."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Deleting rows and assuming storage drops. Snapshots and retained files keep the bytes until they are expired and cleaned up.

- Setting retention by feeling instead of by requirement, so no one can explain why data is kept.
- Keeping everything "just in case" with no tiering, which makes every query and every backup slower.
- Ignoring backups, logs, caches and Kafka topics when applying a delete.
- Using a TTL that deletes data that a downstream pipeline still needs for a backfill.
- Running deletion jobs without a record of what was removed.
- Forgetting legal holds, so an active investigation loses data to an automatic job.

## ✅ Check yourself (quiz)

1. Which property most directly decides the storage tier for a dataset?
   - A) Its column count
   - B) How often and how fast it must be read
   - C) Its primary key type
   - D) The programming language of the producer

<details><summary>Answer</summary>

**B** — tiers trade speed for cost. Frequently and interactively read data belongs in hot storage; rarely read data belongs in cheap, slow storage.

</details>

2. After `DELETE FROM` an Iceberg table, the object storage bucket is the same size. What is the most likely reason?
   - A) Iceberg does not support deletes
   - B) Old snapshots still reference the deleted rows' files, so the files are retained
   - C) The bucket cannot shrink
   - D) `DELETE` only changes the schema

<details><summary>Answer</summary>

**B** — the table moved to a new snapshot, but time travel still reads the old files. Expire old snapshots and remove orphan files to free the space.

</details>

3. A legal investigation requires keeping certain customer records. A TTL job would delete them. What should happen?
   - A) The TTL job runs anyway; investigators get a backup later
   - B) A legal hold blocks deletion for those records, the hold is recorded, and the TTL job respects it
   - C) Retention is disabled for the whole table
   - D) The records are copied to Git

<details><summary>Answer</summary>

**B** — holds are targeted and override automatic deletion. Disabling retention for a whole table (C) keeps data that should be deleted and is a compliance problem of its own.

</details>

4. **(Senior)** Your backfill job needs 2 years of history, but the Kafka topic retains 7 days and the lake keeps raw files for 90 days. What is the design problem and fix?

   - A) Kafka is too slow; switch to a database
   - B) The retention of the source of truth is shorter than the reprocessing window; extend the raw lake retention (tiered to cold storage) to cover the longest backfill, and document it as part of the contract
   - C) Backfills should not be allowed at all
   - D) Increase the Kafka partition count

<details><summary>Answer</summary>

**B** — the retention must cover the longest realistic reprocessing need. Kafka is not the long-term archive; the lake is. Cold tiers make a two-year window affordable.

</details>

5. **(Senior)** A deletion request for one customer must be honoured everywhere. Which copies must you check, and why is that harder than deleting from the main table?

   - A) Only the main table, because derived data is always regenerated
   - B) Raw landing files, Kafka topics within retention, backups, aggregates, feature stores and caches; they are copies, and immutable storage and snapshots keep them alive until they are rewritten or expired
   - C) None; deletion requests only apply to the primary key
   - D) Only the BI dashboards

<details><summary>Answer</summary>

**B** — deletion is a property of every copy. Lineage tells you where copies are, and immutable storage means deleting from one place does not remove bytes elsewhere.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Hot, warm, cold tiers | Hot: fast and expensive for frequent reads; warm: middle ground; cold: cheap and slow for rare access |
| TTL | A rule that deletes or moves data after an age |
| Why deleting rows does not free space in a lakehouse | Old snapshots and their files stay referenced until expired and cleaned up |
| What `expire_snapshots` does | Removes old snapshot metadata so time travel no longer keeps its files alive |
| Legal hold | A flag that blocks deletion for specific records and must override automatic TTL jobs |
| Retention for a source of truth | Must cover the longest backfill or reprocessing window |
| Where retention must apply | Every copy: raw files, tables, snapshots, backups, logs, caches, derived data, Kafka topics |
| Why measure oldest record age | It proves the retention job actually runs and deletes what it should |

## 🔗 Go further

- **[[data-security]]** explains how deletion fits with access control, masking and crypto-shredding.
- **[[pattern-data-deletion]]** and **[[design-user-deletion]]** work through full deletion designs.
- **[[kafka-retention-compaction]]** covers topic retention and compaction in detail.
- **[[lakehouse-table-formats]]** explains snapshots, time travel and file layout, which drive lakehouse retention.
- **[[clickhouse]]** shows how TTL and tiered storage are used in an OLAP engine.
- Read your table format's maintenance documentation (expire, vacuum, orphan cleanup) for the exact procedures and defaults of your version.

