---
term: "Time travel"
aliases: ["snapshot query", "as of query", "point-in-time query", "rollback", "versioned tables"]
category: lakehouse
short: "Querying a table as it existed at an earlier version or timestamp, enabled by immutable files and snapshot metadata."
related: ["iceberg", "delta-lake", "mvcc"]
---
## In plain words (Junior)
It's like version history in a document: you can open how the document looked last Tuesday, compare it with now, or restore the old version after a bad edit. Time travel gives tables this safety net.

## How it works (Mid)
Because data files are immutable and each commit creates a new snapshot listing the files that make up the table, old snapshots simply point to older file sets. Reading "as of" a version or timestamp means resolving the snapshot and reading its files. Uses: audit and reproducibility (re-run a report or model on the exact data), debugging (what changed between runs), rollback after a bad write or job, and incremental reads (diff between snapshots).

```
-- Iceberg / Spark
SELECT * FROM db.orders FOR TIMESTAMP AS OF '2024-05-01 00:00:00';
SELECT * FROM db.orders FOR VERSION AS OF 4817263548223;
CALL system.rollback_to_snapshot('db.orders', 4817263548223);

-- Delta
SELECT * FROM orders VERSION AS OF 42;
RESTORE TABLE orders TO VERSION AS OF 42;

-- Snowflake / BigQuery:  AT (TIMESTAMP => ...)  /  FOR SYSTEM_TIME AS OF ...
```

It works only while old files exist: snapshot expiry (`expire_snapshots`) and `VACUUM` physically delete files no longer referenced, bounding the time-travel window (retention). Warehouses like Snowflake and BigQuery offer similar windows with fail-safe periods.

## Senior angle & interview tips
Time travel is not a backup: it shares storage and catalog with the live table, retention is limited, and corrupt metadata or accidental table drops may defeat it. Trade-off: longer retention costs storage and slows metadata operations. It conflicts with deletion requirements (GDPR): deleted data stays readable until snapshots expire, so align retention with compliance. For reproducible ML, record snapshot ids with each training run. Branches and tags (Iceberg) extend this to write-audit-publish workflows and long-lived named versions.

Interview question: "A bad job overwrote a table an hour ago; how do you recover?" Model answer: find the last good snapshot or version from the table history, roll back or restore to it (or create a branch and verify), then fix the job; this works only if retention hasn't expired the old files, so I'd also set sensible retention and audit alerts.
