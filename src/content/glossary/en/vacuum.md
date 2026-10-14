---
term: "Vacuum"
aliases: ["vacuum command","file cleanup","expire snapshots"]
category: lakehouse
short: "Deleting data files no longer referenced by a table and past the retention window."
related: ["time-travel","compaction","delta-lake","iceberg"]
---
## In plain words (Junior)
Taking out the trash: old versions of the table left lying around are removed to free space, but only after you are sure nobody needs them.

## How it works (Mid)
Table formats keep old files for time travel and concurrent readers. VACUUM (Delta) or expire_snapshots + remove_orphan_files (Iceberg) delete files not referenced by any retained snapshot. Postgres VACUUM reclaims dead tuples from MVCC.

## Senior angle & interview tips
Retention must exceed the longest-running query/stream and your time-travel promise; vacuuming too aggressively breaks readers and rollback. Orphan removal must avoid files from in-flight writes. Schedule it with compaction.

Interview question: "Storage keeps growing after deletes. Why?" Answer: deletes only add new snapshots; old files remain until snapshots expire and vacuum removes them.
