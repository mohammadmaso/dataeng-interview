---
term: "Optimistic concurrency control"
aliases: ["optimistic locking","occ","version check"]
category: distributed
short: "Assume no conflict, work freely, and verify at commit that nobody else changed the data."
related: ["mvcc","deadlock","isolation-level","delta-lake"]
---
## In plain words (Junior)
Two people edit a shared document copy; when saving, the system checks if the original changed and asks the second person to redo if so.

## How it works (Mid)
Each row (or table snapshot) carries a version. A writer reads the version, does work without locks, and at commit applies the update only if the version is unchanged (UPDATE ... WHERE version = ?). On conflict it retries. Lakehouse table formats commit new snapshots optimistically against the table’s latest version.

## Senior angle & interview tips
Great when conflicts are rare; with hot rows retries pile up and pessimistic locking performs better. Define retry limits and make the work idempotent. In Delta/Iceberg, concurrent writers touching the same files conflict, so partition writers by disjoint data.

Interview question: "Two Spark jobs update the same Delta table; one fails. Why?" Answer: optimistic commit detected conflicting file changes; retry or separate their partitions.
