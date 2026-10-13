---
term: "MVCC (multi-version concurrency control)"
aliases: ["mvcc", "multi-version concurrency control", "snapshot isolation"]
category: storage
short: "Keeping several versions of a row so readers see a consistent snapshot without blocking writers."
related: ["wal", "two-phase-commit", "oltp"]
---
## In plain words (Junior)
Think of a shared online document with version history. While you read yesterday's version, a colleague can keep editing the new one; neither of you waits for the other. MVCC lets a database do this: readers look at a stable older version of the data while writers create new versions.

## How it works (Mid)
Instead of overwriting a row in place, an update writes a new version tagged with transaction ids (or timestamps). Each transaction gets a snapshot, the set of versions visible at its start (or statement start), and sees only versions committed before it. Consequently readers do not take locks that block writers and vice versa; only write-write conflicts need handling (locks or first-committer-wins).

```
-- Session A                      -- Session B
BEGIN;                            BEGIN;
SELECT bal FROM acct WHERE id=1;  -- sees 100
                                  UPDATE acct SET bal=50 WHERE id=1;
                                  COMMIT;
SELECT bal FROM acct WHERE id=1;  -- still 100 under REPEATABLE READ
COMMIT;
```

Old versions must be cleaned up: Postgres stores them in the table and uses `VACUUM`; MySQL InnoDB keeps old versions in undo logs and purges them. Lakehouse table formats (Iceberg, Delta) apply the same idea at file level using snapshots.

## Senior angle & interview tips
MVCC gives snapshot isolation, which still permits write skew; serializable needs extra checks (SSI in Postgres) or locks. Costs: bloat and vacuum pressure, long-running transactions that pin old versions (preventing cleanup and growing tables or undo), and transaction-id wraparound in Postgres. Analytic engines use MVCC-like snapshots to give consistent reads during ingestion.

Interview question: "Why don't readers block writers in Postgres?" Model answer: because updates create new row versions and each transaction reads from its own snapshot; the price is dead-tuple bloat that autovacuum has to reclaim, and long transactions can delay that.
