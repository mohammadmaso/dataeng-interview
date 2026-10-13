---
term: "Isolation level"
aliases: ["isolation levels", "read committed", "repeatable read", "serializable", "snapshot isolation", "write skew"]
category: storage
short: "A setting that defines which concurrency anomalies (dirty reads, non-repeatable reads, phantoms, write skew) a transaction may observe."
related: ["acid", "mvcc", "oltp"]
---
## In plain words (Junior)
Two people edit the same spreadsheet at once. Isolation levels decide how much each can see of the other's unfinished changes. Strict rules keep everything neat but make people wait for each other; loose rules are faster but can produce strange results, like both approving the last vacation slot.

## How it works (Mid)
Anomalies: dirty read (seeing uncommitted data), non-repeatable read (same row read twice gives different values), phantom (a repeated query returns new rows), lost update, and write skew (two transactions read overlapping data and write disjoint rows, violating an invariant). The SQL standard defines four levels by which anomalies they forbid; real implementations differ.

```
Level               Dirty  NonRepeatable  Phantom  WriteSkew
READ UNCOMMITTED     yes       yes          yes      yes
READ COMMITTED       no        yes          yes      yes     (Postgres default)
REPEATABLE READ      no        no           yes*     yes     (*Postgres RR = snapshot isolation: no phantoms, but write skew)
SERIALIZABLE         no        no           no       no

-- Write skew: two on-call doctors both check "someone else is on call", both go off.
```

Implementation: lock-based (two-phase locking) or MVCC snapshots. Snapshot isolation prevents most read anomalies but still allows write skew; true serializability needs SSI (Postgres `SERIALIZABLE`), predicate locking, or actual serial execution. Higher levels mean more aborts/retries or blocking; applications must retry serialization failures.

## Senior angle & interview tips
Know your database's real default; many engineers assume stronger guarantees than read committed. For data engineering: extracting a consistent snapshot from OLTP for a full load needs a repeatable-read snapshot (or logical replication slot snapshot), otherwise you read a mix of times. Long-running snapshot transactions hold back vacuum/purge. In lakehouse formats, writers use optimistic concurrency, and isolation (serializable vs snapshot) is configurable on writes per table.

Interview question: "What is write skew and which level prevents it?" Model answer: two transactions read the same condition and each updates different rows so that together they break an invariant (both doctors leave on-call); snapshot isolation allows it, serializable (SSI or locking) prevents it, or you materialize the conflict with a lock/constraint.
