---
term: "Write-ahead log (WAL)"
aliases: ["wal", "write ahead log", "transaction log", "redo log", "commit log"]
category: storage
short: "An append-only log where changes are recorded before being applied, giving durability, crash recovery and a replication stream."
related: ["cdc", "mvcc", "checkpoint", "lsm-tree"]
---
## In plain words (Junior)
A shop owner writes every sale into a ledger line by line before putting money in the drawer. If the power goes out mid-day, the ledger tells exactly what happened and the drawer can be fixed. A write-ahead log is that ledger for a database: change first noted safely in order, then applied.

## How it works (Mid)
Writing to random places in data files is slow and not atomic. Databases instead append a compact record of each change to a sequential log and fsync it; only then is the transaction committed. Data pages are updated in memory and flushed later (checkpoint). After a crash the engine replays the WAL from the last checkpoint (redo) and rolls back uncommitted work (undo).

```
client: UPDATE accounts SET bal = bal - 10 WHERE id = 1;

1. append record to WAL     (sequential write)
2. fsync WAL                (now durable)
3. ack to client
4. later: flush dirty page to data file (checkpoint)

crash? -> replay WAL from last checkpoint
```

The same log is useful beyond recovery: replicas replay it (physical replication), CDC tools decode it (logical decoding in Postgres, binlog in MySQL), and point-in-time recovery restores a base backup plus archived WAL. Kafka's commit log, LSM trees (memtable protected by a log), and Raft use the same idea.

## Senior angle & interview tips
Sequential append gives durability at high throughput, but `fsync` latency bounds commit latency; group commit batches many transactions per fsync. Operational risks: WAL growth when a replica or replication slot lags (disk full), checkpoint spikes, and durability settings traded for speed (`synchronous_commit=off`, disabling fsync) that can lose recent commits. Torn writes require checksums or full-page writes.

Interview question: "How does a database survive a crash?" Model answer: changes hit the WAL and are fsynced before commit is acknowledged; on restart it replays the log from the last checkpoint to redo committed work and undo the rest, so durability and atomicity hold without writing data pages synchronously.
