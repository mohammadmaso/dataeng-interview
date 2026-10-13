---
order: 27
tid: "sql-transactions"
track: "2"
section: null
num: "2.12"
title: "Transactions, Isolation & Locking"
type: "concept"
level: "advanced"
priority: "P1"
est_hours: 3
summary: "ACID, isolation levels (Read Uncommitted to Serializable), anomalies (dirty read, non-repeatable read, phantom, write skew), MVCC and locking."
covers: ["SELECT FOR UPDATE","deadlocks","optimistic vs pessimistic concurrency"]
interview_qs: []
extras: []
prerequisites: ["sql-fundamentals", "sql-indexing"]
related: ["wal-mvcc", "distributed-transactions", "consistency-models", "kafka-exactly-once", "sql-ddl-merge"]
---
## 🎯 TL;DR

> A **transaction** groups several statements into one **all-or-nothing** unit. **Isolation** decides what a transaction may see of other transactions that run at the same time. Weaker isolation is faster but lets anomalies through; stronger isolation is safer but needs locks, retries, or both.

- **ACID**: Atomicity (all or nothing), Consistency (constraints hold), Isolation (concurrent work does not interfere more than the chosen level allows), Durability (committed means kept).
- The isolation levels (SQL standard) are Read Uncommitted, Read Committed, Repeatable Read and Serializable. Engines differ in what each one actually prevents.
- Anomalies: **dirty read**, **non-repeatable read**, **phantom**, **lost update** and **write skew**. Write skew surprises most people.
- Protection comes from **locks** (`SELECT ... FOR UPDATE`), **MVCC** snapshots, or **serializable** checking that aborts and retries.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A bank transfer. Money leaves account A and arrives in account B. If the power fails between the two steps, the money must not vanish. A transaction means both steps happen, or neither does.

The examples below use PostgreSQL.

```sql
CREATE TABLE accounts (id int PRIMARY KEY, owner text, balance numeric NOT NULL CHECK (balance >= 0));
INSERT INTO accounts VALUES (1, 'Ali', 500), (2, 'Sara', 100);

BEGIN;
UPDATE accounts SET balance = balance - 200 WHERE id = 1;
UPDATE accounts SET balance = balance + 200 WHERE id = 2;
COMMIT;
```

Before `COMMIT`, no other session sees the change. If anything fails before `COMMIT` (or you run `ROLLBACK`), both updates are undone. That is **atomicity**. Once `COMMIT` returns, the result survives a crash. That is **durability**.

The `CHECK` constraint is part of **consistency**: an update that would make a balance negative fails, and the transaction with it.

## 🔧 How it really works (Mid)

**Isolation is about concurrency.** Two sessions run at the same time. The isolation level sets which interleavings are allowed. The SQL standard defines the levels by the anomalies they must prevent:

| Level | Dirty read | Non-repeatable read | Phantom | Write skew |
|---|---|---|---|---|
| Read Uncommitted | possible | possible | possible | possible |
| Read Committed | prevented | possible | possible | possible |
| Repeatable Read | prevented | prevented | possible in standard (prevented in PostgreSQL) | possible |
| Serializable | prevented | prevented | prevented | prevented |

Engine reality matters more than the table. In PostgreSQL, `READ UNCOMMITTED` behaves like `READ COMMITTED`, so dirty reads never happen. Its `REPEATABLE READ` is snapshot isolation, and its `SERIALIZABLE` is true serializable checking. Check your engine's docs for each level before you reason about a bug.

**The anomalies in concrete form.** Each one needs two sessions.

- **Non-repeatable read** (Read Committed): session A reads Ali's balance (500). Session B commits `balance = 300`. A reads again and sees 300. Two reads inside one transaction disagree.
- **Phantom**: A runs `SELECT count(*) FROM accounts WHERE balance > 200`. B inserts a matching row and commits. A runs the same count and gets a different number.
- **Lost update** (application read-modify-write):

```text
A: SELECT balance -> 500          B: SELECT balance -> 500
A: write 500 - 200 = 300          B: write 500 - 50 = 450
A: COMMIT                         B: COMMIT
-> final balance 450; the 200 withdrawal is lost
```

The fix is to make the database do the arithmetic, `UPDATE accounts SET balance = balance - 50 WHERE id = 1`, or to lock the row first.

**Write skew** is the anomaly that does not involve the same row. A rule says "at least one doctor must stay on call". Table `oncall(doctor, on_call)` has Alice and Bob on call.

```sql
CREATE TABLE oncall (doctor text PRIMARY KEY, on_call boolean);
INSERT INTO oncall VALUES ('Alice', true), ('Bob', true);
```

Each session runs the same logic:

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM oncall WHERE on_call;   -- sees 2, so it is safe to leave
UPDATE oncall SET on_call = false WHERE doctor = 'Alice';   -- Bob runs the same with 'Bob'
COMMIT;
```

Under snapshot isolation both transactions see 2, they update **different rows**, so no lock conflict occurs, and both commit. Nobody is on call. Under `SERIALIZABLE`, PostgreSQL detects that the outcome is not equivalent to any serial order and aborts one transaction with SQLSTATE `40001` (serialization failure). The application must retry it.

**Locks.** Pessimistic control locks the row before you act:

```sql
BEGIN;
SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;   -- row lock until COMMIT
UPDATE accounts SET balance = balance - 50 WHERE id = 1;
COMMIT;
```

Other sessions that try to `UPDATE` or `FOR UPDATE` the same row wait. `FOR SHARE` is the weaker form for readers. `SKIP LOCKED` makes a queue worker take the next free row instead of waiting, which is useful for job tables.

**Deadlocks.** Two transactions each hold a lock the other needs:

```text
T1: UPDATE accounts WHERE id = 1   (locks row 1)
T2: UPDATE accounts WHERE id = 2   (locks row 2)
T1: UPDATE accounts WHERE id = 2   (waits for T2)
T2: UPDATE accounts WHERE id = 1   (waits for T1)  -> deadlock
```

PostgreSQL detects the cycle and aborts one transaction (SQLSTATE `40P01`). The standard prevention is to **always lock rows in the same order**, for example ascending `id`.

**MVCC.** Readers in PostgreSQL do not block writers, and writers do not block readers, because each row version is kept with its transaction visibility. A reader sees a snapshot. The cost is old row versions that `VACUUM` must clean up. See [[mvcc]].

**Optimistic control** avoids holding locks. Each row carries a version number, and an update succeeds only if the version is unchanged:

```sql
ALTER TABLE accounts ADD COLUMN version int NOT NULL DEFAULT 0;

UPDATE accounts
SET balance = balance - 50, version = version + 1
WHERE id = 1 AND version = 7;
-- application checks the row count: 0 means someone else changed it; re-read and retry
```

## 🧠 Senior depth & trade-offs

- **Pick the isolation level by the invariant, not by habit.** Most applications run on Read Committed and are fine, until a rule spans several rows (write skew) or reads a count and acts on it. For those, use `SERIALIZABLE` with a retry loop, or make the constraint a real constraint (a unique key, an exclusion constraint, or a trigger).
- **Serializable costs retries.** Under contention, SSI aborts transactions. Your application needs idempotent retry logic and a retry limit. Without it, a "fix" becomes a production outage under load.
- **Long transactions are expensive.** A transaction holding a snapshot blocks `VACUUM` from reclaiming old versions, and holding locks blocks other writers. Keep transactions short, do not call external services inside them, and set `idle_in_transaction_session_timeout` for application pools.
- **Lock granularity and escalation.** Row locks are cheap individually, but a huge `UPDATE` touches millions of rows and holds them all until commit. Batch large changes (for example 10,000 rows per transaction) and commit between batches.
- **Warehouses and lakes work differently.** Snowflake and BigQuery support multi-statement transactions with their own rules. Delta Lake and Apache Iceberg commit a table change atomically with optimistic concurrency: two writers that touch the same files conflict at commit time, and one retries. Batch engines rarely offer row locks. Check each engine's documentation; many have no `SELECT FOR UPDATE`.
- **ClickHouse is not an OLTP transaction engine.** Its model is append-heavy, with merges in the background. Do not design application invariants that need multi-row transactions there. Keep the source of truth in PostgreSQL or a similar OLTP database, and stream changes out with [[cdc]].
- **Cross-system atomicity** (a database write plus a Kafka message) is not a single transaction. Use the transactional outbox or Kafka transactions, see [[distributed-transactions]] and [[kafka-exactly-once]].
- **When not to use serializable everywhere.** It is the right default for money movement and inventory, and a poor default for analytics or logging, where a retry storm adds no value.

## 🧪 Try it

Use the `accounts` table from the beginning (PostgreSQL 13 or later). Open two `psql` sessions for the concurrency exercises.

1. Show a **lost update** in two sessions, then fix it with `SELECT ... FOR UPDATE`. Describe the timeline you observed.
2. Reproduce **write skew** with `oncall` under `REPEATABLE READ`, then run the same steps under `SERIALIZABLE`. Report which session fails and with which SQLSTATE.
3. Write a worker query that claims one pending job from a `jobs(id, status)` table without blocking other workers.

<details><summary>Show solutions</summary>

**1.** Session A and B both run `SELECT balance FROM accounts WHERE id = 1;` and then `UPDATE accounts SET balance = <value read> - 50 WHERE id = 1;`. Both can end with the same wrong balance: one of the two withdrawals disappears. The fix:
```sql
BEGIN;
SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;
UPDATE accounts SET balance = balance - 50 WHERE id = 1;
COMMIT;
```
The second session waits at the `SELECT ... FOR UPDATE` until the first commits, then reads the new balance. Or skip the read entirely with `UPDATE accounts SET balance = balance - 50 WHERE id = 1`, which takes the row lock itself.

**2.** Under `REPEATABLE READ` both `COMMIT`s succeed, and `SELECT count(*) FROM oncall WHERE on_call` returns 0 afterwards. Under `SERIALIZABLE`, the second `COMMIT` (or one of the statements) fails with SQLSTATE `40001`, and the application retries; the retry sees one doctor on call and refuses to proceed. The exact moment of the error can differ, so check the outcome, not the line.

**3.**
```sql
UPDATE jobs
SET status = 'running'
WHERE id = (
  SELECT id FROM jobs
  WHERE status = 'pending'
  ORDER BY id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING id;
```
`SKIP LOCKED` skips rows another worker has locked, so workers do not queue behind each other. The whole statement is one transaction in autocommit mode.

</details>

## 💬 Interview corner

**Question: "What is the difference between isolation levels?"** Mid answer: "They define which anomalies are allowed, from dirty reads at the lowest level to none at Serializable." Senior answer: "The table in the textbook is a floor, not the truth. In PostgreSQL, Repeatable Read is snapshot isolation and still allows write skew, so for invariants across rows I use Serializable with retries or enforce the rule with a constraint."

**Question: "How do you prevent a lost update?"** Mid: "Use `SELECT ... FOR UPDATE`, or do the arithmetic in the `UPDATE` itself." Senior: "Either pessimistic locking, or optimistic versioning with a retry, depending on contention. Optimistic is better when conflicts are rare, and pessimistic when they are frequent and retries are expensive."

**Question: "Why does my transaction deadlock?"** Expected answer: two transactions lock the same rows in a different order. The fix is a consistent ordering, and short transactions.

> **🎤 Say this in the interview:** "I think of isolation as a list of anomalies I must rule out. For most queries Read Committed is fine. When an invariant spans rows, like write skew, I use Serializable with retries, or I enforce it with a constraint, because the default level will not protect it."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Reading a value in the application, computing something, and writing it back is a race condition. Do the update in SQL, or lock the row first.

> **⚠️ Watch out:** Assuming `REPEATABLE READ` prevents all anomalies. Write skew still happens under snapshot isolation.

- Treating the isolation level name as the same across engines. MySQL InnoDB, Oracle and PostgreSQL implement them differently.
- Calling an external API inside a transaction, which holds locks for seconds.
- Locking rows in different orders in different code paths, which causes deadlocks.
- Ignoring SQLSTATE `40001` and `40P01` in the application. These errors are expected and should be retried.
- Using `SERIALIZABLE` without retries, which turns occasional aborts into failed requests.
- Running a multi-million-row `UPDATE` in one transaction, which keeps locks and old versions around for hours.

## ✅ Check yourself (quiz)

1. Which property guarantees that a crash after `COMMIT` does not lose the transfer?
   - A) Atomicity
   - B) Consistency
   - C) Isolation
   - D) Durability

<details><summary>Answer</summary>

**D** — durability means a committed transaction survives failures. Atomicity is about all-or-nothing within the transaction.

</details>

2. Two transactions both check that at least one doctor is on call, then each removes a different doctor. Under snapshot isolation, both commit. What is the anomaly?
   - A) Dirty read
   - B) Non-repeatable read
   - C) Phantom
   - D) Write skew

<details><summary>Answer</summary>

**D** — write skew: the transactions read overlapping data, write different rows, and together break a rule. Row locks do not see the conflict, so only serializable checking or a constraint catches it.

</details>

3. In PostgreSQL, which statement describes `READ UNCOMMITTED`?
   - A) It allows dirty reads
   - B) It behaves like `READ COMMITTED`, so dirty reads do not occur
   - C) It is the same as `SERIALIZABLE`
   - D) It is not accepted by the server

<details><summary>Answer</summary>

**B** — PostgreSQL accepts the level but never returns uncommitted data. Other engines may differ, so check the docs.

</details>

4. **(Senior)** A service reads `SELECT count(*) FROM shifts WHERE day = ?`, and if the count is below the limit it inserts a new shift. Under heavy concurrency the limit is exceeded. Which fix is most robust?

   - A) Add `ORDER BY` to the count
   - B) Add a database constraint or a lock that enforces the rule (for example a `FOR UPDATE` on a per-day row, or a unique key on the slot), and keep serializable retries as a backstop
   - C) Move the count into the application cache
   - D) Use `READ UNCOMMITTED` to make it faster

<details><summary>Answer</summary>

**B** — the rule spans rows, which is the write-skew pattern. A lock on a shared row (such as the day) or a database-enforced unique slot makes the check and insert conflict. An application cache or a lower isolation level makes the race more likely.

</details>

5. **(Senior)** Under `SERIALIZABLE` with a high-traffic endpoint, error rates rise sharply after the change. What should the design include, and what should you avoid?

   - A) Turn off serializable checking for that endpoint
   - B) Keep transactions short, retry on SQLSTATE 40001 with a bounded and idempotent retry, and avoid putting external calls inside the transaction
   - C) Remove all `UPDATE` statements
   - D) Increase the `deadlock_timeout` value to zero

<details><summary>Answer</summary>

**B** — serializable aborts are normal under contention, so the application must retry safely with a limit. Shorter transactions reduce conflicts and lock time, so the retry rate drops.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| ACID | Atomicity, Consistency, Isolation, Durability |
| Dirty read | Reading another transaction's uncommitted change |
| Non-repeatable read | The same row returns different values within one transaction |
| Phantom | The same query returns different rows within one transaction |
| Write skew | Transactions read overlapping data, write different rows, and break a rule together |
| `SELECT ... FOR UPDATE` | Locks the selected rows until commit, so other writers wait |
| `SKIP LOCKED` | Skip rows another session has locked, useful for job queues |
| SQLSTATE 40001 / 40P01 | Serialization failure / deadlock detected; both should be retried |

## 🔗 Go further

- [[acid]] and [[isolation-level]] — the glossary definitions behind this lesson.
- [[mvcc]] and [[wal-mvcc]] — how snapshots and durability are implemented.
- [[distributed-transactions]] — when a transaction spans more than one system.
- [[kafka-exactly-once]] — transactions across a database and a stream.
- [[consistency-models]] — the distributed-systems view of the same ideas.

