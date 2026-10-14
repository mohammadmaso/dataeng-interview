---
term: "Deadlock"
aliases: ["lock contention","deadlocks"]
category: distributed
short: "Two or more transactions each waiting on a lock the other holds, so none can proceed."
related: ["database-transaction","isolation-level","mvcc"]
---
## In plain words (Junior)
Two cars meeting on a narrow bridge, each waiting for the other to back up: nobody moves.

## How it works (Mid)
Transaction A locks row 1 and wants row 2; transaction B locks row 2 and wants row 1. The database detects the cycle in the wait-for graph and aborts one victim, which must be retried. Row/table locks, indexes without proper ordering, and gap locks make it more likely.

## Senior angle & interview tips
Prevent by acquiring locks in a consistent order, keeping transactions short, indexing to lock fewer rows, and using optimistic concurrency. Always code a retry on the deadlock error.

Interview question: "How do you avoid deadlocks in a batch updater?" Answer: sort keys before updating, commit in small batches and retry the victim.
