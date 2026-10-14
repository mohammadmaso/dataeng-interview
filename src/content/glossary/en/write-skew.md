---
term: "Write skew"
aliases: ["snapshot anomaly"]
category: distributed
short: "An anomaly where two concurrent transactions each read overlapping data and write different rows, breaking an invariant."
related: ["isolation-level","mvcc","database-transaction"]
---
## In plain words (Junior)
Two on-call doctors each see that the other is on duty and both go off duty at the same time, leaving nobody on call.

## How it works (Mid)
Under snapshot isolation each transaction sees a consistent snapshot and checks a condition that holds, then writes a different row; no write-write conflict is detected, yet the combined result violates the rule. Only serializable isolation (or explicit locking such as SELECT ... FOR UPDATE) prevents it.

## Senior angle & interview tips
Fixes: use serializable isolation, lock the rows read, materialize the conflict into a single row, or add a constraint. It is the classic example showing that snapshot isolation is not serializable.

Interview question: "Give an anomaly snapshot isolation allows." Answer: write skew, as in the double on-call booking.
