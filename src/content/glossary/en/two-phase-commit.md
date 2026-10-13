---
term: "Two-phase commit (2PC)"
aliases: ["2pc", "two phase commit", "distributed transaction", "xa transaction"]
category: distributed
short: "A protocol where a coordinator first asks all participants to prepare, then tells them all to commit or abort, for atomic distributed transactions."
related: ["saga", "exactly-once", "outbox-pattern", "quorum"]
---
## In plain words (Junior)
Several friends need to agree to book one restaurant table for everyone. First the organizer asks, "Can you all make it?" Everyone promises to keep the date free. Only when everyone says yes does the organizer say "Booked!"; otherwise the plan is cancelled for all. Two-phase commit does this for databases so that either all of them apply a change or none.

## How it works (Mid)
Phase 1 (prepare): each participant makes its part durable and ready to commit, holds locks, and votes. Phase 2: if all voted yes, the coordinator durably records the decision and tells everyone to commit; any no or timeout leads to abort. After voting yes a participant cannot unilaterally change its mind; it must wait for the decision.

```
Phase 1 (prepare / vote):
  coordinator -> all participants: "PREPARE"
  each participant: durably log the changes, lock resources, reply YES or NO

Phase 2 (decide):
  if all YES: coordinator logs COMMIT, sends COMMIT to all
  else:       coordinator logs ABORT,  sends ABORT  to all

Danger: coordinator crashes after YES votes -> participants are blocked, holding locks
```

It gives atomicity across systems (XA transactions across databases, Kafka transactions across partitions and offsets, Flink's two-phase-commit sinks tied to checkpoints, Spanner). In Flink exactly-once sinks, pre-commit happens on checkpoint barrier and commit when the checkpoint completes.

## Senior angle & interview tips
The weakness is blocking: if the coordinator fails between phases, participants holding locks can't decide on their own, hurting availability; recovery needs the coordinator's durable log. 2PC also adds latency (extra round trips), lock contention and tight coupling, so it scales poorly across services. Alternatives: sagas with compensations (eventual consistency), the outbox pattern, idempotent consumers, or consensus-replicated coordinators (Spanner uses Paxos groups as participants to avoid single-point blocking). 3PC exists but is rarely used.

Interview question: "2PC vs saga?" Model answer: 2PC gives atomic, isolated commit but blocks on coordinator failure and holds locks, suiting few tightly coupled databases; sagas run local transactions with compensating actions, are available and scalable but only eventually consistent and need idempotent compensations.
