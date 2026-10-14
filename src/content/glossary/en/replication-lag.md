---
term: "Replication lag"
aliases: ["replica lag","read-your-writes"]
category: distributed
short: "The delay between a write on the primary and its appearance on a replica."
related: ["replication","eventual-consistency","cdc"]
---
## In plain words (Junior)
You post a photo, refresh from another device and it is not there yet: the copy has not caught up.

## How it works (Mid)
Asynchronous replicas apply the primary’s log later, so reads from them can be stale. Lag grows under heavy writes, long queries blocking apply, or network issues. Monitor lag in seconds or bytes.

## Senior angle & interview tips
Guarantees to design for: read-your-writes (route a user’s reads to the primary shortly after writing), monotonic reads (stick a session to one replica), and consistent prefix. CDC consumers reading replicas inherit the lag.

Interview question: "User updates a profile and sees the old one. Fix?" Answer: read-your-writes routing or version/timestamp checks against the replica.
