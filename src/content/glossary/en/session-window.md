---
term: "Session window"
aliases: ["session windows","gap window"]
category: streaming
short: "A dynamic window that closes after a period of inactivity."
related: ["sessionization","sliding-window","tumbling-window"]
---
## In plain words (Junior)
A visit to a website: it starts with your first click and ends when you have been idle long enough, so each visitor gets sessions of different lengths.

## How it works (Mid)
Windows are defined per key by a gap timeout. Events within the gap of each other merge into one window; a gap larger than the timeout closes it. Late events can merge two previously separate sessions, so state must be kept until watermark passes end + allowed lateness.

## Senior angle & interview tips
Per-key dynamic windows cause unbounded state for chatty keys; cap session length. Choosing the gap is a product decision (30 minutes is a web convention). In batch the same idea is done with window functions (lag, running sum of "new session" flags).

Interview question: "How do you compute sessions in SQL?" Answer: flag rows where gap to previous event > threshold, cumulative-sum the flags to get a session id.
