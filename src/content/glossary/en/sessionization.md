---
term: "Sessionization"
aliases: ["session window", "session windows", "sessionize", "user sessions"]
category: streaming
short: "Grouping a user's events into sessions separated by an inactivity gap (for example 30 minutes)."
related: ["window-function", "event-time", "watermark"]
---
## In plain words (Junior)
When someone browses a website, they click around, leave for lunch, and come back. We want to count "visits", not individual clicks. If the person is inactive for more than, say, 30 minutes, we say that visit ended and the next click starts a new one. Sessionization applies this rule to raw event streams.

## How it works (Mid)
A session is a maximal sequence of events for the same key where consecutive events are less than a gap apart. Unlike fixed windows (tumbling/sliding), session windows have data-dependent boundaries and different lengths per user.

```
WITH flagged AS (
  SELECT *, CASE WHEN ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts)
                      > INTERVAL '30 minutes' THEN 1 ELSE 0 END AS new_sess
  FROM events
), numbered AS (
  SELECT *, SUM(new_sess) OVER (PARTITION BY user_id ORDER BY ts
                                ROWS UNBOUNDED PRECEDING) AS sess_no
  FROM flagged
)
SELECT user_id, sess_no, MIN(ts) AS start_ts, MAX(ts) AS end_ts, COUNT(*) AS events
FROM numbered GROUP BY user_id, sess_no;
```

The SQL pattern is "gaps and islands": flag a new session when the gap from the previous event exceeds the threshold, then cumulatively sum the flags to number sessions. In stream processors, session windows are built in (Flink `EventTimeSessionWindows.withGap`, Spark `session_window`): each event opens a window of `[ts, ts+gap)` and overlapping windows are merged.

## Senior angle & interview tips
Details that matter: sessions are computed in event time, so late events can merge two previously finished sessions (state and retraction complexity); state per key is held until the watermark passes `last_event + gap`; unbounded sessions (bots) need a max-duration cap; the gap is a product decision, so keep it configurable and document it. In batch, partitioning by user means a heavy user is a skew risk; sort cost dominates. Session metrics (duration, bounce, pages per session) feed funnels and retention analysis.

Interview question: "Compute sessions from a click table in SQL." Model answer: use LAG per user ordered by time to flag gaps over 30 minutes, take a running SUM of the flags as session id, then aggregate by user and session id; in streaming I'd use native session windows with a watermark.
