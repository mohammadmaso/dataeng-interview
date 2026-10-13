---
term: "Tumbling, sliding and session windows"
aliases: ["tumbling window", "sliding window", "hopping window", "windowing", "window types"]
category: streaming
short: "Ways of slicing an unbounded stream into finite groups by time: fixed non-overlapping, overlapping sliding, or activity-based session windows."
related: ["watermark", "event-time", "sessionization", "window-function"]
---
## In plain words (Junior)
Counting visitors per hour is like reading a meter at every full hour (tumbling). A "last 10 minutes, updated every minute" display is a window that slides along (sliding). Grouping a person's actions into "visits" separated by quiet pauses is a session window. Windows turn an endless stream into pieces you can aggregate.

## How it works (Mid)
Tumbling windows: fixed size, no overlap, each event belongs to exactly one window (hourly counts). Sliding (hopping) windows: fixed size with a slide smaller than the size, so windows overlap and each event belongs to size/slide windows (moving averages, rolling 10-minute error rate). Session windows: dynamic, closed by an inactivity gap per key. Global windows with custom triggers are also possible.

```
Tumbling 5 min:   |--00:00-00:05--|--00:05-00:10--|        each event in exactly 1 window
Sliding 10/5:     |-----00:00-00:10-----|                   size 10 min, slide 5 min:
                          |-----00:05-00:15-----|           each event in 2 windows
Session gap 30m:  |e e e|   gap   |e e e e|                 length varies per key

-- Flink SQL
SELECT window_start, COUNT(*) FROM TABLE(
  TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
GROUP BY window_start, window_end;
```

Windows are defined on event time or processing time; results are emitted when the watermark passes the window end (plus allowed lateness) or earlier via triggers for early/updating results. State per open window is kept until closing.

## Senior angle & interview tips
Sliding windows multiply state and compute by size/slide, so a 24h window sliding every minute is 1440x work per event; use incremental/pane-based aggregation or pre-aggregate in tumbling panes and combine. Choose emit semantics (final-only vs early updates with retractions) according to the consumer. Window alignment and time zones (daily windows in local time with DST) are classic bugs. Late events behind the watermark are dropped or updated per allowed lateness. In batch SQL, the equivalents are `GROUP BY date_trunc(...)` and window functions with frames.

Interview question: "Rolling 5-minute error rate updated every 30 seconds?" Model answer: a sliding event-time window of 5 minutes with a 30-second slide, aggregated incrementally with a watermark for lateness; to bound cost, compute 30-second tumbling partials and merge the last 10.
