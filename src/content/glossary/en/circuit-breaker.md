---
term: "Circuit breaker"
aliases: ["circuit breaker pattern","fail fast"]
category: distributed
short: "A guard that stops calling a failing dependency for a while to let it recover."
related: ["retry","rate-limiting","backpressure"]
---
## In plain words (Junior)
Like an electrical breaker: when something keeps failing, it trips and cuts the line instead of burning the house down.

## How it works (Mid)
States: closed (calls pass), open (calls fail immediately after error threshold), half-open (a few trial calls test recovery). Prevents retry storms and thread exhaustion when a downstream is slow or down.

## Senior angle & interview tips
Combine with timeouts, bounded retries with jitter and fallbacks. For pipelines, a breaker on a flaky source API avoids hammering it and lets the DLQ catch work for later.

Interview question: "Retries made the outage worse. Why?" Answer: synchronized retries amplify load; use backoff, jitter, and a circuit breaker.
