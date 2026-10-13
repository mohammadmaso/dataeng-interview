---
term: "Retry with backoff"
aliases: ["retries", "retry", "exponential backoff", "backoff and jitter", "jitter"]
category: distributed
short: "Re-attempting a failed operation after growing, randomized delays so transient errors recover without overloading the dependency."
related: ["idempotency", "dead-letter-queue", "backpressure"]
---
## In plain words (Junior)
If a shop's phone line is busy, calling again instantly ten times doesn't help; it makes the line busier. You wait a little, try again, wait longer, try again. Adding some randomness to the waits means that everyone doesn't call at the same second. That's retry with backoff and jitter.

## How it works (Mid)
Failures split into transient (timeouts, 503s, rate limits, leader elections) and permanent (bad input, auth, schema errors). Retry only transient ones, with a bounded number of attempts and exponentially growing delays plus jitter, so synchronized clients don't create retry storms ("thundering herd"). Set timeouts per attempt and an overall deadline.

```
import random, time

def call_with_retry(fn, max_attempts=5, base=0.5, cap=30):
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except TransientError:
            if attempt == max_attempts: raise
            delay = min(cap, base * 2 ** attempt)
            time.sleep(random.uniform(0, delay))     # "full jitter"
# Airflow: retries=3, retry_delay=timedelta(minutes=5), retry_exponential_backoff=True
```

Retries make operations run more than once, so the operation must be idempotent or protected by an idempotency key. At the pipeline level: orchestrator task retries (Airflow `retries`), Kafka producer retries (`enable.idempotence` prevents duplicates and reordering), consumer retry topics, and HTTP client policies. Circuit breakers stop retrying a dependency that is clearly down.

## Senior angle & interview tips
Failure modes: retry amplification across layers (3 retries at each of 4 layers = 81 calls), retrying non-idempotent side effects (double charge or duplicate rows), retries that hold up an ordered partition (use a retry topic and DLQ instead of blocking), and masking systemic problems with silent retries; always emit metrics and alerts on retry rates. Respect `Retry-After`/rate-limit headers. A retry budget (limit retries to a fraction of traffic) protects dependencies during outages.

Interview question: "Your Airflow task calls an API that is flaky; what do you configure?" Model answer: a small number of retries with exponential backoff and jitter, a per-attempt timeout, an idempotent request (idempotency key or upsert), and a failure alert after retries are exhausted; permanent errors fail fast without retrying.
