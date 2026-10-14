---
term: "Rate limiting"
aliases: ["throttling","token bucket","leaky bucket"]
category: distributed
short: "Capping how many requests or operations a client or job can make per time unit."
related: ["backpressure","retry","circuit-breaker"]
---
## In plain words (Junior)
A turnstile that lets a limited number of people through per minute so the venue is not overwhelmed.

## How it works (Mid)
Algorithms: fixed window, sliding window, token bucket (allows bursts, refills at a steady rate) and leaky bucket (smooths output). Applied per user, key or IP, at API gateways or inside ingestion clients that call third-party APIs. Responses use 429 and Retry-After.

## Senior angle & interview tips
Distributed limits need shared state (Redis) or approximations. Clients should back off with jitter. In data pipelines, rate limiting protects source systems and SaaS APIs during backfills.

Interview question: "Token vs leaky bucket?" Answer: token bucket permits bursts up to bucket size; leaky bucket enforces a constant outflow.
