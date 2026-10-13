---
order: 35
tid: "python-production"
track: "3"
section: null
num: "3.6"
title: "Production-Grade Python for Pipelines"
type: "practice"
level: "intermediate"
priority: "P1"
est_hours: 3
summary: "Reliable pipeline code: retry with backoff, idempotency, structured logging, config and secrets management, testing with pytest and Testcontainers, packaging with uv/poetry, linting and type checking."
covers: ["project structure","unit vs integration tests for data","mocking sources","pydantic for schema validation"]
interview_qs: []
extras: []
prerequisites: ["python-core", "python-concurrency"]
related: ["python-dataframes", "api-ingestion", "dedup-idempotent-loads", "dlq-bad-records", "data-quality", "dataops-cicd", "docker"]
---

## 🎯 TL;DR

> A pipeline script that works once on your laptop is not a pipeline. A **production** pipeline is code that **can be run again safely**, **fails loudly with context**, **reads its settings from outside the code**, and is **tested** before it touches real data.

- **Retry** only transient errors, with exponential backoff and jitter.
- **Idempotency**: running the same batch twice must leave the same result. Use keys and upserts, not blind inserts.
- **Structured logs** (JSON, with fields) so you can query failures, not grep sentences.
- **Config** from environment variables; **secrets** never in the repository.
- **Tests**: fast unit tests for transforms and validation, a few integration tests against a real database in a container.
- **Tooling**: a project with a lockfile (`uv` or `poetry`), `ruff` for lint, `mypy` for types, `pytest` for tests.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Think of a kitchen. A recipe that works once is a note on a napkin. A production recipe lists the exact ingredients (lockfile), the oven setting from the wall, not from memory (config), a timer and a plan for when the oven fails (retry), and a tasting before serving (tests).

Here is the smallest version of a pipeline step that already follows these rules. It validates input, skips bad rows, and logs a summary:

```python
import json
import logging

from pydantic import BaseModel, Field, ValidationError

log = logging.getLogger("pipeline")
logging.basicConfig(level=logging.INFO)

class Order(BaseModel):
    order_id: int = Field(gt=0)
    customer: str = Field(min_length=1)
    amount: float = Field(ge=0)

raw = [
    {"order_id": 1, "customer": "ali", "amount": 10.0},
    {"order_id": -2, "customer": "sara", "amount": 5.0},   # bad: id must be > 0
    {"order_id": 3, "customer": "", "amount": 7.0},        # bad: empty customer
]

valid, rejected = [], 0
for row in raw:
    try:
        valid.append(Order.model_validate(row))
    except ValidationError:
        rejected += 1

log.info("batch done: %d valid, %d rejected", len(valid), rejected)
# INFO:pipeline:batch done: 1 valid, 2 rejected
```

Two ideas to keep: **reject bad records explicitly** instead of letting them break the run or slip through, and **report counts** so someone can see what happened.

## 🔧 How it really works (Mid)

### Project structure

A pipeline that will be run by others (Airflow, a cron job, a container) should be an installable package, not a loose script:

```text
orders-pipeline/
  pyproject.toml          # dependencies, tool config, entry point
  uv.lock                 # exact versions (commit it)
  src/pipeline/
    __init__.py
    models.py             # schemas (pydantic)
    retry.py              # retry helper
    load.py               # idempotent writes
    config.py             # settings from the environment
  tests/
    test_models.py
    test_retry.py
    test_load.py
```

The `src/` layout forces tests to import the installed package, which catches packaging mistakes early. Create it with `uv init --package orders-pipeline`, add dependencies with `uv add pydantic`, and dev tools with `uv add --dev pytest ruff mypy`. Run everything with `uv run pytest`, `uv run ruff check .` and `uv run mypy src`.

### Schemas with pydantic

Validate at the boundary, where data enters your code. Pydantic v2 converts types and enforces constraints:

```python
# src/pipeline/models.py
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

class Order(BaseModel):
    order_id: int = Field(gt=0)
    customer: str = Field(min_length=1)
    amount: Decimal = Field(ge=0)
    created_at: datetime
```

`Decimal` avoids float rounding for money. `model_validate` accepts strings and converts them: `"12.50"` becomes `Decimal("12.50")`, and `"2026-01-05T10:00:00"` becomes a `datetime`. A failed check raises `ValidationError` with a list of every problem, which is exactly what you want in a log.

### Retry with backoff and jitter

Retry only **transient** failures (timeouts, connection resets, HTTP 429 and 503). Retrying a `ValidationError` or an HTTP 400 is wasted time. Exponential backoff spaces attempts out; jitter (a random delay) stops many workers from retrying at the same instant:

```python
# src/pipeline/retry.py
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

def retry(
    attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,  # injectable for tests
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except retry_on:
                    if attempt == attempts:
                        raise
                    delay = min(max_delay, base_delay * 2 ** (attempt - 1))
                    sleep(random.uniform(0, delay))  # "full jitter"
            raise AssertionError("unreachable")
        return wrapper
    return decorator
```

Usage: `@retry(attempts=5, retry_on=(ConnectionError, TimeoutError))`. Notice the design choices: the caller names the retryable errors, the last failure is re-raised (never swallowed), and `sleep` is a parameter so tests run instantly.

### Idempotent loads

Idempotent means: running the same batch twice gives the same final state. Retries, reruns after a crash and backfills all depend on it. The mechanism is a **business key** and an **upsert** (insert or update by key), not a plain `INSERT`:

```python
# src/pipeline/load.py
from collections.abc import Iterable
from typing import Protocol

from pydantic import ValidationError

from pipeline.models import Order

class OrderStore(Protocol):
    def upsert(self, orders: list[Order]) -> None: ...

def load_orders(raw_rows: Iterable[dict], store: OrderStore) -> int:
    valid: list[Order] = []
    rejected = 0
    for row in raw_rows:
        try:
            valid.append(Order.model_validate(row))
        except ValidationError:
            rejected += 1
    store.upsert(valid)
    return rejected

class MemoryStore:
    """In-memory store for tests. A real one would run INSERT ... ON CONFLICT (order_id) DO UPDATE."""

    def __init__(self) -> None:
        self.rows: dict[int, Order] = {}

    def upsert(self, orders: list[Order]) -> None:
        for o in orders:
            self.rows[o.order_id] = o
```

With a `Protocol`, the production code depends on the behaviour you need (`upsert`), not on a database library, which makes it easy to test.

### Structured logging

Logs are data. Put facts in fields, so you can filter by `rows_rejected > 0` in your log platform:

```python
# src/pipeline/logs.py
import json
import logging

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"level": record.levelname, "logger": record.name, "msg": record.getMessage()}
        payload.update(getattr(record, "ctx", {}))
        return json.dumps(payload)

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
log = logging.getLogger("pipeline")
log.addHandler(handler)
log.setLevel(logging.INFO)

log.info("batch loaded", extra={"ctx": {"source": "orders_api", "rows": 3, "rejected": 1}})
# {"level": "INFO", "logger": "pipeline", "msg": "batch loaded", "source": "orders_api", "rows": 3, "rejected": 1}
```

Log the batch id, the source, counts in and out, the duration, and the retry count. Never log secrets, full personal data or whole payloads.

### Configuration and secrets

Configuration (batch size, endpoints) comes from environment variables, which differ per environment. Secrets (passwords, tokens) come from the environment or a secret manager, injected at runtime, and never from a file in git. Fail fast when a required value is missing:

```python
# src/pipeline/config.py
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    db_url: str
    batch_size: int

def load_settings() -> Settings:
    return Settings(
        db_url=os.environ["PIPELINE_DB_URL"],          # KeyError at startup if missing: good
        batch_size=int(os.getenv("PIPELINE_BATCH_SIZE", "500")),
    )
```

`os.environ[...]` (not `.get`) makes a missing secret fail at startup, not three hours into the run.

### Tests

Test in layers. Unit tests are fast and need no services. Integration tests use a real database in a container:

```python
# tests/test_models.py
import pytest
from pydantic import ValidationError

from pipeline.models import Order

def test_valid_order_converts_types():
    o = Order.model_validate({"order_id": "7", "customer": "ali",
                              "amount": "12.50", "created_at": "2026-01-05T10:00:00"})
    assert o.order_id == 7
    assert str(o.amount) == "12.50"

def test_negative_amount_is_rejected():
    with pytest.raises(ValidationError):
        Order.model_validate({"order_id": 1, "customer": "ali",
                              "amount": "-1", "created_at": "2026-01-05T10:00:00"})
```

```python
# tests/test_retry.py
import pytest

from pipeline.retry import retry

def test_retries_then_succeeds():
    calls = {"n": 0}
    sleeps: list[float] = []

    @retry(attempts=3, base_delay=1.0, sleep=sleeps.append)
    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("boom")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3
    assert len(sleeps) == 2          # slept between attempts, not after the last

def test_gives_up_and_reraises():
    @retry(attempts=2, sleep=lambda _: None, retry_on=(ConnectionError,))
    def always_down() -> None:
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        always_down()
```

```python
# tests/test_load.py
from pipeline.load import MemoryStore, load_orders

ROWS = [
    {"order_id": 1, "customer": "ali", "amount": "10", "created_at": "2026-01-05T10:00:00"},
    {"order_id": 2, "customer": "sara", "amount": "5", "created_at": "2026-01-05T11:00:00"},
    {"order_id": 3, "customer": "", "amount": "7", "created_at": "2026-01-05T12:00:00"},
]

def test_rerun_is_idempotent():
    store = MemoryStore()
    load_orders(ROWS, store)
    rejected = load_orders(ROWS, store)     # the same batch again
    assert len(store.rows) == 2             # no duplicates
    assert rejected == 1                    # the empty customer
```

**Mocking sources.** Replace the network with a fake at the function boundary, using `unittest.mock.patch` or pytest's `monkeypatch`, for example `monkeypatch.setattr("pipeline.source.fetch_page", lambda url: [...])`. Mock the boundary you own (your `fetch_page` wrapper), not the library internals.

**Integration tests with Testcontainers.** When the SQL itself matters (`ON CONFLICT`, types, constraints), test against a real engine. The `testcontainers` package starts a throwaway Docker container for the test session:

```python
# tests/integration/conftest.py   (requires Docker and: uv add --dev testcontainers[postgres])
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def pg_url():
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url()
```

Mark these tests (`@pytest.mark.integration`) and run them separately in CI, because they are slower.

### Lint, types and packaging

- `ruff` finds unused imports, bugs and style issues in one fast tool (`uv run ruff check .`, `uv run ruff format .`).
- `mypy` checks annotations (`uv run mypy src`). Enable `strict = true` under `[tool.mypy]` in `pyproject.toml` once the code is typed.
- The lockfile (`uv.lock` or `poetry.lock`) pins every transitive dependency, so a build on Monday and on Friday installs the same packages.

## 🧠 Senior depth & trade-offs

- **Idempotency is a property of the whole path, not one function.** Retries are only safe if the write is keyed. Upserts on `order_id` are idempotent; `INSERT` with a generated UUID is not. Delete-then-insert per partition is idempotent if it is atomic. Read [[dedup-idempotent-loads]] for the warehouse-side patterns.
- **Retries multiply load.** Five attempts with a 30-second max delay on 200 workers is a retry storm against a database that just recovered. Cap total retry time, use a circuit breaker for hard outages, and retry at the smallest unit of work.
- **Dead-letter, don't drop.** Rejected rows should go somewhere queryable (a table or a file with the error and the source line). A count is not enough for an audit. See [[dlq-bad-records]].
- **Validation has a cost.** Pydantic per row is slower than a vectorized schema check on a DataFrame or the database's own constraints. For tens of millions of rows, validate in bulk (Polars or DuckDB, with the schema declared) and keep row-level models for small or sensitive streams. Measure.
- **Contracts beyond the code.** A pydantic model is a local contract. A shared schema registry or data contract makes the producer and consumer agree on fields and evolution rules ([[data-quality]]).
- **Secrets hygiene.** Rotate them, scope them to the minimum permissions, never print them in exceptions (a connection string in a traceback leaks the password), and check that logs and error reports redact them.
- **Reproducible builds.** Commit the lockfile, pin the Python version (`.python-version`), and build a container image from the lock. Dependency drift between environments is one of the most common causes of "works on my machine".
- **Testing trade-offs.** Mocks are fast but can hide SQL mistakes. Container tests catch them but need Docker in CI. A healthy split: many unit tests, a few integration tests per critical query, and one end-to-end run on a small fixture in staging. See [[dataops-cicd]].
- **What to monitor.** Rows in, rows out, rows rejected, retry count per step, batch duration, and freshness (time since the last successful load). Alert on rejection ratio, not on single errors.

## 🧪 Try it

Build the project above (or the snippets in the lesson) in a fresh folder with `uv`. Then do these three exercises.

1. **Cap test.** Write a test that proves the retry delay never exceeds `max_delay`, even for attempt 10. Use the `sleep` injection to record delays.
2. **Rejection threshold.** Change `load_orders` so that it raises `RuntimeError` when more than 5 % of the rows are rejected. Write a test with 1 bad row out of 10 (passes) and 2 bad rows out of 10 (raises).
3. **Duplicates in one batch.** The current upsert keeps the last row for each key. Change `MemoryStore.upsert` so that, for duplicate keys in the same batch, it keeps the row with the latest `created_at`. Add a test where the newer row comes first in the list.

<details><summary>Show solutions</summary>

**1.**
```python
def test_delay_is_capped():
    sleeps: list[float] = []

    @retry(attempts=10, base_delay=1.0, max_delay=4.0, sleep=sleeps.append)
    def always_fails() -> None:
        raise ConnectionError

    with pytest.raises(ConnectionError):
        always_fails()
    assert len(sleeps) == 9
    assert all(0 <= s <= 4.0 for s in sleeps)
```
The upper bound of each jittered delay is `min(4.0, 2**(n-1))`, so no sleep can exceed 4.0 seconds.

**2.**
```python
def load_orders(raw_rows, store):   # same as before, plus a guard before upsert
    rows = list(raw_rows)
    valid, rejected = [], 0
    for row in rows:
        try:
            valid.append(Order.model_validate(row))
        except ValidationError:
            rejected += 1
    if rows and rejected / len(rows) > 0.05:
        raise RuntimeError(f"too many rejected rows: {rejected}/{len(rows)}")
    store.upsert(valid)
    return rejected

def test_threshold():
    good = {"order_id": 1, "customer": "a", "amount": "1", "created_at": "2026-01-01T00:00:00"}
    bad = {"order_id": 0, "customer": "a", "amount": "1", "created_at": "2026-01-01T00:00:00"}
    assert load_orders([good] * 19 + [bad], MemoryStore()) == 1   # 1/20 = 5 %, not above 5 %
    with pytest.raises(RuntimeError):
        load_orders([good] * 8 + [bad, bad], MemoryStore())       # 2/10 = 20 %
```
The guard must run before the upsert, so a bad batch never reaches the store. Note that 1 bad row out of 10 (10 %) would correctly raise with this threshold.

**3.**
```python
class MemoryStore:
    def __init__(self) -> None:
        self.rows: dict[int, Order] = {}

    def upsert(self, orders: list[Order]) -> None:
        for o in orders:
            current = self.rows.get(o.order_id)
            if current is None or o.created_at >= current.created_at:
                self.rows[o.order_id] = o

def test_latest_wins_regardless_of_order():
    store = MemoryStore()
    old = Order.model_validate({"order_id": 1, "customer": "a", "amount": "1", "created_at": "2026-01-01T00:00:00"})
    new = Order.model_validate({"order_id": 1, "customer": "a", "amount": "2", "created_at": "2026-02-01T00:00:00"})
    store.upsert([new, old])
    assert store.rows[1].amount == 2
```
The comparison uses the event time, so the result does not depend on arrival order. The same rule, applied in SQL with `ROW_NUMBER` or `MERGE`, is the warehouse version of this exercise.

</details>

## 💬 Interview corner

**Question: "How do you make a pipeline safe to rerun?"**
Mid answer: "I make writes idempotent with upserts on a business key, and I use retries only for transient errors."
Senior answer: "Every write is keyed, so a rerun or a retry converges to the same state. Each step is atomic at the partition level, so a half-written batch is never visible. I retry transient errors with capped exponential backoff and jitter, and I send rejected records to a dead-letter store with the reason. I can prove this with a test that runs the same batch twice and compares row counts and checksums."

**Question: "What do you test in a data pipeline?"**
Mid: "Unit tests for transformations and schemas, and an integration test for the database query."
Senior: "Fast unit tests for transforms and validation with fake boundaries, container-based integration tests for the SQL that carries semantics (upserts, constraints, types), and a small end-to-end run on fixed data in CI. I also test idempotency and the rejection threshold, because those are what fail at 3 a.m."

> **🎤 Say this in the interview:** "I treat a pipeline as software: validated inputs, idempotent keyed writes, retries only for transient failures, structured logs with counts, configuration from the environment, and tests at three levels. The goal is that a rerun is boring."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Retrying everything. A `ValidationError` or HTTP 400 will fail forever; retrying only delays the alert and multiplies load.

> **⚠️ Watch out:** `except Exception: pass` around a load step. The run "succeeds" with missing data, and nobody knows until a report is wrong.

- Plain `INSERT` in a retried step, which creates duplicates on every retry.
- Hard-coding passwords or connection strings in code or in a committed `.env` file.
- Using `os.getenv("DB_PASSWORD")` without a default check, so the job starts with `None` and fails later in a confusing way.
- Logging the whole row, including personal data, in an error message.
- Running tests only on your laptop with your local database, which then hides a missing migration.
- Skipping the lockfile, so a new library release changes behaviour overnight.

## ✅ Check yourself (quiz)

1. Which error should a pipeline retry with exponential backoff?
   - A) `pydantic.ValidationError` on a malformed row
   - B) HTTP 400 Bad Request from a bad query
   - C) A timeout or HTTP 503 from a temporarily overloaded API
   - D) A `KeyError` for a missing environment variable

<details><summary>Answer</summary>

**C** — timeouts and 503s are transient; retrying can succeed. Validation errors, 400s and configuration errors are permanent and will fail every time, so retrying only wastes time.

</details>

2. A step inserts rows with a generated UUID and is retried after a timeout. What is the problem and the fix?
   - A) UUIDs are too slow; use integers
   - B) A retry after a timeout that actually committed inserts duplicates; use a business key and an upsert (`ON CONFLICT ... DO UPDATE`) so the rerun converges
   - C) Retries must be disabled for all inserts
   - D) The problem is the logging level

<details><summary>Answer</summary>

**B** — the first attempt may have committed before the timeout reached the client. A new UUID makes the second write a different row. A deterministic business key with an upsert makes the operation idempotent.

</details>

3. Why does `os.environ["PIPELINE_DB_PASSWORD"]` serve a production pipeline better than `os.getenv("PIPELINE_DB_PASSWORD")`?
   - A) It is faster
   - B) It raises a `KeyError` at startup when the secret is missing, instead of failing later with `None`
   - C) It encrypts the password
   - D) `getenv` cannot read environment variables

<details><summary>Answer</summary>

**B** — fail-fast at startup is the goal. A `None` password can survive several steps and produce a confusing error hours later.

</details>

4. **(Senior)** Your job retries a failing API call with `attempts=5` and no jitter, across 200 parallel workers. The API comes back after a short outage and immediately falls over again. What happened, and what do you change?

   - A) The API needs more workers
   - B) All workers retry in synchronized waves, creating a retry storm; add randomized jitter, cap the delay and the total retry budget, and consider a circuit breaker
   - C) Retries should never be used with parallel workers
   - D) The fix is to disable logging

<details><summary>Answer</summary>

**B** — without jitter, the retries of all workers align and hit the recovering service at the same instants. Full jitter spreads the load, capping limits the damage, and a circuit breaker stops calls while the service is clearly down.

</details>

5. **(Senior)** A team argues that unit tests with a mocked database are enough. What is the strongest counter-argument, and how do you structure the test suite?

   - A) Mocks are always slower than real databases
   - B) Mocks cannot verify SQL semantics such as `ON CONFLICT`, type coercion and constraints, so a mocked test can pass while the real query fails; keep many fast unit tests, plus container-based integration tests for the queries that carry the logic, and run them in CI
   - C) Mocks are not allowed in pytest
   - D) Integration tests must run in production

<details><summary>Answer</summary>

**B** — the semantics live in the database engine, which a mock does not execute. Use mocks for pure logic and boundaries you own, and real engines for the SQL that matters, with a layered test strategy.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Which errors to retry? | Transient ones: timeouts, connection resets, 429, 503. Not validation errors or 400s |
| Why jitter? | Random delay stops many workers from retrying in synchronized waves |
| What does idempotent mean for a load? | Running the same batch again leaves the same final state |
| Upsert on a business key | Insert or update by key (`ON CONFLICT ... DO UPDATE`), so retries converge |
| Why `os.environ[...]` for required secrets? | It fails at startup when the value is missing |
| What is a lockfile for? | Pins every transitive dependency so builds are reproducible |
| What does Testcontainers give you? | A real database in a throwaway Docker container for integration tests |
| Why use a `src/` layout? | Tests import the installed package, which catches packaging mistakes early |

## 🔗 Go further

- **python-concurrency** explains the threads and async choices that retries and parallel loads sit on top of.
- **python-dataframes** covers the vectorized transformations that usually sit between validation and load.
- **dedup-idempotent-loads** shows the warehouse-side patterns for idempotent writes at scale.
- **dlq-bad-records** explains how to store rejected rows so they can be fixed and replayed.
- **dataops-cicd** and **docker** cover how these tests and images run in continuous integration and delivery.
- Check the docs of `pydantic`, `uv` and `ruff` for your installed versions; flags and config keys change between releases.
