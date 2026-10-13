---
order: 31
tid: "python-concurrency"
track: "3"
section: null
num: "3.2"
title: "Python Concurrency & Internals"
type: "concept"
level: "advanced"
priority: "P1"
est_hours: 3
summary: "The GIL and its impact, threading vs multiprocessing vs asyncio, I/O-bound vs CPU-bound work, and memory management."
covers: ["concurrent.futures","asyncio for API ingestion","reference counting & GC"]
interview_qs: []
extras: []
prerequisites: ["python-core"]
related: ["python-production", "python-dataframes", "python-large-files", "api-ingestion", "backpressure", "os-jvm-essentials"]
---

## 🎯 TL;DR

> Python has **one interpreter lock (the GIL)** in CPython. In practice, threads help when your program **waits** (network, disk), processes help when your program **computes** (CPU), and `asyncio` helps when you have **thousands of waits** and want one thread to juggle them.

- **I/O-bound** work spends its time waiting for something outside the CPU. Use threads or asyncio.
- **CPU-bound** work spends its time computing in Python. Use processes (`multiprocessing`) or push the heavy part into C/Rust code (NumPy, Polars).
- **Reference counting** frees most objects immediately; a **cyclic garbage collector** handles reference cycles.
- Most pipeline bugs in this area are not about speed: they are about blocking the event loop, unbounded concurrency, and processes copying huge objects.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Think of one cashier (the CPU core running Python bytecode) in a shop.
> - **Threads** are several customers waiting for a delivery at the counter. While one waits for the courier, the cashier serves another. Great when everybody is waiting.
> - **Processes** are several cashiers, each with their own till. They can work at the same time, but each one needs its own copy of the shop's stock.
> - **asyncio** is one cashier with a clipboard: they start a task, put it aside when it waits, and pick up the next one. Only works if nobody blocks the cashier by doing a long calculation.

The fastest way to see the difference is to time it. This program "downloads" eight pages. Each download waits half a second:

```python
import time
from concurrent.futures import ThreadPoolExecutor

def fetch(page: int) -> str:
    time.sleep(0.5)  # stands in for a network call; the thread is waiting, not computing
    return f"page {page} done"

start = time.perf_counter()
with ThreadPoolExecutor(max_workers=8) as pool:
    results = list(pool.map(fetch, range(8)))

print(results[0], f"in {time.perf_counter() - start:.1f}s")
# page 0 done in 0.5s   (sequentially it would take 4.0s)
```

Eight waits overlapped, so the total is about one wait. That is the core idea of concurrency for I/O.

## 🔧 How it really works (Mid)

### The GIL

CPython (the standard interpreter) uses a **global interpreter lock**. Only one thread executes Python bytecode at a time. The lock is released:

- while a thread waits on I/O (sockets, files, `time.sleep`),
- inside many C extensions that do heavy work without touching Python objects (for example, NumPy operations on large arrays).

So threads give real parallelism for waiting, but **not** for pure Python arithmetic. Python 3.13 added an experimental "free-threaded" build without the GIL (PEP 703). Check the docs for your exact version before relying on it in production.

### The three tools

| | `threading` / thread pool | `multiprocessing` / process pool | `asyncio` |
|---|---|---|---|
| Unit of work | OS thread | OS process | coroutine (`async def`) on one event loop |
| Best for | I/O-bound, blocking libraries | CPU-bound pure Python | many concurrent network calls |
| Parallel CPU? | No (GIL) | Yes | No |
| Shared memory | Yes (dangerous without locks) | No; data is pickled and copied | Yes, single thread |
| Overhead per unit | Low (~MB stack) | High (new process, pickling) | Very low |
| Failure mode | race conditions, deadlocks | memory copies, slow startup | one blocking call stalls everything |

### CPU-bound work needs processes

```python
import math
from concurrent.futures import ProcessPoolExecutor

def count_primes(limit: int) -> int:
    def is_prime(n: int) -> bool:
        if n < 2:
            return False
        for d in range(2, math.isqrt(n) + 1):
            if n % d == 0:
                return False
        return True
    return sum(is_prime(n) for n in range(limit))

if __name__ == "__main__":  # required for process pools (spawn start method)
    with ProcessPoolExecutor() as pool:
        totals = list(pool.map(count_primes, [200_000] * 4))
    print(totals)  # [17984, 17984, 17984, 17984]
```

Run the same list with `ThreadPoolExecutor` and the wall time is roughly the same as one sequential run, because the threads take turns on the GIL.

### asyncio for many network calls

`asyncio` runs one event loop. A coroutine gives control back at each `await`. A **semaphore** limits how many calls are in flight, which is how you stay polite to an API and inside its rate limits:

```python
import asyncio

async def fetch(sem: asyncio.Semaphore, i: int) -> str:
    async with sem:                  # at most 3 requests in flight
        await asyncio.sleep(0.5)     # stands in for: await http_client.get(...)
        return f"ok {i}"

async def main() -> None:
    sem = asyncio.Semaphore(3)
    results = await asyncio.gather(*(fetch(sem, i) for i in range(9)))
    print(len(results), results[-1])  # 9 ok 8   (about 1.5 s: three waves of three)

asyncio.run(main())
```

If you must call a **blocking** function from async code (an old database driver, a file read), move it to a thread with `await asyncio.to_thread(blocking_fn, arg)` (Python 3.9+). Calling it directly freezes every other task.

### Memory: reference counting and the cyclic GC

Every CPython object has a reference count. When it drops to zero, memory is freed immediately:

```python
import sys

a = []
print(sys.getrefcount(a))  # usually 2: the name `a` plus the call argument
b = a
print(sys.getrefcount(a))  # one more
del b
```

Reference counting cannot free **cycles** (objects that refer to each other). The cyclic garbage collector finds them:

```python
import gc

class Node:
    def __init__(self) -> None:
        self.other: "Node | None" = None

x, y = Node(), Node()
x.other, y.other = y, x   # a cycle
del x, y                  # counts never reach zero...
print(gc.collect())       # ...but the collector finds and frees them (> 0)
```

Two practical consequences: `del` does not guarantee memory is returned to the operating system (CPython's allocator may keep it for reuse), and a process that holds a huge list of small objects can grow large even after you drop most of them.

Process pools on Linux use `fork` by default in older Python versions, so children share memory copy-on-write. Reference counting **writes** to every object it touches, which copies those memory pages anyway. A large read-only Python structure passed to workers can therefore be duplicated per process. Prefer passing NumPy arrays, Arrow tables or file paths.

## 🧠 Senior depth & trade-offs

- **Where threads still win.** Many real data clients (`requests`, database drivers written in C, `boto3`) release the GIL while waiting. A thread pool with 16–64 workers is often the simplest correct choice for ingestion. Choose it when libraries are synchronous.
- **asyncio's failure mode is blocking, not speed.** One synchronous `requests.get` or a CPU loop inside a coroutine stops every other request. Watch for event-loop lag in your metrics; keep CPU work out of the loop.
- **Backpressure is a concurrency decision.** An unbounded `asyncio.gather` over a million URLs creates a million tasks and memory spikes. Use a semaphore, a bounded queue (`asyncio.Queue(maxsize=...)`) or chunks. See [[backpressure]].
- **Processes are a memory and serialization decision.** Pickling a 2 GB DataFrame to send it to a worker is often slower than the computation. Pass a path or a partition key and let each worker read its own slice, or use a columnar engine that parallelizes internally.
- **Engines already parallelize for you.** Spark, Polars, DuckDB and Dask distribute and multi-thread work in native code. If your Python loop is the bottleneck, first ask whether a vectorized or SQL expression removes it.
- **Retries multiply concurrency.** Eight workers that each retry three times with no jitter hammer a struggling service in waves. Combine concurrency limits with backoff ([[retry]]).
- **What to monitor.** Thread/worker pool queue depth, task latency percentiles, event-loop lag, process RSS per worker and the number of open connections. A saturated pool with a short queue is a sign of undersized concurrency; a growing queue with idle CPUs is a sign of blocking calls.
- **When not to use concurrency at all.** If a job runs in 4 minutes once a day, a simpler sequential job with better retries is more reliable than a clever async design.

## 🧪 Try it

1. Change the `fetch` example to use `ProcessPoolExecutor` instead of threads. Predict the total time before running it. Then explain the result (hint: process startup and the GIL are both irrelevant here, since `sleep` does not use the CPU).
2. Add a `for` loop with a million additions inside `fetch` and rerun both pools. Which one now benefits from more workers, and why?
3. Modify the asyncio example so that one of the coroutines calls `time.sleep(2)` instead of `await asyncio.sleep(2)`. Predict how the other coroutines behave, then check.

<details><summary>Show solutions</summary>

**1.** With `ProcessPoolExecutor(max_workers=8)` the total is still about 0.5 s plus process startup (often 0.1–0.5 s more). Processes work, but they cost more to start and to pass data through, and `sleep` does not need a CPU, so there is no benefit.

**2.** With a pure Python loop, the thread pool gets no speed-up (the threads share one GIL), while the process pool scales with the number of CPU cores. This is the CPU-bound vs I/O-bound split in one experiment.

**3.** The blocking `time.sleep(2)` freezes the whole event loop: no other coroutine runs until it returns, so the total run gets longer and the other tasks finish late. Use `await asyncio.sleep(2)` or `await asyncio.to_thread(time.sleep, 2)`.

</details>

## 💬 Interview corner

**Question: "What is the GIL, and does it make Python threads useless?"**
Mid answer: "The GIL lets only one thread run Python bytecode at a time. Threads are still useful for I/O because the lock is released while waiting."
Senior answer: "The GIL serializes Python bytecode in CPython, so threads give concurrency for I/O but not parallel CPU work. For CPU-bound work I use processes or native-code libraries. I also keep in mind that processes copy data, so I pass partitions or file paths, not huge objects. I check the docs for the Python version, since the free-threaded build changes this for newer releases."

**Question: "When would you choose asyncio over a thread pool for ingestion?"**
Mid: "When there are many concurrent network calls."
Senior: "When the client library is async-native and I need thousands of concurrent connections with low memory. I bound concurrency with a semaphore, never call blocking code inside the loop, and monitor loop lag. If the client is synchronous, a bounded thread pool is simpler and just as fast."

> **🎤 Say this in the interview:** "Threads for waiting, processes for computing, asyncio for many waits with one thread. The GIL is the reason, and the first failure I watch for is blocking code where it does not belong."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Calling a blocking function (`requests.get`, a synchronous DB call, `time.sleep`) inside `async def` blocks the whole event loop. Use an async client, or `asyncio.to_thread`.

> **⚠️ Watch out:** A process pool without `if __name__ == "__main__":` can re-run the module in every child (on platforms that use `spawn`, such as Windows and macOS) and fail.

- Expecting threads to speed up pure Python arithmetic.
- Creating an unbounded number of tasks or threads, then running out of memory or file descriptors.
- Sharing mutable state between threads without a `Lock`, then debugging a rare race condition.
- Passing very large DataFrames to process workers instead of paths or partitions.
- Assuming `del big_list` immediately lowers the process's memory usage.
- Catching all exceptions in a worker and silently losing failed tasks. Collect and re-raise, or count them.

## ✅ Check yourself (quiz)

1. A job downloads 10,000 small JSON files over HTTP. Which approach is the most appropriate?
   - A) A process pool with one process per file
   - B) A bounded thread pool or asyncio with a semaphore
   - C) A single thread with a longer timeout
   - D) Pure Python regex on the response bodies in separate threads

<details><summary>Answer</summary>

**B** — the work is I/O-bound, so threads or asyncio overlap the waits. Bounding concurrency keeps the job polite and the memory stable. A process per file (A) is expensive and unnecessary.

</details>

2. Why does a `ThreadPoolExecutor` with 8 workers usually not speed up a pure-Python prime search?
   - A) Threads cannot call functions
   - B) The GIL lets only one thread run Python bytecode at a time
   - C) The OS limits threads to 1 per core
   - D) Prime search needs `await`

<details><summary>Answer</summary>

**B** — CPU-bound Python code takes turns on the GIL, so there is no parallel speed-up. Use a `ProcessPoolExecutor` or a native library.

</details>

3. What happens if a coroutine calls `time.sleep(5)` while nine other coroutines are waiting on `await asyncio.sleep(1)`?
   - A) Nothing; `time.sleep` is cooperative
   - B) The other coroutines are blocked for about 5 seconds because the event loop is frozen
   - C) The sleeping coroutine is moved to another thread automatically
   - D) asyncio raises a `TimeoutError`

<details><summary>Answer</summary>

**B** — `time.sleep` is a blocking call. The single event-loop thread waits inside it, so no other task can run. Use `await asyncio.sleep(5)`.

</details>

4. **(Senior)** You run `asyncio.gather` over 2 million URLs and the container is OOM-killed after a few minutes. What is the most likely cause, and how do you fix it?

   - A) `gather` cannot handle more than 1000 tasks; use `threading`
   - B) All tasks and their pending results are created at once, so memory grows without bound; bound in-flight work with a semaphore or process the URLs in chunks/with a bounded queue
   - C) asyncio leaks memory on every `await`; switch to `multiprocessing`
   - D) The fix is more CPUs

<details><summary>Answer</summary>

**B** — `gather` schedules every coroutine immediately, so the task objects and their results live in memory together. Limit concurrency with a semaphore or feed a bounded queue, and write results out as you go.

</details>

5. **(Senior)** A worker pool reads a 3 GB read-only dictionary (a Python `dict` of millions of strings) and forks eight processes. Memory use grows to eight copies even though the children only read it. Why, and what would you do?

   - A) `fork` always copies memory, so the fix is threads
   - B) Reference counting writes to object headers as they are read, which touches (copies) the pages; use a compact, non-refcounted structure such as a NumPy array, an Arrow table, or memory-map a file and let workers read slices
   - C) The dictionary must be pickled, so use `json`
   - D) This cannot happen on Linux

<details><summary>Answer</summary>

**B** — copy-on-write sharing breaks down with CPython objects because each read increments reference counts, dirtying the pages. Storing the data in a flat, read-only format avoids the per-object writes.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What is the GIL? | A CPython lock that allows only one thread to run Python bytecode at a time |
| Is the GIL released during I/O? | Yes, while a thread waits on sockets, files or `sleep` |
| Best tool for CPU-bound pure Python | `multiprocessing` / `ProcessPoolExecutor` |
| Best tool for thousands of concurrent network waits | `asyncio` with a bounded semaphore |
| What blocks an event loop? | Any synchronous call that waits or computes (use `asyncio.to_thread`) |
| Why can reference counting hurt fork sharing? | Reading objects updates refcounts, which copies the memory pages |
| What frees reference cycles? | The cyclic garbage collector (`gc`) |
| Why `if __name__ == "__main__":` for process pools? | Child processes may re-import the module (spawn start method) |

## 🔗 Go further

- **python-core** covers the object model, which is the foundation of reference counting.
- **python-large-files** shows how to read data in chunks, which replaces many "load it all, then parallelize" designs.
- **api-ingestion** applies the semaphore and retry patterns to real API pipelines.
- **backpressure** explains why bounded queues matter more than raw worker count.
- Read the `concurrent.futures` and `asyncio` sections of the standard library docs for your Python version; the defaults for process start methods have changed across releases.
