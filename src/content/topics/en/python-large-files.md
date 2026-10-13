---
order: 32
tid: "python-large-files"
track: "3"
section: null
num: "3.3"
title: "Processing Large Files in Python"
type: "practice"
level: "intermediate"
priority: "P0"
est_hours: 3
summary: "Processing files larger than RAM: line-by-line streaming, chunking, generators, external sort and hand-rolled map-reduce. A common DE coding question."
covers: []
interview_qs: ["Find top-10 URLs in a 50 GB log with 8 GB RAM","Merge K sorted files","Deduplicate a stream with bounded memory"]
extras: []
prerequisites: ["python-core"]
related: ["pyspark-api", "spark-memory-tuning", "mapreduce", "small-files", "dedup-idempotent-loads", "pattern-dedup"]
---
## 🎯 TL;DR

> To process a file larger than RAM, **never hold the whole file**. Stream it line by line, keep only bounded state (a counter for the top K, a window of recent keys), and when the state itself is too big, **split the data by hash** into buckets that fit, then combine the results.

- Reading with `read()` or `readlines()` loads everything. Iterating the file object loads one line at a time.
- Streaming gives O(1) memory in the input size; the aggregate state is what decides memory.
- External sort = sort chunks that fit in RAM, write sorted runs to disk, then **k-way merge** them with `heapq.merge`.
- Exact deduplication with bounded memory is impossible in general; you either partition the data or accept an approximate, windowed answer.

## 🌱 Start simple (Junior)

> **💡 Analogy:** You have a 1,000-page book and a desk that fits 10 pages. You do not photocopy the whole book onto the desk. You read page by page, keep a short note of what you count, and throw each page away when you are done.

The bug that causes most out-of-memory crashes:

```python
with open("access.log", "w") as f:            # sample file for the demo
    f.write("GET /a 200\nGET /b 500\nGET /c 500\n")

# Loads the whole file into memory: fails for a 50 GB file
with open("access.log") as f:
    lines = f.readlines()
```

The streaming version reads one line at a time:

```python
# Memory stays flat: one line at a time
n_errors = 0
with open("access.log", encoding="utf-8", errors="replace") as f:
    for line in f:
        if " 500 " in line:
            n_errors += 1
print(n_errors)
```

The file object is an iterator that reads from a buffer. Python never builds a list unless you ask for one, so the second version works for any file size, as long as your **state** stays small.

## 🔧 How it really works (Mid)

### Streaming records and aggregating

For structured files, use the `csv` module as a generator, and keep only the running totals:

```python
import csv

def read_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        yield from csv.DictReader(f)

def total_by_region(path):
    totals = {}
    for row in read_rows(path):
        totals[row["region"]] = totals.get(row["region"], 0) + int(row["amount"])
    return totals
```

The state is `totals`, which grows with the number of **distinct regions**, not with the number of rows. That is the key question for any streaming aggregation: how big is the state?

### Top-K with bounded state

For `Counter` over a 50 GB log, the state is one entry per distinct URL. If there are hundreds of millions of distinct URLs, that state does not fit. The classic answer is **partition, then aggregate**:

1. Hash each URL to one of `N` bucket files. Every occurrence of the same URL goes to the same bucket.
2. Count each bucket on its own. Its state is only the distinct URLs in that bucket.
3. Keep a min-heap of size `K` across buckets.

```python
import heapq, os, tempfile, zlib
from collections import Counter

def top_k_urls(path, k=10, n_buckets=64):
    tmp = tempfile.mkdtemp()
    paths = [os.path.join(tmp, f"b{i}.txt") for i in range(n_buckets)]
    outs = [open(p, "w", encoding="utf-8") for p in paths]
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                url = line.rstrip("\n")
                if url:
                    # crc32 is stable across processes; hash() is randomised per run
                    outs[zlib.crc32(url.encode()) % n_buckets].write(url + "\n")
    finally:
        for o in outs:
            o.close()

    best = []                                 # min-heap of (count, url), size <= k
    for p in paths:
        counts = Counter()
        with open(p, encoding="utf-8") as f:
            for line in f:
                counts[line.rstrip("\n")] += 1
        for url, c in counts.items():         # each URL lives in exactly one bucket
            if len(best) < k:
                heapq.heappush(best, (c, url))
            elif c > best[0][0]:
                heapq.heapreplace(best, (c, url))
        os.remove(p)
    os.rmdir(tmp)
    return sorted(best, reverse=True)

if __name__ == "__main__":
    import random
    rnd = random.Random(1)
    src = os.path.join(tempfile.mkdtemp(), "log.txt")
    with open(src, "w", encoding="utf-8") as f:
        for _ in range(20_000):
            f.write(f"https://ex.com/p{rnd.randint(1, 500)}\n")
    print(top_k_urls(src, k=3))
```

The correctness argument matters: because each URL is in exactly one bucket, its count in that bucket **is** its total count. Without that property, you would have to add counts across buckets.

### External sort and k-way merge

To sort a file bigger than RAM: sort chunks that fit, write each sorted chunk ("run") to disk, then merge the runs. `heapq.merge` reads lazily from all runs, holding only one line per run in memory:

```python
import heapq, os, tempfile
from itertools import islice

def merge_sorted_files(paths, out_path):
    files = [open(p, encoding="utf-8") for p in paths]
    try:
        with open(out_path, "w", encoding="utf-8") as out:
            out.writelines(heapq.merge(*files))   # lazy k-way merge
    finally:
        for f in files:
            f.close()

def external_sort(in_path, out_path, chunk_lines=100_000):
    tmp = tempfile.mkdtemp()
    runs = []
    with open(in_path, encoding="utf-8") as f:
        while True:
            chunk = [l if l.endswith("\n") else l + "\n" for l in islice(f, chunk_lines)]
            if not chunk:
                break
            chunk.sort()
            run = os.path.join(tmp, f"run{len(runs)}.txt")
            with open(run, "w", encoding="utf-8") as out:
                out.writelines(chunk)
            runs.append(run)
    merge_sorted_files(runs, out_path)
    for r in runs:
        os.remove(r)
    os.rmdir(tmp)

if __name__ == "__main__":
    import random
    rnd = random.Random(2)
    d = tempfile.mkdtemp()
    src, dst = os.path.join(d, "in.txt"), os.path.join(d, "out.txt")
    with open(src, "w", encoding="utf-8") as f:
        for _ in range(50):
            f.write(f"{rnd.randint(0, 999):03d}\n")       # fixed width: text sort = numeric sort
    external_sort(src, dst, chunk_lines=10)
    with open(dst, encoding="utf-8") as f:
        lines = f.read().split()
    print(lines == sorted(lines), len(lines))            # True 50
```

The zero-padded numbers are deliberate. Lines are compared as text, so `"10" < "9"`. Pad numbers to a fixed width, or pass a `key` that parses them.

### Deduplicating a stream

Exact deduplication needs memory for every distinct key seen so far. You have three options:

- **Partition** (as in top-K): hash keys into buckets and dedupe each bucket with a set. Exact, but needs disk and a second pass.
- **Windowed**: remember only the last `W` keys. Exact within the window, approximate beyond it.
- **Probabilistic**: a Bloom filter answers "definitely new" or "probably seen". It can drop a unique record (a false positive), so use it only when that is acceptable. See [[bloom-filter]].

```python
from collections import deque

def dedup_window(stream, window=1_000):
    seen = set()
    order = deque()
    for item in stream:
        if item in seen:
            continue
        seen.add(item)
        order.append(item)
        if len(order) > window:
            seen.discard(order.popleft())
        yield item

print(list(dedup_window([1, 2, 1, 3, 2], window=2)))   # [1, 2, 3]
```

Here `2` is correctly dropped because it is still in the window. If a key falls out of the window and comes back, it is emitted again. Name this trade-off explicitly in the design.

### Chunking with pandas

When the data is columnar-friendly, pandas can process chunks, each a `DataFrame`:

```python
import io
import pandas as pd

sample = io.StringIO("amount\n1\n2\n3\n")     # stands in for a huge file on disk
total = 0
for chunk in pd.read_csv(sample, chunksize=2):   # each chunk is a DataFrame
    total += chunk["amount"].sum()
print(total)                                     # 6
```

On a real file, use a chunk size that fits your memory budget (for example 500,000 rows). Each chunk is a normal DataFrame, so you can use vectorised operations, but the state across chunks is still yours to manage.

## 🧠 Senior depth & trade-offs

- **Find the state size first.** Before writing code, estimate `distinct keys × bytes per entry`. A Python dict entry costs far more than the string alone. Measure with a sample and `tracemalloc`, then pick the bucket count so each bucket fits with headroom.
- **Pick the bucket count from the budget, not from habit.** With 100 million distinct URLs at roughly 150 bytes each (an estimate; measure it), the state is about 15 GB. With 64 buckets, each bucket is about 230 MB. Check this against the memory you can actually give the process.
- **Skew is per key, not per bucket.** One very hot URL cannot be split by hashing, but it is only one entry in its bucket. The problem appears when a single key is so frequent that a per-key approach breaks. For that, the distributed analogue is [[hot-key]] handling and salting.
- **Beyond one machine, move to the engine.** Once the data needs more than one disk or more than one hour, the same ideas (partitioning, external sort, merge) are what Spark and Hadoop do for you. The Python version is the right tool for an interview, a one-off job, or a single-machine stage.
- **I/O dominates.** A streaming loop is usually bound by disk or network throughput. Use large reads, avoid per-line expensive work, and compress with `gzip` or `zstd` streams to cut I/O. Measure before optimising Python code.
- **Failure handling.** Temporary files must be cleaned up on errors (use `try/finally` or `tempfile.TemporaryDirectory`). For a restartable job, write runs atomically (write to `.tmp`, then rename) so a crash never leaves a half-written run that looks valid.
- **Encoding.** Real logs contain invalid UTF-8. Choose `errors="replace"` or `"surrogateescape"` deliberately, and count how many lines were affected; silent corruption is worse than a crash.

## 🧪 Try it

1. **Sum without loading.** Write `sum_column(path, name)` that sums an integer CSV column with a streaming reader. Test it on a 100,000-row file you generate in a temp directory.
2. **Verify top-K.** Generate 20,000 random URLs. Check that `top_k_urls(path, 3)` returns the same counts as `collections.Counter(urls).most_common(3)`.
3. **Merge three sorted files.** Given three files, each with sorted zero-padded numbers, produce one sorted output using `heapq.merge` without reading any file fully into memory.

<details><summary>Show solutions</summary>

**1.**
```python
import csv, os, random, tempfile

def sum_column(path, name):
    total = 0
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total += int(row[name])
    return total

d = tempfile.mkdtemp()
p = os.path.join(d, "x.csv")
rnd = random.Random(0)
with open(p, "w", newline="", encoding="utf-8") as f:
    f.write("id,amount\n")
    expected = 0
    for i in range(100_000):
        v = rnd.randint(0, 100)
        expected += v
        f.write(f"{i},{v}\n")
print(sum_column(p, "amount") == expected)   # True
```

**2.** Reuse `top_k_urls` from above on a file of 20,000 generated URLs. Compare `[c for c, _ in got]` with `[c for _, c in Counter(urls).most_common(3)]`. Compare counts, not URLs: ties at the cut-off can legitimately pick different URLs.

**3.**
```python
import heapq, os, tempfile

d = tempfile.mkdtemp()
runs = []
for i, nums in enumerate([[1, 4, 9], [2, 3, 10], [0, 5, 6]]):
    p = os.path.join(d, f"r{i}.txt")
    with open(p, "w", encoding="utf-8") as f:
        f.writelines(f"{n:03d}\n" for n in sorted(nums))
    runs.append(p)

files = [open(p, encoding="utf-8") for p in runs]
out = os.path.join(d, "merged.txt")
with open(out, "w", encoding="utf-8") as w:
    w.writelines(heapq.merge(*files))
for f in files:
    f.close()
print(open(out, encoding="utf-8").read().split())
# ['000', '001', '002', '003', '004', '005', '006', '009', '010']
```

</details>

## 💬 Interview corner

**"Find the top 10 URLs in a 50 GB log on a machine with 8 GB RAM."** Mid: "I'd stream the file and use a `Counter`, then `most_common(10)`." Senior: "A `Counter` holds one entry per distinct URL, which may not fit. I'd hash-partition the URLs into buckets so each bucket fits in memory, count each bucket, keep a size-10 min-heap across buckets, and clean up temp files. I'd state the assumption about distinct-key count and measure the state size on a sample first."

**"Merge K sorted files."** Mid: "Use a min-heap with one head per file." Senior: "That is `heapq.merge`: O(N log K) time and O(K) memory, lazily. I'd use it as the merge step of an external sort, and I'd make sure the sort key is typed correctly, not compared as text."

**"Deduplicate a stream with bounded memory."** Senior: "Exact dedup needs state proportional to distinct keys. With a fixed budget I choose between a windowed set (exact within the window), a Bloom filter (no false negatives but false positives drop records), or partitioned dedup on disk. I'd say which trade-off I accept and why."

> **🎤 Say this in the interview:** "I start from the state, not the input. If the state fits, streaming is enough. If it does not, I partition by key so each part fits, then merge. I say out loud what I trade away, such as exactness or disk I/O."

## ⚠️ Common mistakes

> **⚠️ Watch out:** `f.read()`, `f.readlines()` and `list(f)` all materialise the whole file. Iterate the file object instead.

> **⚠️ Watch out:** Using Python's built-in `hash()` to choose buckets. It is randomised per process (`PYTHONHASHSEED`), so a restarted job partitions differently. Use a stable hash such as `zlib.crc32` or `hashlib`.

- Sorting text lines containing numbers without padding: `"10"` sorts before `"9"`.
- Forgetting that a generator can be consumed only once.
- Leaving temporary run files on disk after an exception.
- Writing a partially written run that looks complete after a crash.
- Assuming `set` membership is free in memory: a set of 100 million strings costs gigabytes.
- Ignoring encoding errors silently in a large log.

## ✅ Check yourself (quiz)

1. Which read pattern keeps memory independent of file size?
   - A) `data = f.read()`
   - B) `lines = f.readlines()`
   - C) `for line in f:`
   - D) `list(f)`

<details><summary>Answer</summary>

**C** — iterating the file object reads through an internal buffer, one line at a time. The other three build a structure proportional to the file.

</details>

2. Why does `top_k_urls` hash each URL to a bucket before counting?
   - A) To sort the URLs faster
   - B) So each URL's occurrences are in one bucket, making each bucket's counts complete while each bucket's state fits in RAM
   - C) To encrypt the URLs
   - D) Because `Counter` cannot count strings

<details><summary>Answer</summary>

**B** — partitioning by a stable hash keeps each key in exactly one bucket. Each bucket's state is small, and no per-key count is split across buckets.

</details>

3. You use `hash(url) % 64` for buckets. What goes wrong when the job is restarted on the same data?
   - A) Nothing; `hash` is stable
   - B) Python randomises string hashes per process, so the same URL can land in a different bucket on the next run, which breaks a resumable job's assumptions
   - C) The bucket count must be a power of two
   - D) `Counter` rejects `%` results

<details><summary>Answer</summary>

**B** — `hash()` for `str` is salted per process by default. Use `zlib.crc32` or `hashlib` for a partition function that stays the same across runs.

</details>

4. **(Senior)** Your dedup stream keeps the last 1 million IDs in a window. A producer retries an event 2 million events later. What happens and what do you tell the business?
   - A) Nothing; the window catches all duplicates
   - B) The duplicate passes through, because it has left the window; state the window as an explicit guarantee and, if needed, use a persistent key store or partitioned exact dedup
   - C) The generator raises an error
   - D) The set grows without bound

<details><summary>Answer</summary>

**B** — a windowed set is exact only within the window. Name the window size as part of the contract, and use a durable store or partitioned dedup if late duplicates must be caught.

</details>

5. **(Senior)** A merge of 500 sorted run files is slow and memory-hungry. What is the cheapest improvement?
   - A) Load all runs into one list and call `sorted`
   - B) Merge in multiple passes (for example 20 runs at a time) so each pass has few open files and small heads, instead of holding 500 open files at once
   - C) Replace `heapq.merge` with `min()` on a list of heads
   - D) Increase the runs' size until there is one run

<details><summary>Answer</summary>

**B** — a multi-pass merge limits open file handles and I/O contention. It is the same idea as multi-level merge in external sorts. Option C is O(K) per line and option D is the original problem.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| `f.read()` vs `for line in f:` | `read()` loads everything; the loop keeps one line in memory |
| What decides streaming memory? | The size of the aggregate state, not the size of the input |
| External sort | Sort in-memory chunks into runs, then k-way merge the runs |
| `heapq.merge` cost | O(N log K) time, O(K) memory, lazy |
| Why partition before counting | Each key lands in one bucket, so per-bucket counts are complete |
| Stable partition hash | `zlib.crc32` or `hashlib`, not `hash()` |
| Windowed dedup trade-off | Exact inside the window, duplicates may pass after it |
| Bloom filter risk | No false negatives, but false positives drop unique records |

## 🔗 Go further

- **python-core** covers the generators, `Counter` and `heapq` this lesson depends on.
- **pattern-dedup** and **dedup-idempotent-loads** cover deduplication at pipeline scale.
- **pyspark-api** and **spark-memory-tuning** show how the same partition, sort and merge ideas run on a cluster.
- **mapreduce** explains the map, shuffle and merge model that this single-machine approach imitates.
- **small-files** matters when external sort produces many tiny run files; compact them before the next step.
