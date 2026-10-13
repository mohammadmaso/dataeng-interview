---
order: 10
tid: "dsa-for-de"
track: "1"
section: null
num: "1.4"
title: "Data Structures & Algorithms for DE"
type: "concept"
level: "intermediate"
priority: "P1"
est_hours: 10
summary: "Algorithm level in DE interviews is usually LeetCode Easy–Medium. Focus on hash maps, heaps, sorting, two pointers, sliding window, intervals, graphs and topological sort (the basis of DAGs)."
covers: ["Big-O","arrays/strings","hashing","heaps & top-K","sorting & external merge sort","intervals","BFS/DFS","topological sort","union-find (basic)","LRU cache"]
interview_qs: ["Top-K frequent items","Merge overlapping intervals","Detect cycle in a DAG of jobs","Implement an LRU cache"]
extras: []
prerequisites: []
related: ["sql-window-functions", "python-core", "kafka-architecture", "airflow"]
---

## 🎯 TL;DR

> Data engineering interviews do not test algorithm theory for its own sake. They test whether you can pick the right **data structure** for a streaming or batch problem, and whether you can say how fast it runs on a dataset that does not fit in memory.

- **Big-O** tells you how the cost grows with input size `n`. Count the dominant loop, and the cost of each lookup.
- **Hash maps** give average O(1) lookups. They are the default tool for counting, deduplication and joins in memory.
- **Heaps** keep the top K of a stream in O(n log k) time and O(k) memory.
- **Sorting** unlocks two pointers, interval merging and dedup. For data larger than memory, the same idea becomes external merge sort.
- **Graphs** model pipelines. Topological sort orders jobs and detects cycles, which is exactly what an orchestrator does with a DAG.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A library. A **hash map** is the card catalogue: you go straight to the shelf for a book title. A **list** is a pile of books: to find one you may check every book. A **heap** is a tournament bracket: you always know the best player without sorting everyone.

Big-O in one table (n = number of items):

| Operation | Hash map (dict) | List / array | Sorted list |
|---|---|---|---|
| lookup by key | O(1) average | O(n) scan | O(log n) binary search |
| insert at end | O(1) | O(1) amortised | O(n) to keep order |
| scan all | O(n) | O(n) | O(n) |

Counting how many times each value appears is the first problem to solve with a hash map:

```python
from collections import Counter

events = ["click", "view", "click", "buy", "click", "view"]
counts = Counter(events)
print(counts)                  # Counter({'click': 3, 'view': 2, 'buy': 1})
print(counts["buy"])           # 1
print(counts["refund"])        # 0  (missing keys count as 0 in Counter)
```

Without a hash map the naive version is a nested loop: for each event, count the matches in the whole list. That is O(n²). With a hash map, one pass is O(n).

```python
# O(n^2): slow for a million events
def count_naive(events):
    return {e: events.count(e) for e in set(events)}

# O(n): one pass
def count_fast(events):
    counts = {}
    for e in events:
        counts[e] = counts.get(e, 0) + 1
    return counts
```

## 🔧 How it really works (Mid)

### Top-K with a heap

"Top 10 most frequent items" is a classic. Sorting all counts costs O(d log d) for d distinct items. A min-heap of size K costs O(d log K): keep the K largest seen so far, and drop the smallest when a bigger one arrives.

Python's `heapq.nlargest` does this internally:

```python
import heapq
from collections import Counter

def top_k_frequent(items, k):
    counts = Counter(items)                          # O(n) to count
    return heapq.nlargest(k, counts, key=counts.get) # O(d log k)

print(top_k_frequent(["a", "b", "a", "c", "a", "b"], 2))   # ['a', 'b']
```

When data is a stream and you cannot hold all counts, use approximate structures such as a Count-Min sketch or a space-saving algorithm; a [[hot-key]] problem in Kafka partitions is a real-world case where this matters.

### Intervals and sorting

Many DE problems are intervals: sessions, time windows, active periods of a job. The trick is almost always: **sort by start, then sweep**.

```python
def merge_intervals(intervals):
    merged = []
    for start, end in sorted(intervals):             # O(n log n)
        if merged and start <= merged[-1][1]:        # overlaps the last interval
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged

print(merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]]))
# [[1, 6], [8, 10], [15, 18]]
```

Touching intervals such as `[1, 3]` and `[3, 5]` merge with `<=`. If your business rule says touching is not overlap, use `<`.

### Two pointers and sliding window

Two pointers work on sorted input or contiguous ranges and turn O(n²) into O(n).

```python
def max_sum_window(values, k):
    """Largest sum of k consecutive values, O(n)."""
    window = sum(values[:k])
    best = window
    for right in range(k, len(values)):
        window += values[right] - values[right - k]  # slide: add new, drop old
        best = max(best, window)
    return best

print(max_sum_window([2, 1, 5, 1, 3, 2], 3))   # 9  (5 + 1 + 3)
```

A variable-size window works the same way: expand `right`, shrink `left` while the window breaks a rule. Example: longest substring without repeated characters, O(n).

### Graphs and topological sort

A pipeline is a directed acyclic graph (DAG): an edge `a -> b` means "a must finish before b". A **topological order** is a valid execution order. **Kahn's algorithm** builds it with in-degree counts; if some nodes never reach in-degree zero, there is a **cycle**.

```python
from collections import defaultdict, deque

def topo_sort(n_jobs, edges):
    """edges: (a, b) means job a must finish before job b. Jobs are 0..n_jobs-1."""
    graph = defaultdict(list)
    indeg = [0] * n_jobs
    for a, b in edges:
        graph[a].append(b)
        indeg[b] += 1

    ready = deque(i for i in range(n_jobs) if indeg[i] == 0)
    order = []
    while ready:
        u = ready.popleft()
        order.append(u)
        for v in graph[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                ready.append(v)

    if len(order) != n_jobs:
        raise ValueError("cycle detected: the job graph is not a DAG")
    return order

# extract -> clean -> load, and extract -> validate -> load
print(topo_sort(4, [(0, 1), (1, 3), (0, 2), (2, 3)]))   # [0, 1, 2, 3] or [0, 2, 1, 3]
```

Both orders are valid; interviewers accept either as long as every edge is respected. The same algorithm answers "which tasks can run in parallel": all nodes in one wave of `ready` are independent.

### LRU cache

An LRU (least recently used) cache evicts the entry that has not been touched the longest. The clean Python solution uses `OrderedDict`, which keeps insertion order and supports O(1) move-to-end.

```python
from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.data = OrderedDict()

    def get(self, key):
        if key not in self.data:
            return -1
        self.data.move_to_end(key)          # mark as most recently used
        return self.data[key]

    def put(self, key, value):
        if key in self.data:
            self.data.move_to_end(key)
        self.data[key] = value
        if len(self.data) > self.capacity:
            self.data.popitem(last=False)   # evict least recently used

cache = LRUCache(2)
cache.put("a", 1); cache.put("b", 2)
cache.get("a")          # a is now most recent
cache.put("c", 3)       # evicts b
print(cache.get("b"))   # -1
```

### External merge sort and k-way merge

When data is larger than memory, sort in chunks, write each sorted chunk to disk, and merge the chunks with a heap of size k (one head per chunk). This is what `sort` in Linux does, and it is what database engines do for `ORDER BY` on large tables.

```python
import heapq

def merge_sorted_streams(*streams):
    """Merge already sorted iterables in O(total * log k) time and O(k) memory."""
    yield from heapq.merge(*streams)

chunk_a = iter([1, 4, 9])     # imagine: each is a sorted file read line by line
chunk_b = iter([2, 3, 10])
chunk_c = iter([5, 6])
print(list(merge_sorted_streams(chunk_a, chunk_b, chunk_c)))   # [1, 2, 3, 4, 5, 6, 9, 10]
```

## 🧠 Senior depth & trade-offs

- **Big-O is not the whole cost.** A hash map lookup is O(1), but a cache miss in a 100 GB hash table on disk costs far more than a comparison in a small sorted array. For data engineering, I/O and shuffle usually dominate. Think in terms of passes over the data: one pass is cheap, a sort is a pass plus disk spill, a join may be a shuffle over the network. See [[shuffle]].
- **Memory budget first.** Ask how much fits in memory before picking the structure. A dict of 500 million keys does not fit on a 16 GB worker; the answer becomes partitioning by key (so each partition fits), a sketch (HyperLogLog for distinct counts, Count-Min for frequencies), or pushing the work to the engine.
- **Skew breaks the average.** Hash partitioning spreads keys evenly only if the keys are spread. One hot key puts all its work on one partition, and "O(1) average" is useless when one worker runs everything. See [[data-skew]] and [[salting]].
- **Topological sort is what an orchestrator does.** Airflow resolves task dependencies as a DAG and refuses to schedule a cycle. When you design a pipeline, a cycle means a dependency you have not noticed; detect it at parse time, not at 3 a.m.
- **Streaming changes the question.** For unbounded input, you cannot sort everything. You need windowed algorithms, bounded state and an eviction policy (the LRU idea), and you must say what happens to late data. See [[watermark]].
- **When not to write it yourself.** Use `heapq`, `collections`, `sort` or the engine's built-in top-N and distinct-count functions. Hand-written data structures are a liability in production. In an interview, state the library choice and then show you understand the algorithm behind it.
- **What to say about complexity.** Give time and space, the assumption (average-case hashing, comparison sort), and the bottleneck at scale. "O(n log k) time, O(k) memory, and this works for a stream" is a complete answer.

## 🧪 Try it

1. Given a list of `(user_id, start, end)` sessions, return the number of users who had at least one session overlapping `[t0, t1]`. Use a set and one pass.
2. Given a list of `n` tasks and a list of dependency pairs, return `True` if all tasks can finish (no cycle), otherwise `False`. Use the `topo_sort` from this lesson.
3. Find the 3 most frequent words in a list, and in case of ties, prefer the word that appears first in the input. Explain the time and space cost.

<details><summary>Show solutions</summary>

**1.**
```python
def users_in_window(sessions, t0, t1):
    users = set()
    for user_id, start, end in sessions:
        if start <= t1 and end >= t0:      # interval overlap test
            users.add(user_id)
    return len(users)

print(users_in_window([(1, 0, 5), (2, 10, 12), (1, 20, 25)], 4, 11))   # 2
```
O(s) time for `s` sessions, O(u) memory for `u` users. The overlap test is `start <= t1 and end >= t0`.

**2.**
```python
def can_finish(n_tasks, deps):
    try:
        topo_sort(n_tasks, deps)
        return True
    except ValueError:
        return False
```
Kahn's algorithm is O(V + E). The cycle check is `len(order) != n_jobs`.

**3.** Use a first-seen index so ties are broken by the earliest position:
```python
from collections import Counter

def top3_words(words):
    counts = Counter(words)                      # Counter keeps insertion order for ties in CPython 3.7+
    return sorted(counts, key=lambda w: -counts[w])[:3]
```
Time O(n + d log d) with a full sort, or O(n + d log 3) with `heapq.nlargest(3, ...)`. Memory O(d). Note that `sorted` is stable, so ties keep first-seen order. Mention that the stability guarantee is what makes the tie rule work.

</details>

## 💬 Interview corner

**Question: "Top-K frequent items."** Mid answer: "Count with a hash map in O(n), then use a min-heap of size K for O(d log K)." Senior answer: "The exact version is O(n + d log K) time and O(d) memory. If the stream does not fit in memory, I would use a space-saving or Count-Min sketch and accept an approximate answer with a stated error bound. If one key dominates, I would check for skew before trusting the average."

**Question: "Merge overlapping intervals."** Mid: "Sort by start, then sweep, extending the last interval when the next one starts before it ends." Senior: "O(n log n) because of the sort. I would clarify whether touching intervals merge, and I would say the same sort-and-sweep is how a streaming system groups events into sessions with a gap timeout."

**Question: "Detect a cycle in a DAG of jobs."** Mid: "Kahn's algorithm: if the topological order has fewer nodes than the graph, there is a cycle." Senior: "Also O(V + E). I would report the nodes that were never processed, since they include the cycle, and I would enforce the check in CI on the DAG definition so the scheduler never receives it."

**Question: "Implement an LRU cache."** Mid: "A hash map plus a doubly linked list, or `OrderedDict` in Python for O(1) get and put." Senior: "Both are O(1). I would note thread safety: the Python version needs a lock under concurrency, and at high throughput I would consider a sharded cache so the lock does not become the bottleneck."

> **🎤 Say this in the interview:** "I state the complexity first, then the data structure: a hash map for lookups, a heap for top-K, sort plus sweep for intervals, Kahn's algorithm for dependency order. I also say where the algorithm stops being the bottleneck, which at scale is usually I/O and skew."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Sorting when you only need a top-K. A full sort is O(n log n); a heap of size K is O(n log K). For K = 10 and n = 10⁹, the difference matters.

- Using `list.count` or `in` on a list inside a loop. That hides an O(n²) algorithm behind a simple-looking line.
- Forgetting that `dict` iteration order was not a guarantee before Python 3.7; today it is insertion order, but do not rely on it for correctness unless you state it.
- Using `heapq` as a max-heap without negating values. Python's `heapq` is a min-heap.
- Merging intervals without sorting first.
- Missing the cycle check in topological sort, so the loop silently returns a partial order.
- Forgetting that a join on a hot key in Spark is a skew problem, not an algorithm problem.
- Answering "O(n)" without saying what `n` is, or ignoring memory when the data does not fit.

## ✅ Check yourself (quiz)

1. What is the time complexity of counting frequencies of `n` items with a dictionary, and why?
   - A) O(n²), because each insert scans the dict
   - B) O(n), because each insert and update is O(1) on average
   - C) O(log n), because dictionaries are sorted
   - D) O(1), because the work is hidden in the hash

<details><summary>Answer</summary>

**B** — each dictionary update is O(1) on average, so `n` updates cost O(n). Option A describes a list scan, not a dict; C confuses a dict with a sorted structure.

</details>

2. You need the 10 most frequent items among 500 million events, with only a few GB of RAM. Which approach is the best fit?
   - A) Sort all events, then take the top 10
   - B) Count per key in a hash map that fits in memory (partitioned if needed), then select the top 10 with a heap of size 10
   - C) Use a nested loop over the events
   - D) Store the events in a list and call `in` for each candidate

<details><summary>Answer</summary>

**B** — counting is one pass, and a heap of size 10 selects the top items cheaply. If the distinct key space does not fit in memory, partition by key first, or use an approximate sketch. Sorting all 500 million events (A) is far more expensive than needed.

</details>

3. Given the dependencies `a -> b`, `b -> c`, `c -> a`, what should a topological sort report?
   - A) The order `a, b, c`
   - B) A cycle, because no node can start first
   - C) The order `c, b, a`
   - D) Nothing; topological sort ignores edges

<details><summary>Answer</summary>

**B** — every node has in-degree 1, so none is ready at the start and Kahn's algorithm processes zero nodes. The result has fewer nodes than the graph, which means a cycle.

</details>

4. **(Senior)** A sliding-window maximum over a stream of 10⁹ values with window size 10⁴ is implemented by sorting each window. What is the main problem, and what would you change?
   - A) Sorting is not allowed in Python
   - B) Each window costs O(w log w), so the total is far too slow; use a sliding window with a monotonic deque to get O(n) total
   - C) The window must be a power of two
   - D) Use a heap for each window and rebuild it from scratch

<details><summary>Answer</summary>

**B** — re-sorting each window repeats work that overlaps almost entirely. A monotonic deque maintains the candidates for the maximum as the window slides, giving O(n) overall. Rebuilding a heap per window (D) keeps the same repeated work.

</details>

5. **(Senior)** Your DAG scheduler runs a daily job for 1,000 tables, and one hot table takes 80% of the runtime because all its rows fall in one partition. Why does an O(n log n) analysis of the job not predict this, and what do you check?
   - A) The analysis is always correct; the hot table is a bug in the data
   - B) Big-O assumes work is spread evenly; skew puts most work on one worker, so the wall-clock time follows the largest partition, not the average. Check the key distribution and consider salting or a broadcast join
   - C) Topological sort does not support 1,000 nodes
   - D) Hash maps cannot be used in distributed systems

<details><summary>Answer</summary>

**B** — complexity counts total operations, but a parallel job finishes when its slowest partition finishes. Measure the key distribution (for example, top keys by count), then split the hot key (salting) or broadcast the small side of the join. See [[data-skew]].

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Hash map lookup cost | O(1) on average; worst case O(n) with many collisions |
| Top-K with a heap | O(n + d log K) time, O(d) memory for d distinct items |
| Why sort intervals first? | So overlapping ones are adjacent; then one sweep merges them |
| Kahn's algorithm, in one line | Repeatedly remove nodes with in-degree 0; leftover nodes mean a cycle |
| LRU cache in Python | `OrderedDict` with `move_to_end` on access and `popitem(last=False)` on overflow |
| External merge sort | Sort chunks that fit in memory, write them, then k-way merge with a heap |
| Sliding window maximum | Monotonic deque gives O(n) total |
| Why does skew break "O(1) average"? | Parallel time follows the slowest partition; one hot key overloads one worker |

## 🔗 Go further

- **sql-window-functions** shows the same ranking and sliding-window ideas expressed in SQL, which many DE interviews test as well.
- **python-core** covers the built-in collections (`dict`, `deque`, `heapq`, `OrderedDict`) used in this lesson, with their complexity.
- **kafka-architecture** explains partitions and hot keys, the streaming version of the skew problem.
- **airflow** shows how a DAG of tasks is scheduled, which is topological sort in production.
- Practise on LeetCode Easy and Medium problems tagged "heap", "intervals" and "graph" first; they cover most of this lesson's patterns.
