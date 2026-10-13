---
order: 30
tid: "python-core"
track: "3"
section: null
num: "3.1"
title: "Core Python"
type: "technology"
level: "beginner"
priority: "P0"
est_hours: 4
summary: "Built-in data structures (list, dict, set, tuple), comprehensions, iterators and generators, context managers, decorators, and the collections and itertools modules."
covers: ["mutability","dict ordering","defaultdict/Counter/deque","heapq","functools (lru_cache, partial)","dataclasses","typing"]
interview_qs: []
extras: []
prerequisites: []
related: ["python-dataframes", "python-concurrency", "python-large-files", "python-production"]
---
## 🎯 TL;DR

> For data work, core Python is about **choosing the right built-in container** and **processing data lazily** so memory does not blow up.

- `list`, `dict`, `set` and `tuple` differ in order, mutability and duplicates. Pick by the question you need answered.
- `dict` keeps insertion order (guaranteed since Python 3.7). `set` and `dict` lookups are O(1) on average; `x in some_list` is O(n).
- A list comprehension builds the whole result in memory. A generator produces one item at a time.
- `collections`, `heapq`, `itertools` and `functools` replace most hand-written loops for counting, grouping, sliding windows, top-N and caching.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Think of containers as storage in a warehouse. A `list` is a numbered shelf you can add to and rearrange. A `tuple` is a sealed box: once packed, nobody changes it. A `set` is a bag where you only ask "is this inside?". A `dict` is a labelled locker: you look things up by key.

| Type | Ordered | Mutable | Duplicates | Typical use |
|---|---|---|---|---|
| `list` | yes | yes | yes | a sequence of records |
| `tuple` | yes | no | yes | a fixed record, or a dict key |
| `set` | no | yes | no | unique values, fast membership test |
| `dict` | insertion order | yes | keys are unique | mapping key → value |

A small example that uses all four:

```python
orders = [("ali", 120), ("sara", 80), ("ali", 50)]   # list of tuples

spent = {}                                 # dict: user -> total
for user, amount in orders:
    spent[user] = spent.get(user, 0) + amount

print(spent)                               # {'ali': 170, 'sara': 80}
print("sara" in spent)                     # True: checks keys
print(len({user for user, _ in orders}))   # 2: a set of unique users
```

The `for` loop unpacks each tuple into `user, amount`. This "tuple unpacking" appears in almost every data script you will write.

## 🔧 How it really works (Mid)

### Mutability and aliasing

Assignment copies a **reference**, not the object. Two names can point to the same list:

```python
a = [1, 2, 3]
b = a            # same object, not a copy
b.append(4)
print(a)         # [1, 2, 3, 4]
c = a.copy()     # shallow copy: a new list with the same items
```

Immutable types (`int`, `str`, `tuple`, `frozenset`) cannot be changed in place, so sharing them is safe. Mutable types need care when they cross function boundaries.

> **⚠️ Watch out:** A mutable default argument is created **once**, when the function is defined. It is shared by every call.
>
> ```python
> def add_item(items=[]):        # bug
>     items.append(1)
>     return items
>
> print(add_item(), add_item())  # [1, 1] [1, 1]: both names are the same list
>
> def add_item_safe(items=None):
>     if items is None:
>         items = []
>     items.append(1)
>     return items
>
> print(add_item_safe(), add_item_safe())  # [1] [1]
> ```

### Hashability and dict keys

A `dict` key must be **hashable**: its hash must not change over its life. `tuple` of immutable values is hashable; `list` and `dict` are not.

```python
seen = {}
seen[("ali", "2026-01-01")] = 1     # OK: tuple key
# seen[["ali", "2026-01-01"]] = 1   # TypeError: unhashable type: 'list'
```

### Comprehensions and generators

```python
squares_list = [x * x for x in range(1_000_000)]   # builds all million items now
squares_gen = (x * x for x in range(1_000_000))    # builds nothing yet
print(sum(squares_gen))                            # consumes one item at a time
```

Both lines compute the same sum, but the generator never stores the million values. Use `( ... )` when the result is consumed once and only needs one pass. Use a list when you need `len()`, indexing, or several passes.

A generator function uses `yield`:

```python
def read_records(n):
    for i in range(n):
        yield {"id": i, "amount": i * 10}

print(sum(r["amount"] for r in read_records(5)))   # 0+10+20+30+40 = 100
```

Execution pauses at each `yield` and resumes on the next `next()` call. This is the core idea behind streaming, and it is covered in depth in [[python-large-files]].

### collections

```python
from collections import Counter, defaultdict, deque

words = "data pipelines need clean data and clean code".split()

counts = Counter(words)
print(counts.most_common(2))        # [('data', 2), ('clean', 2)]

by_length = defaultdict(list)       # missing keys start as []
for w in words:
    by_length[len(w)].append(w)
print(sorted(by_length))            # [3, 4, 5, 9]

recent = deque(maxlen=3)            # fixed-size window, old items drop off
for x in [1, 2, 3, 4, 5]:
    recent.append(x)
print(list(recent))                 # [3, 4, 5]
```

`Counter` is a dict subclass for counting. `defaultdict` removes the "if key not in dict" check. `deque` is O(1) on both ends, so it is the right tool for queues and sliding windows, whereas `list.pop(0)` is O(n).

### heapq for top-N

Sorting a billion items to get the top 10 wastes work. `heapq` keeps a small heap:

```python
import heapq

revenue = [("api", 5), ("web", 9), ("db", 2), ("cache", 7)]
print(heapq.nlargest(2, revenue, key=lambda r: r[1]))   # [('web', 9), ('cache', 7)]

h = []
for x in [5, 1, 4]:
    heapq.heappush(h, x)
print(heapq.heappop(h))             # 1: heapq is a min-heap
```

### itertools and functools

```python
from itertools import islice, groupby
from functools import lru_cache, partial

rows = [("eu", 1), ("eu", 2), ("us", 3), ("us", 4), ("us", 5)]
for region, grp in groupby(rows, key=lambda r: r[0]):   # input MUST be sorted by key
    print(region, sum(v for _, v in grp))               # eu 3 / us 12

print(list(islice(range(10**9), 3)))                    # [0, 1, 2]: lazy slicing

@lru_cache(maxsize=None)
def fib(n):
    return n if n < 2 else fib(n - 1) + fib(n - 2)

print(fib(80))                                          # 23416728348467685

def convert(value, rate):
    return value * rate

usd_to_eur = partial(convert, rate=0.9)
print(usd_to_eur(100))                                  # 90.0
```

### Decorators and context managers

A decorator wraps a function. `functools.wraps` keeps the original name and docstring:

```python
import functools

def retry(times):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last = None
            for _ in range(times):
                try:
                    return fn(*args, **kwargs)
                except ConnectionError as e:
                    last = e
            raise last
        return wrapper
    return deco

calls = {"n": 0}

@retry(3)
def flaky():
    calls["n"] += 1
    if calls["n"] < 3:
        raise ConnectionError("boom")
    return "ok"

print(flaky(), calls["n"])                              # ok 3
```

A context manager guarantees cleanup, even when an exception happens. `with open(...)` is the most common example:

```python
from contextlib import contextmanager

@contextmanager
def batch(name):
    rows = []
    print("open", name)
    try:
        yield rows
    finally:
        print("close", name, "rows:", len(rows))

with batch("load") as rows:
    rows.append("a")
    rows.append("b")
# open load
# close load rows: 2
```

### dataclasses and typing

```python
from dataclasses import dataclass, field

@dataclass(frozen=True)
class Event:
    user_id: int
    action: str
    tags: tuple[str, ...] = ()        # immutable default is fine

@dataclass
class Batch:
    events: list[Event] = field(default_factory=list)   # never `= []`

e = Event(1, "click")
print(e)                              # Event(user_id=1, action='click', tags=())
```

`frozen=True` makes instances immutable and hashable, so they can be dict keys or set members. Type hints are not enforced at runtime; a checker such as mypy enforces them.

## 🧠 Senior depth & trade-offs

- **Memory is the real cost.** A Python object carries a header plus its own type pointer. A list of a million Python ints uses far more memory than the same values in a NumPy array or an Arrow column. Measure with `tracemalloc` or `sys.getsizeof` on a sample before you design around it. Columnar formats are covered in [[parquet]].
- **Generators are single-pass.** Once a generator is consumed, it is empty. Passing one into two functions is a bug that often shows up as "the second function sees nothing".
- **`lru_cache` is unbounded by default.** `maxsize=None` never evicts. In a long-running service this is a memory leak; give it a size limit.
- **`Counter` and `defaultdict` hold every key in memory.** Counting a stream with unbounded cardinality (for example, every user ID for a year) needs a bounded approach such as sketches. See [[hyperloglog]] and [[cardinality]].
- **The GIL.** CPython's global interpreter lock means threads do not run Python bytecode in parallel. Core containers are safe to use in one thread, but CPU-bound parallelism needs processes or a distributed engine. See [[python-concurrency]].
- **Determinism.** Sets have no defined order. Sort before writing output or comparing in tests.

## 🧪 Try it

1. **Group and count.** Using `defaultdict` (not `dict.get`), count how many words in `"the quick brown fox jumps over the lazy dog"` start with each letter. Print the letters in sorted order.
2. **Top-N without sorting everything.** Given `sales = [("a", 40), ("b", 90), ("c", 15), ("d", 70), ("e", 60)]`, print the top 2 by amount using `heapq`.
3. **Fix the bug.** This function is supposed to return a new list each call. Fix it and explain what was wrong.

   ```python
   def collect(x, bucket=[]):
       bucket.append(x)
       return bucket
   ```

<details><summary>Show solutions</summary>

**1.**
```python
from collections import defaultdict

words = "the quick brown fox jumps over the lazy dog".split()
by_letter = defaultdict(int)
for w in words:
    by_letter[w[0]] += 1
for letter in sorted(by_letter):
    print(letter, by_letter[letter])
```
Output: `b 1`, `d 1`, `f 1`, `j 1`, `l 1`, `o 1`, `q 1`, `t 2`.

**2.**
```python
import heapq
sales = [("a", 40), ("b", 90), ("c", 15), ("d", 70), ("e", 60)]
print(heapq.nlargest(2, sales, key=lambda s: s[1]))
# [('b', 90), ('d', 70)]
```

**3.** The default `[]` is created once at definition time, so every call appends to the same list. Use `bucket=None` and create the list inside the function when it is `None`.

</details>

## 💬 Interview corner

**"What is the difference between a list and a tuple?"** Mid: "A list is mutable and a tuple is immutable. Tuples are hashable, so they can be dict keys." Senior: "Immutability makes tuples safe to share across threads and processes and cheaper to hold; the trade-off is that you cannot grow them, so for growing sequences a list (or a preallocated array) is better."

**"Why is `x in my_set` faster than `x in my_list`?"** Mid: "A set is a hash table, so membership is O(1) on average; a list scans every element." Senior: "Average O(1) assumes a good hash distribution. Many colliding keys, or an expensive `__hash__`, degrade it, which is also why hash joins suffer on skewed keys."

**"Generator or list comprehension?"** Mid: "A generator when I need one pass; a list when I need `len`, indexing or repeated iteration." Senior: "For a pipeline over a file that does not fit in RAM I chain generators so memory stays flat. If the result could exceed memory, do not materialise it."

> **🎤 Say this in the interview:** "I pick the container by the operations I need: membership, order, uniqueness or mapping. For large inputs I stream with generators so memory stays flat, and I bound caches and counters explicitly."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Mutable default arguments (`def f(x=[])`) are shared across calls. Use `None` and create the object inside the function.

> **⚠️ Watch out:** `lambda` in a loop captures the variable, not its value at that time. All lambdas see the final `i`:
> ```python
> fns = [lambda: i for i in range(3)]
> print([f() for f in fns])          # [2, 2, 2]
> fns = [lambda i=i: i for i in range(3)]
> print([f() for f in fns])          # [0, 1, 2]
> ```

- Modifying a dict or list while iterating over it. Build a new collection, or iterate over a copy.
- Assuming `groupby` groups all equal keys. It only groups **adjacent** equal keys, so sort first.
- Using `is` to compare values. Use `==`; `is` checks identity, and small ints are cached only by accident.
- Using `list.pop(0)` as a queue. Use `collections.deque`.
- Using `Counter` or a dict on a stream with huge cardinality and expecting constant memory.
- Writing `x == None` instead of `x is None`.

## ✅ Check yourself (quiz)

1. Which structure gives the fastest "is this value present?" check on a large collection?
   - A) `list`
   - B) `tuple`
   - C) `set`
   - D) `str`

<details><summary>Answer</summary>

**C** — a set is backed by a hash table, so membership is O(1) on average. A list, tuple or string is scanned element by element, which is O(n).

</details>

2. What does this print?
   ```python
   def f(x, acc=[]):
       acc.append(x)
       return acc
   f(1)
   print(f(2))
   ```
   - A) `[2]`
   - B) `[1, 2]`
   - C) `2`
   - D) raises `TypeError`

<details><summary>Answer</summary>

**B** — the default list is created once and shared, so the second call sees the element from the first call.

</details>

3. Which is the right tool for a sliding window of the last 100 events?
   - A) `list` with `pop(0)` when it exceeds 100
   - B) `deque(maxlen=100)`
   - C) `set` of the last 100 events
   - D) `tuple` rebuilt on each event

<details><summary>Answer</summary>

**B** — `deque(maxlen=100)` drops the oldest item automatically in O(1). `list.pop(0)` is O(n) because every element shifts.

</details>

4. **(Senior)** A job reads a 40 GB CSV and does `rows = [parse(line) for line in f]` before aggregating. The machine runs out of memory. What is the cause and the fix?
   - A) `parse` should be a decorator
   - B) The list comprehension materialises every row; replace it with a generator expression, or iterate the file directly and aggregate as you go
   - C) Use `lru_cache` on `parse`
   - D) Switch to `tuple`, which uses less memory

<details><summary>Answer</summary>

**B** — the list holds all rows at once. Streaming (`for line in f: agg.update(parse(line))`) keeps memory bounded by the aggregate state, not the input size.

</details>

5. **(Senior)** You cache a function with `@lru_cache(maxsize=None)` in a service that receives millions of distinct arguments per day. What is the risk and how do you mitigate it?
   - A) No risk; the cache is evicted automatically
   - B) Memory grows without bound; set a finite `maxsize` and monitor hit rate
   - C) The cache is shared between processes, causing races
   - D) It makes the function non-deterministic

<details><summary>Answer</summary>

**B** — `maxsize=None` never evicts entries. Choose a finite `maxsize` that fits the working set, and measure `cache_info()` to confirm the hit rate justifies the memory.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Mutable vs immutable | `list`, `dict`, `set` can change in place; `tuple`, `str`, `frozenset`, `int` cannot |
| Why `dict` keys must be hashable | The hash is used to locate the entry; a changing hash breaks lookup |
| `x in list` vs `x in set` | O(n) scan vs O(1) average hash lookup |
| Generator vs list comprehension | Generator yields one item at a time; list stores everything |
| `defaultdict(list)` | Missing keys are created with an empty list on first access |
| `deque(maxlen=n)` | Bounded queue; oldest items drop off automatically |
| `heapq.nlargest(k, items)` | Top-k without sorting all items |
| `groupby` requirement | Input must be sorted by the key (groups adjacent keys only) |

## 🔗 Go further

- **python-dataframes** shows the same ideas with pandas, where the container is a column and the cost model changes.
- **python-large-files** applies generators and bounded memory to files larger than RAM.
- **python-concurrency** explains threads, processes and asyncio, and when the GIL matters.
- **python-production** covers packaging, logging and testing for pipelines.
- The official `collections`, `itertools` and `dataclasses` documentation lists every function; check the version-specific details for your Python release.
