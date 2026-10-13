---
order: 15
tid: "os-jvm-essentials"
track: "1"
section: null
num: "1.9"
title: "OS & JVM Essentials for Data Systems"
type: "concept"
level: "advanced"
priority: "P1"
est_hours: 4
summary: "Spark, Kafka, Flink and Hadoop run on the JVM, so understand heap, GC, off-heap memory and the OS page cache. Kafka's speed comes from the page cache and zero-copy."
covers: ["processes vs threads","virtual memory","page cache","sequential vs random I/O","mmap","zero-copy (sendfile)","JVM heap & GC (G1, ZGC)","GC pauses in long-running jobs"]
interview_qs: []
extras: []
prerequisites: ["linux-shell"]
related: ["kafka-architecture","spark-memory-tuning","spark-architecture","networking-essentials"]
---
## 🎯 TL;DR

> Spark, Kafka, Flink and Hadoop are **processes** that run on the **JVM**, and they read and write files through the **operating system**. Their speed and their stalls come from three layers: the OS (threads, virtual memory, the page cache, I/O), the JVM (heap, garbage collection, off-heap memory), and the data path between them.

- A **process** has its own address space; **threads** share one. A JVM is one process with many threads.
- The **page cache** is the OS using free RAM to hold file data. Kafka's speed depends on it.
- **Sequential** I/O is much faster than **random** I/O, on disks and on SSDs too.
- **Zero-copy** (`sendfile`) moves bytes from the page cache to a socket without copying them through user space.
- The **JVM heap** is managed by a garbage collector. Long GC pauses stop all work in a JVM, which shows up as lag, timeouts and slow tasks.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Think of an office. A **process** is a whole office with its own rooms and locked files. **Threads** are the workers in that office, sharing the same desks and files. The **page cache** is a whiteboard the OS keeps near the door: frequently used documents stay on it, so people do not walk to the archive each time. The **JVM** is an office with a cleaner (the garbage collector) who sometimes stops everyone for a few seconds to tidy up.

You already use this model without noticing:

- When you run a Spark job, the driver and each executor are **separate processes**.
- Inside one executor, many **threads** run tasks at the same time.
- When you read a file twice, the second read is often much faster, because the OS kept the data in RAM.
- When a big Spark job suddenly slows down for a few seconds and then recovers, a **GC pause** is a common reason.

A small rule of thumb for juniors: **the fastest I/O is the one that never reaches the disk**, and the worst pauses are the ones where the whole JVM stops.

## 🔧 How it really works (Mid)

### Processes, threads and virtual memory

Each process gets its own **virtual address space**. The CPU and the OS map those virtual addresses to physical RAM in **pages** (typically 4 KB on Linux x86). A page is loaded lazily: the first access triggers a **page fault**, and the OS fills the page from disk or allocates it.

- Threads in the same process share the address space, so they share heap objects without copying. That is why a JVM can run hundreds of tasks cheaply, and also why a bug can corrupt shared state.
- Creating a process is expensive compared with a thread, and a context switch costs time on every switch. Many data systems use threads inside a few large processes for that reason.
- **RSS** (resident set size) is the part of the address space that is in RAM. A process can have a large virtual size and a small RSS.

### The page cache

When you read a file, the kernel keeps the pages in RAM. The next read of the same range is served from memory. When you write, the kernel buffers writes in the page cache and flushes them to disk later (or on `fsync`).

The small Python script below shows the effect. It writes a 256 MB file, asks the kernel to drop its cached pages, and then reads random 4 KB blocks cold and warm:

```python
import mmap
import os
import random
import tempfile
import time

BLOCK = 4096
SIZE = 256 * 1024 * 1024          # 256 MB test file
N_READS = 20_000


def make_file(path: str) -> None:
    chunk = os.urandom(1024 * 1024)
    with open(path, "wb") as f:
        for _ in range(SIZE // len(chunk)):
            f.write(chunk)
        f.flush()
        os.fsync(f.fileno())


def evict(fd: int) -> None:
    # Ask the kernel to drop this file's clean pages from the page cache
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)


def time_reads(fd: int, offsets) -> float:
    t0 = time.perf_counter()
    for off in offsets:
        os.pread(fd, BLOCK, off)
    return time.perf_counter() - t0


def main() -> None:
    d = tempfile.mkdtemp(prefix="osio-")
    path = os.path.join(d, "data.bin")
    make_file(path)
    blocks = SIZE // BLOCK
    sequential = [i * BLOCK for i in range(N_READS)]
    rnd = random.Random(42)
    randomized = [rnd.randrange(blocks) * BLOCK for _ in range(N_READS)]

    fd = os.open(path, os.O_RDONLY)
    evict(fd)
    print(f"cold sequential: {time_reads(fd, sequential):.3f}s")
    evict(fd)
    print(f"cold random:     {time_reads(fd, randomized):.3f}s")
    print(f"warm random:     {time_reads(fd, randomized):.3f}s   (second pass, page cache is hot)")

    # Memory-mapped access: the same pages, read through the address space
    with open(path, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
        t0 = time.perf_counter()
        total = 0
        for off in randomized:
            total += mm[off]          # touching a byte faults in its page
        print(f"mmap random touch (warm): {time.perf_counter() - t0:.3f}s checksum={total}")
    os.close(fd)
    os.remove(path)
    os.rmdir(d)


if __name__ == "__main__":
    main()
```

On a typical Linux laptop, cold random reads are several times slower than cold sequential ones, and a warm second pass is much faster again. Exact numbers depend on your disk and RAM, so read the pattern, not the digits.

### Sequential versus random I/O

A disk head (HDD) or an SSD's flash channel works best with long, contiguous reads and writes. Random 4 KB reads pay per-request overhead. The rule for data engineering is the same as the one in the script: **organize data so that the engine reads large contiguous ranges.** Columnar files, large row groups and log-structured writes all follow this rule. Kafka writes each partition as an append-only log, and that is why it is fast.

### mmap

`mmap` maps a file into the address space. Reading a byte of the mapping faults in its page through the page cache, with no explicit `read` call. It is convenient, and it is used by several storage engines and by the JVM for some file access. The cost is that page faults and the cache eviction policy are now in the hands of the OS, so memory pressure can cause latency spikes that look like random slowness.

### Zero-copy with sendfile

A naive network send copies the data: disk to page cache, page cache to a user buffer, user buffer to the socket buffer, then to the NIC. `sendfile` lets the kernel send the page-cache pages to the socket directly, skipping the user-space copies. Kafka uses this for consumer fetches of plaintext data, through the JVM call `FileChannel.transferTo`. This is one reason Kafka serves reads from the page cache at high throughput with little CPU.

The trade-off: when TLS is enabled, bytes must be encrypted in user space, so the simple zero-copy path does not apply in the same way. Plan network and CPU capacity with that in mind.

### The JVM heap and garbage collection

The JVM allocates objects on the **heap** and frees them with a **garbage collector** (GC). Its job is to find unreachable objects and reclaim their memory. The main collectors you will meet are:

- **G1** (the default collector in recent JDKs). It divides the heap into regions and aims for pause-time goals, using concurrent marking and incremental evacuation.
- **ZGC**. It is designed for very short pauses, even with large heaps, and most of its work runs concurrently with your application threads.

Stop-the-world pauses still happen in every collector. They are shorter with the right collector and heap size, but they never disappear entirely. A pause in an executor or a broker stops its threads, so tasks look stuck, heartbeats are missed, and sometimes a coordinator declares the node dead.

Useful JVM options (check the names and defaults for your JDK version):

```bash
# Spark executor example: fixed heap, G1 collector, GC logging
spark-submit \
  --conf spark.executor.memory=8g \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -Xlog:gc*:file=/var/log/executor-gc.log:time,uptime,level,tags" \
  my_job.py
```

Set `-Xms` equal to `-Xmx` in servers to avoid heap resizing, and read the GC log before you tune anything.

### Off-heap memory

Not every byte lives on the heap. Direct buffers (NIO, Netty), the Spark unified memory when off-heap is enabled, and native libraries allocate **off-heap** memory that the garbage collector does not manage. Off-heap memory is counted against the container or machine limit, not against `-Xmx`. An executor that runs out of memory outside the heap is killed by the container runtime, and the JVM logs show nothing wrong. Account for both.

Heaps above roughly 32 GB lose compressed object pointers, so a 34 GB heap can hold fewer objects than a 30 GB one. Use the largest heap that still keeps the pointers compressed unless you have measured a reason not to.

## 🧠 Senior depth & trade-offs

- **Heap versus page cache.** Kafka brokers keep the JVM heap modest and leave most of the RAM to the page cache, because the cache holds the log data that consumers read. On a shared host that gives a page cache too small to hold the hot data, and every consumer read goes to disk. Size RAM for the working set, not for the heap alone.
- **Spark executors.** Spark splits executor memory between execution, storage and overhead. Cached data, shuffle buffers and off-heap buffers all compete with the heap. A job that spills to disk, or runs long GC, is often a memory-configuration problem, not a code problem. See [[spark-memory-tuning]].
- **Pause budgets.** For a streaming job, a 2-second GC pause can exceed a latency target or a session timeout. Measure pause times with GC logs, and choose the collector and heap size against your latency budget. For batch, long G1 pauses on a huge heap usually mean too many live objects, which is a code issue (caching large collections, creating objects per row).
- **Serialization and object count.** The JVM's cost is driven by the number of live objects. A `HashMap` of boxed `Long`s per row creates millions of objects and GC work. Columnar or binary representations (Spark's Tungsten, Arrow buffers) keep objects few and large, which is why they are fast.
- **Containers.** In Kubernetes or YARN the JVM must see the container memory limit. Set the heap as a fraction of the container limit and leave room for off-heap and the OS. An OOM kill with a small heap is usually off-heap growth.
- **Fsync and durability.** Writes return after the page cache holds them unless the code calls `fsync` or the producer waits for replication. Know what a "successful write" means for your system before you promise durability.
- **Monitoring.** Watch GC pause time and frequency, heap and off-heap usage, page faults and major faults, disk read throughput and iowait, and the cache hit pattern of consumers. A jump in major page faults often precedes a latency incident.

## 🧪 Try it

Use Linux with Python 3.10 or later. The script in the Mid section needs about 300 MB of free disk.

1. **Measure the cache.** Run the script twice. Explain the difference between "cold random" and "warm random" in one sentence each.
2. **Sequential versus random.** Change `N_READS` to 100,000 and compare the cold sequential and cold random times. Write down the ratio, and explain what the ratio would mean for a Kafka consumer that reads a partition from the start versus one that seeks to random offsets.
3. **Read a GC log.** Run any small JVM program (or a Spark job locally) with `-Xlog:gc`, then find one young collection and one full (or mixed) collection in the output. Write down the pause times.

<details><summary>Show solutions</summary>

**1.** Cold random reads miss the page cache, so each read goes to disk. Warm random reads hit RAM, so the second pass avoids disk I/O.

**2.** The ratio is usually large on disks and still noticeable on SSDs. A consumer reading sequentially from the start benefits from read-ahead and large contiguous log segments. A consumer that seeks to random offsets loses that benefit, and if the data is not in the page cache, every fetch costs a disk seek.

**3.** Answers depend on your run. A young collection should be short and frequent; a mixed or full collection is longer and rarer. If full collections happen often, check live data size and object allocation.

</details>

## 💬 Interview corner

**Question: "Why is Kafka fast, given that it writes everything to disk?"**
Mid answer: "It writes sequentially to append-only logs, and consumers read from the OS page cache. Zero-copy `sendfile` sends the bytes to the socket without copying them through the JVM."
Senior answer: "Three things. Sequential appends turn random writes into streams the disk handles well. Reads are served from the page cache, so the hot data never leaves RAM, which is why the broker's heap stays small. And fetches use zero-copy when TLS is off, so the broker does little CPU work per byte. The risks are page-cache eviction under memory pressure and TLS, which removes the zero-copy path."

**Question: "A Spark job slows down for a few seconds every minute. Where do you look?"**
Mid: "I'd check the GC logs for long pauses in the executors."
Senior: "I'd correlate pauses in GC logs with stage timings, check whether the executors are close to their memory limit (heap or off-heap), and look for big cached objects or skewed partitions that create large live sets. Then I'd choose a collector and heap size against the latency target, and fix the object count in the code if that is the cause."

> **🎤 Say this in the interview:** "Data systems on the JVM are limited by three things: how the OS moves bytes, how much is cached in RAM, and how long the garbage collector pauses the process. I look at I/O patterns first, then the page cache, and then GC logs."

## ⚠️ Common mistakes

> **⚠️ Watch out:** A GC pause stops the application threads. A "slow" task with no CPU use may be waiting for a pause, not for I/O.

> **⚠️ Watch out:** Setting `-Xmx` to nearly the whole machine leaves no room for off-heap memory, the OS or the page cache. The process is then killed or swaps.

- Assuming a file read is slow without checking whether the page cache already holds it.
- Treating random and sequential I/O as the same, then designing a random-access pattern on a log-structured system.
- Tuning the heap before reading the GC log.
- Forgetting off-heap memory when an executor is killed with no heap error.
- Making the heap very large to fix an OOM, which makes GC pauses longer.
- Assuming zero-copy applies when TLS is on.

## ✅ Check yourself (quiz)

1. What is the main difference between a process and a thread?
   - A) A process shares heap memory with its threads
   - B) A process has its own address space; threads in it share that address space
   - C) A thread can have its own file descriptor table, and a process cannot
   - D) Threads always run on separate machines

<details><summary>Answer</summary>

**B** — A process owns an address space, and its threads share it. That sharing makes communication cheap and also makes data races possible. Option C is also false here, since threads share the process's file descriptors.

</details>

2. Why is Kafka's consumer read path fast when TLS is off?
   - A) Kafka compresses every read on the fly
   - B) Pages come from the page cache and are sent to the socket with zero-copy `sendfile`
   - C) Kafka keeps all data on the JVM heap
   - D) Consumers read the file with random I/O

<details><summary>Answer</summary>

**B** — The data is already in the page cache for hot reads, and `sendfile` moves it to the socket without copying through user space. The heap (C) is intentionally small, and random I/O (D) is the opposite of the pattern Kafka uses.

</details>

3. A Spark executor is killed by the container runtime, but the heap usage in the GC log looks normal. What is the most likely cause?
   - A) A GC pause that was too long
   - B) Off-heap memory growth, which the heap metrics do not show
   - C) A corrupted JVM class file
   - D) The page cache being too large

<details><summary>Answer</summary>

**B** — Off-heap memory (direct buffers, native libraries, Netty) counts against the container limit but not against the heap. The fix is to account for it in the container size and find the allocation source.

</details>

4. (Senior) A team doubles the heap of a streaming job to reduce GC frequency, and the pause times get longer. Why, and what would you do instead?

<details><summary>Answer</summary>

A larger heap means the collector has more live and dead space to process. Some collectors' pauses grow with heap size or with the amount of live data, so each pause can be longer even if they are less frequent. Measure the live set in the GC log, reduce object churn and large cached collections in the code, and pick a collector and heap size against the latency target rather than enlarging the heap by default.

</details>

5. Why is the page cache worth keeping RAM for on a Kafka broker?
   - A) It stores the JVM's class files
   - B) Hot log data is served from RAM, avoiding disk reads for consumers that read recent data
   - C) It replaces replication
   - D) It compresses the log for the producer

<details><summary>Answer</summary>

**B** — Consumers that read recent data hit RAM through the page cache. Replication (C) is a separate mechanism, and compression (D) is a producer or broker setting, not a page cache feature.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Process vs thread | A process owns an address space; threads in it share that space |
| Virtual memory page | Fixed-size unit (typically 4 KB), loaded on first access by a page fault |
| Page cache | RAM the kernel uses to hold file data; reads and writes go through it |
| Sequential vs random I/O | Sequential reads and writes are much faster; design for contiguous access |
| `sendfile` / zero-copy | Kernel sends page-cache data to a socket without user-space copies |
| Why TLS matters for Kafka fetches | Encryption in user space removes the simple zero-copy path |
| Stop-the-world pause | All application threads stop during a GC phase |
| Off-heap memory | Memory outside the GC heap, counted against the container limit |

## 🔗 Go further

- Related lessons: [[spark-memory-tuning]] for executor memory layout, [[kafka-architecture]] for how brokers use the log and page cache, and [[networking-essentials]] for the socket path that zero-copy skips.
- Related: [[spark-architecture]] for driver and executor processes, and [[jvm-languages-for-de]] for the language side of the JVM.
- Practice: run a Spark job locally with GC logging on, identify the longest pause, and find the stage it happened in with the Spark UI.

