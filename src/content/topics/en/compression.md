---
order: 13
tid: "compression"
track: "1"
section: null
num: "1.7"
title: "Compression"
type: "concept"
level: "intermediate"
priority: "P1"
est_hours: 1
summary: "Compression codecs and the speed vs ratio trade-off: gzip, snappy, lz4, zstd. Impact on CPU, network and file splittability."
covers: ["codec choice for Kafka","Parquet","ClickHouse; splittable vs non-splittable"]
interview_qs: []
extras: []
prerequisites: ["data-formats"]
related: ["columnar-storage","kafka-producers"]
---
## 🎯 TL;DR

> Compression trades **CPU time** for **bytes**. Fewer bytes mean less disk, less network and less I/O. The price is CPU to compress and decompress, and, for some formats, the loss of **splittability** (the ability to read a file in parallel chunks).

- **gzip**: strong ratio, slow-ish, a standard choice for text files. A gzipped text file is not splittable.
- **snappy**: very fast, modest ratio, no levels. A common default in columnar and streaming stacks.
- **lz4**: the fastest decompression, lower ratio. Good for hot paths.
- **zstd**: tunable from fast to very strong, good ratio at good speed, and supports trained dictionaries. A strong default for new systems.
- Compression works best on **encoded, columnar** data: encodings (dictionary, run-length, delta) first, then a general codec.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Packing a suitcase. Folding clothes tightly (compression) makes the suitcase lighter to carry (network and disk), but folding takes time, and unfolding takes time too. If you only have a short trip with a fast carrier, you pack lightly. If you ship a container across an ocean, you pack as tight as you can.

Here is the core idea with numbers. Assume you must move **10 GB** of logs over a **1 Gbps** link. 1 Gbps is 125 MB/s.

- Raw: 10,000 MB / 125 MB/s = **80 seconds** on the wire.
- Compressed 4:1: 2,500 MB / 125 MB/s = **20 seconds** on the wire, plus the CPU time to compress and decompress.

Whether compression wins depends on whether the network (or disk) is the bottleneck, and how fast your CPU can compress. Both answers change with the data.

Two words you will hear constantly:

- **Ratio**: original size divided by compressed size. `10 GB -> 2.5 GB` is a 4:1 ratio, written 4x.
- **Splittable**: a big file can be processed by several workers, each reading a different byte range. This is what lets Spark or Hadoop run many tasks on one file.

## 🔧 How it really works (Mid)

### The codecs

| Codec | Ratio | Compress speed | Decompress speed | Levels | Typical use |
|---|---|---|---|---|---|
| gzip (DEFLATE) | high | slow to medium | medium | yes | Archives, text exports, HTTP |
| snappy | medium | very fast | very fast | no | Common Parquet and streaming choice |
| lz4 | low to medium | very fast | fastest | yes | Hot paths, caches, ClickHouse default |
| zstd | high (tunable) | fast to slow | fast | yes (1 to 22) | General purpose, new systems, dictionaries |

Speeds are **relative**. Always measure your own data on your own hardware. The script below runs stdlib codecs on synthetic log lines so you can see the shape of the trade-off:

```python
import bz2
import gzip
import lzma
import random
import time
import zlib

random.seed(7)
# Synthetic web-log lines: repetitive paths and status codes, like real logs
PATHS = ["/home", "/search?q=kafka", "/api/v1/orders", "/static/app.js", "/login"]
lines = []
for i in range(200_000):
    lines.append(f"2026-10-01T12:{i % 60:02d}:00Z GET {random.choice(PATHS)} "
                 f"{random.choice([200, 200, 200, 404, 500])} {random.randint(100, 9000)}\n")
raw = "".join(lines).encode("utf-8")

codecs = {
    "zlib (DEFLATE, gzip's core)": (lambda b: zlib.compress(b, 6), zlib.decompress),
    "gzip level 6": (lambda b: gzip.compress(b, compresslevel=6), gzip.decompress),
    "bz2 level 9": (lambda b: bz2.compress(b, 9), bz2.decompress),
    "lzma (xz) preset 6": (lambda b: lzma.compress(b, preset=6), lzma.decompress),
}

print(f"raw size: {len(raw) / 1e6:.1f} MB")
for name, (enc, dec) in codecs.items():
    t0 = time.perf_counter()
    packed = enc(raw)
    t1 = time.perf_counter()
    restored = dec(packed)
    t2 = time.perf_counter()
    assert restored == raw, "round trip failed"
    print(f"{name:28s} ratio {len(raw) / len(packed):6.1f}x  "
          f"compress {t1 - t0:6.3f}s  decompress {t2 - t1:6.3f}s")
```

Run it. The stronger codecs (bz2, lzma) give a better ratio but cost much more CPU to compress. The exact timings depend on your machine, so treat them as a pattern, not a benchmark.

### Splittability: the hidden cost

A splittable format lets an engine read a 10 GB file as 100 chunks of 100 MB, in parallel. A non-splittable compressed stream must be read from the start, so **one task reads the whole file**.

- A **gzipped CSV** is not splittable. One Spark task gets the entire file, and the other executors wait.
- A **Parquet** file is split into row groups and column chunks. Compression is applied **inside** those units, so the file stays splittable whatever codec you choose.
- **Avro** compresses its data blocks with the chosen codec, so the file stays splittable at block boundaries, like Parquet.

Rule: for raw text, prefer a splittable block format or uncompressed-but-small files. For columnar data, compression inside the format is free of this problem.

### Compression in Kafka

Kafka compresses **batches** of records, not single messages. The producer sets the codec:

```properties
# producer configuration
compression.type=zstd
linger.ms=20
batch.size=131072
```

Larger batches compress better, which is why `linger.ms` and `batch.size` matter. The broker stores the batch as received. Consumers decompress it. If the broker's topic setting differs from the producer's, the broker may recompress, which costs broker CPU. Keep the codec consistent across producers and topics.

### Compression in Parquet and Spark

Columnar formats compress each column chunk. A column of low-cardinality strings (country codes, statuses) compresses far better than a column of random UUIDs, because the **encoding** (dictionary, run-length, delta) does most of the work before the codec runs.

```python
(df.write
   .option("compression", "zstd")          # codec for the Parquet output
   .mode("overwrite")
   .parquet("s3a://lake/bronze/events/"))
```

The same setting is available as the Spark SQL configuration `spark.sql.parquet.compression.codec`. Check the default for your Spark version instead of assuming it.

### Compression in ClickHouse

ClickHouse lets you choose a codec **per column**, and it supports specialised codecs that run before the general one:

```sql
CREATE TABLE events
(
    ts       DateTime CODEC(DoubleDelta, ZSTD(1)),
    user_id  UInt64   CODEC(Delta, ZSTD),
    url      String   CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY ts;
```

`DoubleDelta` turns steadily growing timestamps into near-zero deltas, which the general codec then squeezes. Higher ZSTD levels trade CPU at insert time for smaller parts on disk.

## 🧠 Senior depth & trade-offs

- **Pick by bottleneck.** If the cluster is network-bound (cross-AZ traffic, slow object storage) a stronger codec pays off. If it is CPU-bound (a join or a regex that already saturates cores), a lighter codec like snappy or lz4 is cheaper overall. Measure end-to-end job time, not only file size.
- **Compression level is a dial, not a flag.** For zstd, levels 1 to 3 are fast and cheap; very high levels can add large CPU cost for small gains. For ingestion, a fast level is usually right; for cold archive, a high level.
- **Small files defeat compression.** Compressing many tiny files gives poor ratios and adds per-file overhead. Compact small files before they are compressed, and see [[small-files]].
- **Dictionaries.** zstd can train a dictionary on samples of similar small records (for example, JSON events of the same schema). This helps Kafka topics and key-value stores with small messages, where each message compresses poorly alone.
- **Encrypted data does not compress.** Encrypt after compression, never before. Ciphertext looks random and will not shrink.
- **Monitor.** Track bytes in and out per topic or table, the compression ratio over time, CPU on producers and brokers, and decompression time in consumers. A sudden drop in ratio often means the upstream schema changed (for example, a new random ID column was added).
- **Interoperability.** Every consumer must support the codec. Ship a new codec only after you have upgraded all readers, or you will get read failures that look like corruption.

## 🧪 Try it

Use the Python script in the Mid section, or any dataset of your own with more than 10 MB of text.

1. **Ratio vs CPU.** Change the codecs to a single pair (gzip level 1 and gzip level 9). Predict which one compresses better and which one takes longer. Then run it.
2. **Splittability.** Explain in two sentences why a 50 GB gzipped CSV on a 100-executor Spark cluster can run slower than a 5 GB Parquet file of the same data.
3. **Kafka math.** A producer sends 200 MB/s of JSON. Compression gives 5:1. How many MB/s go over the network, and how much disk per day is saved if retention is 7 days and the stored data is the compressed size?

<details><summary>Show solutions</summary>

**1.** The level 9 variant compresses better and takes longer to compress; decompression time is usually similar. The exact numbers depend on your data and machine.

**2.** The gzip file cannot be split, so one task reads all 50 GB while the other 99 executors have nothing to do. The Parquet file splits into many row groups, so the work is spread across the cluster and each task reads only the columns it needs.

**3.** Over the network: 200 MB/s / 5 = 40 MB/s. Per day: 40 MB/s x 86,400 s = 3,456,000 MB = about 3.456 TB of compressed data written per day. Uncompressed would be 200 MB/s x 86,400 s = 17.28 TB per day. Over 7 days, the compressed retention is about 24.2 TB, against 120.96 TB uncompressed, so about 96.8 TB is saved.

</details>

## 💬 Interview corner

**Question: "Which compression would you use for a Kafka topic of JSON events?"**
Mid answer: "I'd use zstd or lz4 at the producer, with batching so the batches are big enough to compress well."
Senior answer: "It depends on the bottleneck. If brokers or the network are the limit, zstd at a moderate level, because the ratio saves disk and cross-AZ traffic. If producers are CPU-bound, lz4 or snappy. I'd batch with `linger.ms` so compression sees enough data, and I'd check that every consumer supports the codec before I switch."

**Question: "Why does my Spark job run one task for a 40 GB file?"**
Mid: "The file is probably gzipped text, which is not splittable."
Senior: "A gzip stream has no internal split points, so Spark assigns the whole file to one task. The fix is to write the data as Parquet or another block format, or to split the input into multiple compressed files so the parallelism comes from the file count."

> **🎤 Say this in the interview:** "Compression trades CPU for bytes. I pick the codec by the bottleneck, check that the format is splittable, and I always test the ratio and the decode time on real data before I commit."

## ⚠️ Common mistakes

> **⚠️ Watch out:** A gzipped text file is not splittable. A 50 GB `.csv.gz` runs in one task, however many executors you add.

> **⚠️ Watch out:** Compressing already-compressed or encrypted data gives no gain and wastes CPU.

- Choosing the highest level by default. The extra ratio is often small and the CPU cost is large.
- Judging a codec by its ratio alone. Include decompression speed, because readers pay it on every query.
- Compressing tiny messages one at a time in Kafka. Use batching.
- Forgetting that the codec must be readable by every consumer and engine.
- Encrypting before compressing, which kills the ratio.
- Assuming the Spark or Kafka default codec without checking the version you run.

## ✅ Check yourself (quiz)

1. Which property makes a compressed file processable by many parallel tasks?
   - A) High ratio
   - B) Splittability
   - C) Encryption
   - D) A large block size on disk

<details><summary>Answer</summary>

**B** — A splittable format lets each task read a different range. Ratio (A) and disk block size (D) do not change how tasks divide the work, and encryption (C) does not help.

</details>

2. A `.csv.gz` file of 40 GB is read by a Spark job. What is the most likely symptom?
   - A) Many tasks finish quickly
   - B) One task does most of the work while other executors sit idle
   - C) The file cannot be read at all
   - D) Parquet pushdown is used automatically

<details><summary>Answer</summary>

**B** — A gzip stream cannot be split, so the whole file goes to one task. The other executors have nothing to read.

</details>

3. Why do Parquet files stay splittable even with gzip or zstd compression?
   - A) Compression is applied to the whole file
   - B) Compression is applied per row group and column chunk, so each unit is independent
   - C) Parquet disables compression
   - D) Parquet stores data in ASCII

<details><summary>Answer</summary>

**B** — The file is divided into independently compressed units. A reader can seek to a row group and decompress only that unit.

</details>

4. (Senior) Your producer switched to a stronger zstd level and the broker CPU rose sharply, but the network did not change. What is the likely cause, and what would you check?

<details><summary>Answer</summary>

The most likely cause is recompression: the topic's `compression.type` differs from what the producer sends, so brokers decompress and re-encode every batch. Check the topic setting and make it match the producer. Then decide whether the higher zstd level is worth its CPU by measuring the ratio and the end-to-end latency. Decompression cost does not depend much on the level, so consumers are not the usual culprit here.

</details>

5. Why should encryption come after compression?
   - A) Encryption is slower on compressed data
   - B) Encrypted output looks random, so it cannot be compressed
   - C) Compression needs the key
   - D) Keys are stored in the compressed block

<details><summary>Answer</summary>

**B** — Good ciphertext has no repeating patterns, so a codec cannot shrink it. Compress first, then encrypt.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Compression ratio | Original size divided by compressed size (for example, 4:1) |
| Splittable | A file can be read by several parallel tasks, each on a byte or block range |
| Why is gzipped CSV hard to parallelise? | gzip is one stream without internal split points |
| Fastest decompression codec here | lz4 (lower ratio) |
| Tunable codec with dictionaries | zstd |
| Where does Parquet compress? | Inside each row group and column chunk |
| Kafka compresses what unit? | Batches of records, set by the producer `compression.type` |
| Compress before or after encrypting? | Before. Ciphertext does not compress. |

## 🔗 Go further

- Related lessons: [[columnar-storage]] for why columns compress better, [[data-formats]] for Parquet, Avro and ORC, and [[kafka-producers]] for batching and producer settings.
- Related: [[small-files]] for the cost of many tiny compressed files.
- Practice: take one real dataset from your project, write it three ways (gzipped CSV, Parquet with snappy, Parquet with zstd), and compare file size, read time and task count in Spark.

