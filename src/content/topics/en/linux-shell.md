---
order: 7
tid: "linux-shell"
track: "1"
section: null
num: "1.1"
title: "Linux & Shell for Data Engineers"
type: "technology"
level: "beginner"
priority: "P1"
est_hours: 4
summary: "Everyday server-side data work: fast file processing with CLI tools, process and disk management, Bash scripting. Interviewers sometimes ask \"how would you analyze a 100 GB file without Spark?\""
covers: ["grep/awk/sed/cut/sort/uniq/xargs/parallel","find","jq","head/tail/split","pipes & redirection","cron","systemd","top/htop/iostat/vmstat","df/du","lsof","ssh/scp/rsync","file permissions"]
interview_qs: ["Count unique IPs in a huge log file","Find the 10 largest files","Why is the disk full but `du` shows free space?"]
extras: []
prerequisites: []
related: ["python-large-files", "file-ingestion", "data-formats", "networking-essentials"]
---

## 🎯 TL;DR

> Linux gives you small, sharp tools that read text and files as streams. Chained with **pipes** (`|`), they can process a multi-gigabyte file on one machine in minutes, often before you even think about a cluster.

- Streams beat loading: `grep`, `awk`, `sort` and `uniq` read line by line; memory stays small (except `sort`, which spills to disk).
- Every process has three standard streams: stdin (0), stdout (1) and stderr (2). Redirection (`>`, `>>`, `2>`, `<`) and pipes connect them.
- Know the disk and process tools (`df`, `du`, `lsof`, `top`, `iostat`). Most "mystery" production problems are a full disk, a stuck process or a saturated I/O device.
- Write scripts that fail loudly: `set -euo pipefail`, quoted variables, and explicit exit codes.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A kitchen assembly line. Each station (command) does one job and passes the result to the next station (pipe). You do not carry the whole pile of vegetables from station to station at once; each item flows through.

Create a small access log to practise with:

```bash
mkdir -p ~/lab && cd ~/lab
printf '%s\n' \
  '10.0.0.1 GET /home 200' \
  '10.0.0.2 GET /login 401' \
  '10.0.0.1 POST /api 500' \
  '10.0.0.3 GET /home 200' \
  '10.0.0.1 GET /home 200' > access.log
```

Three commands you will use every day:

```bash
wc -l access.log          # count lines: 5
head -n 2 access.log      # first 2 lines
tail -n 1 access.log      # last line
```

Now the first pipeline. The question: "how many requests did each IP send, busiest first?"

```bash
awk '{print $1}' access.log | sort | uniq -c | sort -rn
```

Read it left to right: `awk` takes the first column (the IP), `sort` puts equal IPs next to each other (`uniq` only collapses **adjacent** duplicates, so sorting first is required), `uniq -c` counts them, and the last `sort -rn` orders by count, numerically, descending.

Output:

```text
      3 10.0.0.1
      1 10.0.0.2
      1 10.0.0.3
```

> **⚠️ Watch out:** `uniq` without `sort` gives wrong counts. It compares only neighbouring lines.

## 🔧 How it really works (Mid)

### Streams, redirection and pipes

Each process starts with three file descriptors: `0` (stdin), `1` (stdout), `2` (stderr). A pipe connects the stdout of one process to the stdin of the next, and both run **at the same time**. Data moves in small buffers, so memory does not grow with file size.

```bash
grep ' 500 ' access.log > errors.txt        # stdout to a file (overwrite)
grep ' 500 ' access.log >> errors.txt       # append
grep ' 500 ' access.log 2> err.log          # redirect stderr only
some_job > out.log 2>&1                     # both streams to the same file
```

`2>&1` means "send stderr to wherever stdout currently points". The order matters: `> out.log 2>&1` works, `2>&1 > out.log` does not.

### The text toolbox

| Tool | Use it for | Example |
|---|---|---|
| `grep` | filter lines by pattern | `grep -E 'ERROR\|FATAL' app.log` |
| `cut` | pick fields by delimiter | `cut -d, -f1,3 data.csv` |
| `awk` | field logic, sums, conditions | `awk -F, 'NR>1 {s+=$3} END {print s}' sales.csv` |
| `sed` | stream edits | `sed 's/,/\t/g' in.csv` |
| `sort` / `uniq` | group and count | `sort \| uniq -c` |
| `xargs` | turn stdin into arguments | `find . -name '*.gz' \| xargs -n1 gunzip` |
| `split` | cut big files into parts | `split -l 1000000 big.csv part_` |
| `jq` | query JSON lines | `jq -r '.user_id' events.jsonl` |

`sort` is special: it uses **external merge sort**. It sorts chunks that fit in memory, writes them to temporary files, and merges them. You can help it with `-S` (memory buffer), `-T` (temp directory on a disk with space) and `--parallel`.

```bash
sort -S 2G --parallel=4 -T /data/tmp big_ids.txt | uniq > unique_ids.txt
```

### Finding files

```bash
# 10 largest regular files under /var, human-readable sizes
find /var -type f -printf '%s\t%p\n' 2>/dev/null | sort -rn | head -n 10

# files older than 30 days in a staging folder, delete them safely
find /data/staging -type f -mtime +30 -print0 | xargs -0 -r rm --
```

`-print0` with `xargs -0` handles filenames that contain spaces or newlines. Always use it in scripts.

### Parallel work

For independent files, let several workers run at once:

```bash
ls *.csv.gz | xargs -P 4 -I{} sh -c 'gunzip -c "{}" | wc -l'
```

`-P 4` runs four jobs in parallel. GNU `parallel` offers more features (progress, ordering) if it is installed.

### Processes, CPU, I/O and disk

- `top` / `htop`: CPU and memory per process. Look at the load average against the number of cores (`nproc`).
- `vmstat 1`: `si`/`so` (swap in/out) non-zero means memory pressure.
- `iostat -x 1` (package `sysstat`): `%util` near 100 and high `await` mean the disk is the bottleneck.
- `df -h`: free space per **filesystem**. `du -sh dir/`: space used by a **directory tree**.
- `lsof +L1`: open files whose link count is below 1, that is, deleted files still held open by a process.

### Scheduling and services

```bash
# crontab -e : every day at 02:00, append output and errors to a log
0 2 * * * /opt/etl/run_daily.sh >> /var/log/etl/daily.log 2>&1
```

Cron has a minimal `PATH` and no terminal. Use absolute paths inside the script, and log both streams. For long-running daemons (Kafka brokers, Airflow workers) a `systemd` unit is better: it restarts the process, captures logs in `journalctl` and manages dependencies.

### Remote work

```bash
ssh etl@db-host 'df -h /data'                  # run one command remotely
scp report.csv etl@db-host:/data/inbox/        # copy one file
rsync -avz --partial /data/export/ etl@db-host:/data/inbox/   # resumable, incremental sync
```

`rsync` is the tool for large transfers: it sends only changed parts and resumes after a broken connection.

### Permissions

`ls -l` shows `rwxr-x---`: owner, group, others. Numeric form: `chmod 750 script.sh` means owner rwx (7), group r-x (5), others nothing (0). Data pipelines often run as a service user; a `Permission denied` error is usually a wrong owner or a missing execute bit on a directory.

## 🧠 Senior depth & trade-offs

- **When not to use shell tools.** A shell pipeline is excellent for one machine, one-off analysis and ETL glue. Once the input is larger than one disk, must be joined with other datasets, or needs retries and lineage, move to Spark, DuckDB or an orchestrator. Keep the shell for what it does well.
- **Locale and sorting.** `sort` and `grep` depend on `LC_ALL`. In a script set `export LC_ALL=C` for byte-order sorting and fast regex. Otherwise `sort | uniq` may behave differently on two servers.
- **Memory and disks.** `sort` temp files can fill `/tmp`. Point `-T` at a disk with room, and monitor `df` before a large job. A job that dies with "No space left on device" often leaves partial output files; write to `file.tmp` and `mv` on success.
- **Disk full but `du` shows free space.** Two common causes: (1) a deleted log file is still held open by a process, so its blocks are not freed. `lsof +L1` shows it; restart the process or truncate the log instead of deleting. (2) `df` counts the whole filesystem, including reserved blocks. Also check inodes with `df -i`: a disk can be full of tiny files with free bytes but no free inodes.
- **Observability.** For every batch script, log start time, input row count, output row count and exit code. Alert on exit codes, not on the presence of a log line. Use `set -o pipefail`: without it, a pipeline reports the exit code of its last command only, so `bad_cmd | wc -l` looks successful.
- **Scripts that are safe to rerun.** Write to a temp name, validate, then rename. Use a lock (`flock`) so two cron runs cannot overlap. These are the shell versions of [[idempotency]].

```bash
#!/usr/bin/env bash
set -euo pipefail

IN_DIR="/data/inbox"
OUT="/data/clean/events.csv"
LOCK="/tmp/events_etl.lock"

exec 9>"$LOCK"
flock -n 9 || { echo "another run is active" >&2; exit 0; }

tmp="$(mktemp "${OUT}.XXXX")"
trap 'rm -f "$tmp"' EXIT

for f in "$IN_DIR"/*.csv; do
  [ -e "$f" ] || { echo "no input files" >&2; exit 1; }
  tail -n +2 "$f"
done | awk -F, 'NF == 4' > "$tmp"     # keep only well-formed rows

[ -s "$tmp" ] || { echo "empty output, aborting" >&2; exit 1; }
mv "$tmp" "$OUT"
echo "rows: $(wc -l < "$OUT")"
```

The `trap` removes the temp file on any exit, and `mv` makes the final file appear atomically on the same filesystem.

## 🧪 Try it

Use the `access.log` from the beginning of the lesson (or any log you have).

1. Print the top 3 status codes with their counts, busiest first.
2. Count requests per IP for only `GET` requests.
3. Given a big CSV with a header, print the sum of column 3 without loading the file into Python.

<details><summary>Show solutions</summary>

**1.**
```bash
awk '{print $4}' access.log | sort | uniq -c | sort -rn | head -n 3
```

**2.**
```bash
awk '$2 == "GET" {print $1}' access.log | sort | uniq -c | sort -rn
```

**3.**
```bash
awk -F, 'NR > 1 {sum += $3} END {print sum}' big.csv
```
`NR > 1` skips the header row. `awk` keeps only a running total, so memory is constant.

</details>

## 💬 Interview corner

**Question: "Count unique IPs in a huge log file."** Mid answer: "`awk '{print $1}' log | sort | uniq -c`. The sort is required because `uniq` only sees adjacent lines." Senior answer: "The same pipeline works for a 100 GB file on one machine with `LC_ALL=C` and a `sort` temp directory on a large disk. For exact distinct counts that do not fit in memory I would switch to approximate counting such as [[hyperloglog]], or to a distributed engine if the data is already in a lake."

**Question: "Find the 10 largest files."** Mid: "`du -ah / | sort -rh | head`." Senior: "`du` walks the whole tree and is slow on millions of files. I would use `find ... -printf '%s %p\n' | sort -rn | head`, limit the path to the suspect filesystem with `-xdev`, and run it with low I/O priority (`ionice -c3`) on a production box."

**Question: "Why is the disk full but `du` shows free space?"** Senior answer: "Either a deleted file is still open by a process (`lsof +L1` shows it, and the space is freed when the process exits or the file is truncated), or the filesystem is out of inodes (`df -i`), or a mount point is hiding files under it. I would check these in that order."

> **🎤 Say this in the interview:** "I start with streams: `grep`, `awk`, `sort | uniq -c`, `head`. I know when a single-machine pipeline is enough and when the data needs Spark. I write scripts with `set -euo pipefail`, temp files and locks, so a rerun is safe."

## ⚠️ Common mistakes

> **⚠️ Watch out:** `rm -rf $DIR/*` with an empty or unset `DIR` deletes from the root. Quote variables and use `${DIR:?}` to fail early.

- Parsing `ls` output in scripts. Filenames with spaces break it; use globs or `find -print0`.
- Forgetting `sort` before `uniq`.
- Running a pipeline and trusting its exit code without `pipefail`.
- Deleting a huge log file that a process still writes to. The space is not freed.
- Writing the final output file directly; a crash leaves half a file that looks valid.
- Running `cron` jobs with relative paths and no `PATH`, then debugging "works in my terminal".
- Using `grep` on a binary or gzipped file without `zgrep` or `zcat`.

## ✅ Check yourself (quiz)

1. Which command pair correctly counts occurrences of each distinct value in the first column of a file?
   - A) `cut -f1 f | uniq -c`
   - B) `cut -f1 f | sort | uniq -c`
   - C) `uniq -c f | sort`
   - D) `sort f | cut -f1 -c`

<details><summary>Answer</summary>

**B** — `uniq` only collapses adjacent duplicates, so the values must be sorted first. Option A gives wrong counts on unsorted input.

</details>

2. You run `./load.sh | tee load.log` and the script fails, but the pipeline exits with status 0. What is missing?
   - A) `set -x`
   - B) `set -o pipefail` (or `set -euo pipefail`)
   - C) `nohup`
   - D) `chmod +x`

<details><summary>Answer</summary>

**B** — without `pipefail`, the exit status of a pipeline is the status of its last command (`tee`), which succeeded. With `pipefail` the first failing command's status is returned.

</details>

3. The `/var` partition is 100% full. You deleted a 40 GB log file, but `df -h` still shows 100%. What is the most likely cause?
   - A) `df` caches results; wait 24 hours
   - B) A process still has the deleted file open; the blocks are freed only when it closes the file
   - C) The log must be moved, not deleted
   - D) `du` needs the `-a` flag

<details><summary>Answer</summary>

**B** — the directory entry is gone but the inode and its blocks remain while a file descriptor is open. `lsof +L1` finds it. Restarting the process (or truncating the file with `: > file` if the process must keep running) frees the space.

</details>

4. **(Senior)** A nightly job reads 2 TB of gzipped JSON on one 16-core server and must produce a deduplicated list of user IDs. Which approach is most defensible, and what is the main risk?
   - A) `cat *.gz | sort -u` with the default temp directory; risk is none
   - B) Parallel per-file extraction with `xargs -P`, a sorted unique merge with `sort -u` using a large `-T` disk, and an atomic `mv` at the end; risk is the temp disk filling up
   - C) Load everything into a Python dict in one process; risk is speed only
   - D) `grep -c` on each file; risk is that `grep` cannot read gzip

<details><summary>Answer</summary>

**B** — parallel extraction uses the cores, `sort -u` merges in bounded memory with disk spill, and the atomic `mv` keeps reruns safe. The risk is that temp files can need a large part of 2 TB, so check free space first. Option A ignores the small `/tmp`, C runs out of memory, D counts lines rather than producing IDs.

</details>

5. **(Senior)** Why is `cron` the wrong tool for a daemon that must restart after a crash?
   - A) `cron` cannot run shell scripts
   - B) `cron` starts jobs on a schedule; it does not supervise a long-running process or restart it on failure, which `systemd` does
   - C) `cron` only runs as root
   - D) `cron` cannot write logs

<details><summary>Answer</summary>

**B** — cron is a scheduler for one-shot jobs. A `systemd` service with `Restart=on-failure` supervises the process, captures its logs in the journal and orders it after its dependencies.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Why sort before `uniq`? | `uniq` only compares adjacent lines; sorting groups equal lines |
| What is `2>&1`? | Send stderr to the same place stdout currently points |
| What does `set -o pipefail` do? | A pipeline fails if any command in it fails, not only the last one |
| `xargs -0` with `find -print0` | Safe handling of filenames with spaces or newlines |
| `df` vs `du` | `df`: free space per filesystem; `du`: space used by a directory tree |
| Disk full, `du` shows free space | A deleted file still held open by a process (`lsof +L1`), or no free inodes (`df -i`) |
| How does `sort` handle big files? | External merge sort: sorted chunks on disk, then merged |
| Why use `flock` in cron jobs? | Prevent two runs from overlapping and corrupting output |

## 🔗 Go further

- **python-large-files** shows the same "stream, do not load" idea in Python with generators and chunked reads.
- **file-ingestion** and **data-formats** cover how the files you process with these tools are laid out (CSV, JSON lines, Parquet).
- **networking-essentials** explains the `ssh`, `rsync` and HTTP traffic that moves data between these machines.
- [[shuffle]] describes the distributed version of the sort-and-group pattern you used here with `sort | uniq`.
- Read `man sort`, `man find` and `man awk` once; their option lists are the fastest way to avoid a wrong result in production.
