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
summary: "پردازش فایلی بزرگ‌تر از RAM: خواندن جریانی خط‌به‌خط، chunking، generatorها، external sort و map-reduce دستی. سؤال رایج کدنویسی در مصاحبه‌های داده."
covers: []
interview_qs: ["Find top-10 URLs in a 50 GB log with 8 GB RAM","Merge K sorted files","Deduplicate a stream with bounded memory"]
extras: []
prerequisites: ["python-core"]
related: ["pyspark-api", "spark-memory-tuning", "mapreduce", "small-files", "dedup-idempotent-loads", "pattern-dedup"]
---
## 🎯 خلاصه

> برای پردازش فایلی بزرگ‌تر از RAM، **هرگز کل فایل را نگه‌دار نکن**. آن را خط‌به‌خط جریانی بخوان، فقط وضعیتِ محدود نگه‌دار (مثلاً شمارنده‌ی K مورد برتر یا پنجره‌ی کلیدهای اخیر)، و اگر خودِ وضعیت بزرگ شد، داده را **با hash** به سطل‌هایی تقسیم کن که در حافظه جا می‌شوند و بعد نتایج را ترکیب کن.

- خواندن با `read()` یا `readlines()` همه‌چیز را در حافظه بار می‌کند. پیمایش روی شیء فایل فقط یک خط در لحظه بار می‌کند.
- جریان‌خوانی حافظه‌ای O(1) نسبت به اندازه‌ی ورودی می‌دهد؛ آنچه حافظه را تعیین می‌کند، وضعیت تجمیعی است.
- external sort یعنی: تکه‌های جاشونده در RAM را مرتب کن، run‌های مرتب را روی دیسک بنویس، و بعد با `heapq.merge` آن‌ها را **k-way merge** کن.
- deduplication دقیق با حافظه‌ی محدود به‌طور کلی ممکن نیست؛ یا داده را partition می‌کنی، یا به یک پاسخ تقریبی و پنجره‌ای قانع می‌شوی.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** یک کتاب ۱۰۰۰ صفحه‌ای داری و میزی که فقط ۱۰ صفحه روی آن جا می‌شود. کل کتاب را روی میز کپی نمی‌کنی. صفحه‌به‌صفحه می‌خوانی، یادداشت کوتاهی از چیزی که می‌شماری نگه می‌داری، و هر صفحه را بعد از تمام شدنش کنار می‌گذاری.

باگی که بیشترین کرش‌های out-of-memory را می‌سازد:

```python
with open("access.log", "w") as f:            # sample file for the demo
    f.write("GET /a 200\nGET /b 500\nGET /c 500\n")

# Loads the whole file into memory: fails for a 50 GB file
with open("access.log") as f:
    lines = f.readlines()
```

نسخه‌ی جریانی هر بار یک خط می‌خواند:

```python
# Memory stays flat: one line at a time
n_errors = 0
with open("access.log", encoding="utf-8", errors="replace") as f:
    for line in f:
        if " 500 " in line:
            n_errors += 1
print(n_errors)
```

شیء فایل یک iterator است که از یک بافر می‌خواند. پایتون لیستی نمی‌سازد مگر این‌که تو بخواهی، پس نسخه‌ی دوم برای هر اندازه‌ای از فایل کار می‌کند، به شرطی که **وضعیت** کوچک بماند.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### جریان‌خوانی رکوردها و تجمیع

برای فایل‌های ساخت‌یافته، ماژول `csv` را به‌عنوان generator به کار ببر و فقط جمع‌های در حال اجرا را نگه دار:

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

وضعیت همان `totals` است که با تعداد **منطقه‌های متمایز** رشد می‌کند، نه با تعداد سطرها. این سؤال کلیدیِ هر تجمیع جریانی است: وضعیت چقدر بزرگ است؟

### Top-K با وضعیت محدود

برای `Counter` روی یک لاگ ۵۰ گیگابایتی، وضعیت یک ورودی به ازای هر URL متمایز است. اگر صدها میلیون URL متمایز داشته باشی، آن وضعیت در حافظه جا نمی‌شود. پاسخ کلاسیک این است: **اول partition کن، بعد تجمیع کن**:

1. هر URL را به یکی از `N` فایل سطل hash کن. هر وقوع یک URL یکسان به یک سطل می‌رود.
2. هر سطل را جداگانه بشمار. وضعیت آن فقط URLهای متمایز همان سطل است.
3. یک min-heap به اندازه‌ی `K` در میان سطل‌ها نگه دار.

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

نکته‌ی درستی این است: چون هر URL در دقیقاً یک سطل است، شمارش آن در سطلش **همان** شمارش کل است. بدون این ویژگی باید شمارش‌ها را بین سطل‌ها جمع می‌زدی.

### external sort و k-way merge

برای مرتب‌کردن فایلی بزرگ‌تر از RAM: تکه‌هایی را که جا می‌شوند مرتب کن، هر تکه‌ی مرتب (یک «run») را روی دیسک بنویس، و بعد run‌ها را merge کن. `heapq.merge` به‌صورت تنبل از همه‌ی run‌ها می‌خواند و فقط یک خط از هر run را در حافظه نگه می‌دارد:

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

اعداد با صفر پُر شده‌اند عمداً. خطوط به‌صورت متن مقایسه می‌شوند، پس `"10" < "9"` است. اعداد را به عرض ثابت پُر کن، یا `key` بده که عدد را parse کند.

### deduplication روی یک جریان

deduplication دقیق برای هر کلیدِ متمایزی که تاکنون دیده شده به حافظه نیاز دارد. سه گزینه داری:

- **Partition** (مثل top-K): کلیدها را در سطل‌ها hash کن و هر سطل را با یک set dedupe کن. دقیق است، اما به دیسک و یک پیمایش دوم نیاز دارد.
- **پنجره‌ای**: فقط `W` کلید آخر را به یاد بیاور. داخل پنجره دقیق است و بیرونش تقریبی.
- **احتمالاتی**: یک Bloom filter می‌گوید «قطعاً جدید است» یا «احتمالاً دیده شده». ممکن است یک رکورد یکتا را حذف کند (false positive)، پس فقط وقتی استفاده کن که این قابل‌قبول است. به [[bloom-filter]] نگاه کن.

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

اینجا `2` درست حذف می‌شود چون هنوز در پنجره است. اگر کلیدی از پنجره بیرون برود و دوباره بیاید، دوباره منتشر می‌شود. این trade-off را صریحاً در طراحی بنویس.

### chunking با pandas

وقتی داده برای پردازش ستونی مناسب است، pandas می‌تواند تکه‌ها (هر کدام یک `DataFrame`) را پردازش کند:

```python
import io
import pandas as pd

sample = io.StringIO("amount\n1\n2\n3\n")     # stands in for a huge file on disk
total = 0
for chunk in pd.read_csv(sample, chunksize=2):   # each chunk is a DataFrame
    total += chunk["amount"].sum()
print(total)                                     # 6
```

هر تکه یک DataFrame عادی است، پس می‌توانی از عملیات برداری استفاده کنی، اما وضعیتِ بین تکه‌ها هم‌چنان با خودِ توست. روی فایل واقعی، اندازه‌ی تکه را طوری انتخاب کن که در بودجه‌ی حافظه‌ات جا شود (مثلاً ۵۰۰ هزار سطر). 

## 🧠 عمق و trade-offها (سنیور)

- **اول اندازه‌ی وضعیت را بسنج.** قبل از نوشتن کد، `تعداد کلیدهای متمایز × بایت به ازای هر ورودی` را تخمین بزن. یک ورودی dict پایتون خیلی بیشتر از خود رشته هزینه دارد. با یک نمونه و `tracemalloc` اندازه بگیر، سپس تعداد سطل‌ها را طوری انتخاب کن که هر سطل با حاشیه‌ی امن جا شود.
- **تعداد سطل را از بودجه‌ی حافظه انتخاب کن، نه از عادت.** با ۱۰۰ میلیون URL متمایز و تقریباً ۱۵۰ بایت برای هر کدام (تخمین؛ اندازه‌اش را بگیر)، وضعیت حدود ۱۵ گیگابایت است. با ۶۴ سطل، هر سطل حدود ۲۳۰ مگابایت است. این را با حافظه‌ای که واقعاً به پروسس می‌دهی بسنج.
- **skew مربوط به کلید است، نه سطل.** یک URL خیلی داغ با hash قابل تقسیم نیست، اما در سطلش فقط یک ورودی است. مشکل وقتی ظاهر می‌شود که یک کلید آن‌قدر پرتکرار باشد که رویکرد per-key خراب شود. معادل توزیع‌شده‌اش مدیریت [[hot-key]] و salting است.
- **فراتر از یک ماشین، سراغ موتور برو.** وقتی داده به بیش از یک دیسک یا بیش از یک ساعت نیاز دارد، همان ایده‌ها (partition، external sort، merge) همان کاری است که Spark و Hadoop برایت انجام می‌دهند. نسخه‌ی پایتون ابزار درستی برای مصاحبه، یک جاب یک‌باره یا یک مرحله‌ی تک‌ماشینی است.
- **I/O غالب است.** حلقه‌ی جریانی معمولاً محدود به توان دیسک یا شبکه است. خواندن‌های بزرگ، پرهیز از کار گران به‌ازای هر خط، و فشرده‌سازی با جریان‌های `gzip` یا `zstd` ورودی/خروجی را کم می‌کند. قبل از بهینه‌سازی کد پایتون اندازه بگیر.
- **مدیریت خطا.** فایل‌های موقت باید هنگام خطا پاک شوند (`try/finally` یا `tempfile.TemporaryDirectory`). برای جاب قابل‌ازسرگیری، run‌ها را اتمی بنویس (اول در `.tmp`، بعد rename) تا کرش هرگز یک run نیمه‌نوشته نگذارد که معتبر به نظر برسد.
- **encoding.** لاگ‌های واقعی UTF-8 نامعتبر دارند. `errors="replace"` یا `"surrogateescape"` را عمداً انتخاب کن و تعداد خطوط متأثر را بشمار؛ خرابی بی‌صدا از کرش بدتر است.

## 🧪 امتحان کن

1. **جمع بدون بارگذاری کامل.** تابع `sum_column(path, name)` بنویس که یک ستون CSVِ عددی را با خواننده‌ی جریانی جمع کند. آن را روی فایل ۱۰۰ هزار سطری که در یک temp directory می‌سازی تست کن.
2. **تأیید top-K.** ۲۰ هزار URL تصادفی بساز. بررسی کن که `top_k_urls(path, 3)` همان شمارش‌های `collections.Counter(urls).most_common(3)` را برمی‌گرداند.
3. **merge سه فایل مرتب.** سه فایل داری که هرکدام اعداد مرتب با صفرِ پُرکننده دارند. با `heapq.merge` یک خروجی مرتب بساز، بدون این‌که هیچ فایلی را کامل در حافظه بخوانی.

<details><summary>جواب‌ها را ببین</summary>

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

**2.** از `top_k_urls` بالا روی فایلی با ۲۰ هزار URL تولیدشده استفاده کن. `[c for c, _ in got]` را با `[c for _, c in Counter(urls).most_common(3)]` مقایسه کن. شمارش‌ها را مقایسه کن نه URLها: تساوی در مرز انتخاب ممکن است URLهای متفاوتی را مشروع انتخاب کند.

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

## 💬 گوشه‌ی مصاحبه

**«ده URL پرتکرار را در یک لاگ ۵۰ گیگابایتی با ۸ گیگابایت RAM پیدا کن.»** متوسط: «فایل را جریانی می‌خوانم و با `Counter` می‌شمارم، بعد `most_common(10)`.» سنیور: «`Counter` برای هر URL متمایز یک ورودی نگه می‌دارد که شاید جا نشود. URLها را با hash به سطل‌هایی تقسیم می‌کنم تا هر سطل در حافظه جا شود، هر سطل را می‌شمارم، یک min-heap به اندازه‌ی ۱۰ بین سطل‌ها نگه می‌دارم و فایل‌های موقت را پاک می‌کنم. قبل از آن تعداد کلیدهای متمایز را روی یک نمونه می‌سنجم.»

**«K فایل مرتب را merge کن.»** متوسط: «یک min-heap با یک head برای هر فایل.» سنیور: «این همان `heapq.merge` است: زمان O(N log K) و حافظه‌ی O(K)، به‌صورت تنبل. آن را مرحله‌ی merge یک external sort می‌گذارم و مطمئن می‌شوم کلید مرتب‌سازی به‌درستی تایپ شده، نه این‌که به‌صورت متن مقایسه شود.»

**«یک stream را با حافظه‌ی محدود dedupe کن.»** سنیور: «dedup دقیق به وضعیتی نیاز دارد که با تعداد کلیدهای متمایز متناسب است. با بودجه‌ی ثابت، بین یک مجموعه‌ی پنجره‌ای (دقیق داخل پنجره)، یک Bloom filter (بدون false negative، اما false positive رکورد را حذف می‌کند) یا dedup پارتیشن‌شده روی دیسک انتخاب می‌کنم. می‌گویم کدام trade-off را می‌پذیرم و چرا.»

> **🎤 این را در مصاحبه بگو:** «از وضعیت شروع می‌کنم، نه از ورودی. اگر وضعیت جا شود، جریان‌خوانی کافی است. اگر جا نشود، داده را بر اساس کلید partition می‌کنم تا هر بخش جا شود و بعد merge. با صدای بلند می‌گویم چه چیزی را از دست می‌دهم، مثل دقت کامل یا I/O دیسک.»

## ⚠️ اشتباهات رایج

> **⚠️ مراقب باش:** `f.read()`، `f.readlines()` و `list(f)` همگی کل فایل را materialize می‌کنند. به‌جایش روی شیء فایل پیمایش کن.

> **⚠️ مراقب باش:** استفاده از `hash()` داخلی پایتون برای انتخاب سطل. این تابع برای هر پروسس تصادفی می‌شود (`PYTHONHASHSEED`)، پس یک جاب ری‌استارت‌شده داده را متفاوت partition می‌کند. از hash پایدار مثل `zlib.crc32` یا `hashlib` استفاده کن.

- مرتب‌کردن خطوط متنی حاوی عدد بدون پُرکردن صفر: `"10"` قبل از `"9"` می‌آید.
- فراموش‌کردن این‌که generator فقط یک‌بار مصرف می‌شود.
- باقی‌گذاشتن فایل‌های موقت run روی دیسک بعد از یک exception.
- نوشتن یک run نیمه‌کاره که بعد از کرش کامل به نظر می‌رسد.
- فرض این‌که عضویت در set در حافظه رایگان است: یک set با ۱۰۰ میلیون رشته چندین گیگابایت هزینه دارد.
- نادیده‌گرفتن بی‌صدای خطاهای encoding در یک لاگ بزرگ.

## ✅ خودت را بسنج (کوئیز)

1. کدام الگوی خواندن حافظه را مستقل از اندازه‌ی فایل نگه می‌دارد؟
   - A) `data = f.read()`
   - B) `lines = f.readlines()`
   - C) `for line in f:`
   - D) `list(f)`

<details><summary>جواب</summary>

**C** — پیمایش روی شیء فایل از یک بافر داخلی، یک خط در لحظه، می‌خواند. سه گزینه‌ی دیگر ساختاری متناسب با فایل می‌سازند.

</details>

2. چرا `top_k_urls` هر URL را قبل از شمارش به یک سطل hash می‌کند؟
   - A) برای مرتب‌کردن سریع‌تر URLها
   - B) تا وقوع‌های هر URL در یک سطل باشد و شمارش‌های هر سطل کامل باشد، در حالی که وضعیت هر سطل در RAM جا شود
   - C) برای رمزنگاری URLها
   - D) چون `Counter` نمی‌تواند رشته بشمارد

<details><summary>جواب</summary>

**B** — partition با hash پایدار باعث می‌شود هر کلید دقیقاً در یک سطل باشد. وضعیت هر سطل کوچک است و شمارش هیچ کلیدی بین سطل‌ها تقسیم نمی‌شود.

</details>

3. برای سطل‌ها از `hash(url) % 64` استفاده کرده‌ای. وقتی جاب روی همان داده دوباره اجرا شود چه اتفاقی می‌افتد؟
   - A) هیچ؛ `hash` پایدار است
   - B) پایتون hash رشته‌ها را برای هر پروسس تصادفی می‌کند، پس همان URL در اجرای بعدی ممکن است به سطل دیگری برود و فرض‌های یک جاب قابل‌ازسرگیری را خراب کند
   - C) تعداد سطل‌ها باید توانی از دو باشد
   - D) `Counter` نتیجه‌ی `%` را نمی‌پذیرد

<details><summary>جواب</summary>

**B** — `hash()` برای `str` به‌طور پیش‌فرض برای هر پروسس salt می‌شود. برای تابع partition که بین اجراها ثابت بماند، از `zlib.crc32` یا `hashlib` استفاده کن.

</details>

4. **(سنیور)** dedup stream تو یک پنجره از ۱ میلیون شناسه‌ی آخر را نگه می‌دارد. یک producer یک رویداد را ۲ میلیون رویداد بعد دوباره می‌فرستد. چه می‌شود و به کسب‌وکار چه می‌گویی؟
   - A) هیچ؛ پنجره همه‌ی تکرارها را می‌گیرد
   - B) تکرار عبور می‌کند، چون از پنجره بیرون رفته؛ پنجره را به‌عنوان یک تضمین صریح بیان کن و اگر لازم است از یک کلیدفروشگاه پایدار یا dedup دقیق پارتیشن‌شده استفاده کن
   - C) generator خطا می‌دهد
   - D) set بی‌کران رشد می‌کند

<details><summary>جواب</summary>

**B** — یک set پنجره‌ای فقط داخل پنجره دقیق است. اندازه‌ی پنجره را بخشی از قرارداد بنویس و اگر تکرارهای دیررس باید گرفته شوند، از یک store پایدار یا dedup پارتیشن‌شده استفاده کن.

</details>

5. **(سنیور)** merge کردن ۵۰۰ فایل run مرتب کند و پرحافظه است. ارزان‌ترین بهبود کدام است؟
   - A) همه‌ی run‌ها را در یک list بارگذاری کن و `sorted` صدا بزن
   - B) در چند مرحله merge کن (مثلاً ۲۰ تا در هر بار) تا هر مرحله تعداد کمی فایل باز و head‌های کوچک داشته باشد، به‌جای نگه‌داشتن ۵۰۰ فایل باز هم‌زمان
   - C) `heapq.merge` را با `min()` روی list از headها عوض کن
   - D) اندازه‌ی run‌ها را آن‌قدر بزرگ کن تا یک run شود

<details><summary>جواب</summary>

**B** — merge چندمرحله‌ای تعداد handleهای باز و رقابت I/O را محدود می‌کند؛ همان ایده‌ی multi-level merge در external sort. گزینه‌ی C برای هر خط O(K) هزینه دارد و گزینه‌ی D همان مسئله‌ی اصلی است.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| `f.read()` در برابر `for line in f:` | `read()` همه‌چیز را بار می‌کند؛ حلقه یک خط را در حافظه نگه می‌دارد |
| چه چیزی حافظه‌ی جریان را تعیین می‌کند؟ | اندازه‌ی وضعیت تجمیعی، نه اندازه‌ی ورودی |
| External sort | تکه‌های درون‌حافظه را به run تبدیل کن، بعد k-way merge |
| هزینه‌ی `heapq.merge` | زمان O(N log K)، حافظه‌ی O(K)، تنبل |
| چرا قبل از شمارش partition کنیم | هر کلید در یک سطل می‌افتد، پس شمارش هر سطل کامل است |
| hash پایدار برای partition | `zlib.crc32` یا `hashlib`، نه `hash()` |
| trade-off dedup پنجره‌ای | داخل پنجره دقیق؛ تکرارها ممکن است بعد از آن عبور کنند |
| ریسک Bloom filter | false negative ندارد، اما false positive رکوردهای یکتا را حذف می‌کند |

## 🔗 ادامه

- **python-core** generatorها، `Counter` و `heapq` را که این درس به آن‌ها وابسته است پوشش می‌دهد.
- **pattern-dedup** و **dedup-idempotent-loads** deduplication را در مقیاس pipeline پوشش می‌دهند.
- **pyspark-api** و **spark-memory-tuning** نشان می‌دهند همین ایده‌های partition، sort و merge چطور روی خوشه اجرا می‌شوند.
- **mapreduce** مدل map، shuffle و merge را توضیح می‌دهد که این رویکرد تک‌ماشینی از آن تقلید می‌کند.
- **small-files** وقتی مهم است که external sort فایل‌های run خیلی کوچک تولید کند؛ قبل از مرحله‌ی بعد آن‌ها را compact کن.
