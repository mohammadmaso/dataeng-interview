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
summary: "ساختارهای داده‌ی داخلی (list، dict، set، tuple)، comprehension، iterator و generator، context manager، decorator، و ماژول‌های collections و itertools."
covers: ["mutability","dict ordering","defaultdict/Counter/deque","heapq","functools (lru_cache, partial)","dataclasses","typing"]
interview_qs: []
extras: []
prerequisites: []
related: ["python-dataframes", "python-concurrency", "python-large-files", "python-production"]
---
## 🎯 خلاصه

> برای کار با داده، Python پایه یعنی **انتخاب درست ظرف داخلی** و **پردازش تنبل (lazy) داده** تا حافظه منفجر نشود.

- `list`، `dict`، `set` و `tuple` از نظر ترتیب، قابل‌تغییر بودن و تکراری بودن فرق دارند. ظرف را با توجه به سؤالی که باید جواب بدهد انتخاب کن.
- `dict` ترتیب درج را نگه می‌دارد (از Python 3.7 تضمین شده). جست‌وجو در `set` و `dict` به‌طور میانگین O(1) است؛ اما `x in some_list` از نوع O(n) است.
- list comprehension کل نتیجه را یک‌جا در حافظه می‌سازد. generator در هر لحظه فقط یک آیتم تولید می‌کند.
- `collections`، `heapq`، `itertools` و `functools` جای بسیاری از حلقه‌های دستی را برای شمارش، گروه‌بندی، پنجره‌ی کشویی، top-N و کش می‌گیرند.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** ظرف‌ها را مثل انبار در نظر بگیر. `list` یک قفسه‌ی شماره‌دار است که می‌توانی به آن اضافه کنی و جابه‌جایش کنی. `tuple` یک جعبه‌ی مهروموم‌شده است: بعد از بسته‌بندی کسی آن را عوض نمی‌کند. `set` کیفی است که فقط می‌پرسی «این چیز داخلش هست یا نه؟». `dict` یک کمد برچسب‌دار است: با کلید دنبال چیزی می‌گردی.

| نوع | ترتیب‌دار | قابل‌تغییر | تکرار | کاربرد معمول |
|---|---|---|---|---|
| `list` | بله | بله | بله | دنباله‌ای از رکوردها |
| `tuple` | بله | خیر | بله | یک رکورد ثابت، یا کلید dict |
| `set` | خیر | بله | خیر | مقادیر یکتا، بررسی سریع عضویت |
| `dict` | ترتیب درج | بله | کلیدها یکتا | نگاشت کلید → مقدار |

یک مثال کوچک که هر چهار ظرف را استفاده می‌کند:

```python
orders = [("ali", 120), ("sara", 80), ("ali", 50)]   # list of tuples

spent = {}                                 # dict: user -> total
for user, amount in orders:
    spent[user] = spent.get(user, 0) + amount

print(spent)                               # {'ali': 170, 'sara': 80}
print("sara" in spent)                     # True: checks keys
print(len({user for user, _ in orders}))   # 2: a set of unique users
```

حلقه‌ی `for` هر tuple را در `user, amount` باز می‌کند. به این کار «unpacking» تاپل می‌گویند و تقریباً در هر اسکریپت داده‌ای که می‌نویسی ظاهر می‌شود.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### قابل‌تغییر بودن و aliasing

انتساب (`=`) یک **reference** را کپی می‌کند، نه خود شیء. دو نام می‌توانند به یک list اشاره کنند:

```python
a = [1, 2, 3]
b = a            # same object, not a copy
b.append(4)
print(a)         # [1, 2, 3, 4]
c = a.copy()     # shallow copy: a new list with the same items
```

نوع‌های immutable (مثل `int`، `str`، `tuple` و `frozenset`) را نمی‌شود در جا تغییر داد، پس اشتراک‌گذاری‌شان امن است. نوع‌های mutable وقتی از مرز تابع رد می‌شوند نیاز به دقت دارند.

> **⚠️ مراقب باش:** آرگومان پیش‌فرضِ mutable **یک بار**، موقع تعریف تابع ساخته می‌شود و بین همه‌ی فراخوانی‌ها مشترک است.
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

### Hashability و کلیدهای dict

کلید `dict` باید **hashable** باشد: هَش آن نباید در طول عمرش تغییر کند. `tuple` از مقادیر immutable هَش‌پذیر است؛ `list` و `dict` نیستند.

```python
seen = {}
seen[("ali", "2026-01-01")] = 1     # OK: tuple key
# seen[["ali", "2026-01-01"]] = 1   # TypeError: unhashable type: 'list'
```

### Comprehension و generator

```python
squares_list = [x * x for x in range(1_000_000)]   # builds all million items now
squares_gen = (x * x for x in range(1_000_000))    # builds nothing yet
print(sum(squares_gen))                            # consumes one item at a time
```

هر دو خط یک جمع را حساب می‌کنند، اما generator هیچ‌وقت یک میلیون مقدار را ذخیره نمی‌کند. از `( ... )` وقتی استفاده کن که نتیجه یک‌بار مصرف می‌شود و فقط یک پیمایش لازم است. از list وقتی استفاده کن که `len()`، اندیس‌گذاری یا چند پیمایش لازم داری.

تابع generator از `yield` استفاده می‌کند:

```python
def read_records(n):
    for i in range(n):
        yield {"id": i, "amount": i * 10}

print(sum(r["amount"] for r in read_records(5)))   # 0+10+20+30+40 = 100
```

اجرا در هر `yield` متوقف می‌شود و با `next()` بعدی ادامه پیدا می‌کند. ایده‌ی اصلیِ پردازش جریانی همین است و در [[python-large-files]] عمیق‌تر بررسی شده است.

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

`Counter` یک زیرکلاس dict برای شمارش است. `defaultdict` بررسی «اگر کلید نیست» را حذف می‌کند. `deque` از هر دو طرف O(1) است، برای صف و پنجره‌ی کشویی مناسب است؛ در حالی که `list.pop(0)` از نوع O(n) است.

### heapq برای top-N

مرتب‌کردن یک میلیارد آیتم فقط برای ۱۰ تای اول کار بیهوده است. `heapq` یک heap کوچک نگه می‌دارد:

```python
import heapq

revenue = [("api", 5), ("web", 9), ("db", 2), ("cache", 7)]
print(heapq.nlargest(2, revenue, key=lambda r: r[1]))   # [('web', 9), ('cache', 7)]

h = []
for x in [5, 1, 4]:
    heapq.heappush(h, x)
print(heapq.heappop(h))             # 1: heapq is a min-heap
```

### itertools و functools

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

### Decorator و context manager

decorator یک تابع را در بر می‌گیرد. `functools.wraps` نام و docstring تابع اصلی را نگه می‌دارد:

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

context manager پاک‌سازی را تضمین می‌کند، حتی وقتی خطایی رخ می‌دهد. `with open(...)` رایج‌ترین نمونه است:

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

### dataclasses و typing

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

`frozen=True` نمونه‌ها را immutable و hashable می‌کند، پس می‌توانند کلید dict یا عضو set باشند. type hintها در زمان اجرا اعمال نمی‌شوند؛ ابزاری مثل mypy آن‌ها را بررسی می‌کند.

## 🧠 عمق و trade-offها (سنیور)

- **حافظه هزینه‌ی واقعی است.** هر شیء پایتون هدر و اشاره‌گر نوع خودش را دارد. یک list با یک میلیون int پایتون حافظه‌ی خیلی بیشتری از همان مقادیر در یک آرایه‌ی NumPy یا یک ستون Arrow مصرف می‌کند. قبل از طراحی، روی یک نمونه با `tracemalloc` یا `sys.getsizeof` اندازه بگیر. فرمت‌های ستونی در [[parquet]] توضیح داده شده‌اند.
- **generator یک‌بار مصرف است.** وقتی یک generator مصرف شد، خالی است. دادن آن به دو تابع یک باگ رایج است که به شکل «تابع دوم هیچ‌چیز نمی‌بیند» ظاهر می‌شود.
- **`lru_cache` به‌طور پیش‌فرض بی‌کران است.** با `maxsize=None` هیچ‌وقت چیزی حذف نمی‌شود. در سرویس‌هایی که مدت طولانی اجرا می‌شوند این یک نشت حافظه است؛ حد مشخص بگذار.
- **`Counter` و `defaultdict` همه‌ی کلیدها را در حافظه نگه می‌دارند.** شمارش یک جریان با تعداد مقادیر متمایز بی‌کران نیاز به روش محدود دارد، مثل sketchها. به [[hyperloglog]] و [[cardinality]] نگاه کن.
- **GIL.** قفل سراسری مفسر CPython باعث می‌شود threadها بایت‌کد پایتون را همزمان اجرا نکنند. ظرف‌های داخلی در یک thread امن‌اند، اما موازی‌سازی CPU-bound به process یا موتور توزیع‌شده نیاز دارد. به [[python-concurrency]] نگاه کن.
- **ترتیب set تعریف‌شده نیست.** قبل از نوشتن خروجی یا مقایسه در تست، مرتب کن.

## 🧪 امتحان کن

1. **گروه‌بندی و شمارش.** با `defaultdict` (نه `dict.get`) شمار کلمات `"the quick brown fox jumps over the lazy dog"` را بر اساس حرف اول بشمار. حروف را به ترتیب الفبا چاپ کن.
2. **Top-N بدون مرتب‌کردن همه.** با `sales = [("a", 40), ("b", 90), ("c", 15), ("d", 70), ("e", 60)]` با `heapq` دو مورد برتر بر اساس مبلغ را چاپ کن.
3. **باگ را درست کن.** این تابع قرار است هر بار یک list جدید برگرداند. آن را درست کن و توضیح بده چه اشکالی داشت.

   ```python
   def collect(x, bucket=[]):
       bucket.append(x)
       return bucket
   ```

<details><summary>جواب‌ها را ببین</summary>

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
خروجی: `b 1`، `d 1`، `f 1`، `j 1`، `l 1`، `o 1`، `q 1`، `t 2`.

**2.**
```python
import heapq
sales = [("a", 40), ("b", 90), ("c", 15), ("d", 70), ("e", 60)]
print(heapq.nlargest(2, sales, key=lambda s: s[1]))
# [('b', 90), ('d', 70)]
```

**3.** مقدار پیش‌فرض `[]` یک بار موقع تعریف ساخته می‌شود، پس هر فراخوانی به همان list اضافه می‌کند. از `bucket=None` استفاده کن و اگر `None` بود، list را داخل تابع بساز.

</details>

## 💬 گوشه‌ی مصاحبه

**«تفاوت list و tuple چیست؟»** متوسط: «list قابل‌تغییر است و tuple غیرقابل‌تغییر. tuple هَش‌پذیر است، پس می‌تواند کلید dict باشد.» سنیور: «immutable بودن باعث می‌شود tuple بین threadها و processها امن باشد و نگه‌داشتنش ارزان‌تر شود؛ هزینه‌اش این است که نمی‌شود بزرگ‌ترش کرد، پس برای دنباله‌ای که رشد می‌کند list (یا آرایه‌ی از پیش تخصیص‌یافته) بهتر است.»

**«چرا `x in my_set` از `x in my_list` سریع‌تر است؟»** متوسط: «set یک hash table است، پس عضویت به‌طور میانگین O(1) است، ولی list همه‌ی عناصر را می‌گردد.» سنیور: «O(1) میانگین فرض توزیع خوب هَش است. کلیدهای زیاد با برخورد هَش، یا `__hash__` گران، آن را کند می‌کند؛ به همین دلیل hash join با کلید skew شده مشکل دارد.»

**«generator یا list comprehension؟»** متوسط: «generator وقتی فقط یک پیمایش لازم است؛ list وقتی به `len`، اندیس‌گذاری یا پیمایش تکراری نیاز دارم.» سنیور: «برای pipeline روی فایلی که در RAM جا نمی‌شود، generatorها را زنجیر می‌کنم تا حافظه ثابت بماند. اگر نتیجه ممکن است از حافظه بزرگ‌تر شود، آن را materialize نکن.»

> **🎤 این را در مصاحبه بگو:** «ظرف را بر اساس عملیاتی که لازم دارم انتخاب می‌کنم: عضویت، ترتیب، یکتایی یا نگاشت. برای ورودی‌های بزرگ با generator جریانی پردازش می‌کنم تا حافظه ثابت بماند و cache و شمارنده‌ها را صریحاً محدود می‌کنم.»

## ⚠️ اشتباهات رایج

> **⚠️ مراقب باش:** آرگومان‌های پیش‌فرض mutable (`def f(x=[])`) بین فراخوانی‌ها مشترک‌اند. از `None` استفاده کن و شیء را داخل تابع بساز.

> **⚠️ مراقب باش:** `lambda` داخل حلقه متغیر را می‌گیرد، نه مقدارش را در آن لحظه. همه‌ی lambdaها `i` آخر را می‌بینند:
> ```python
> fns = [lambda: i for i in range(3)]
> print([f() for f in fns])          # [2, 2, 2]
> fns = [lambda i=i: i for i in range(3)]
> print([f() for f in fns])          # [0, 1, 2]
> ```

- تغییر dict یا list حین پیمایش روی همان. یک مجموعه‌ی جدید بساز یا روی کپی پیمایش کن.
- فرض این‌که `groupby` همه‌ی کلیدهای برابر را گروه می‌کند. فقط کلیدهای **مجاور** را گروه می‌کند، پس اول مرتب کن.
- استفاده از `is` برای مقایسه‌ی مقدار. از `==` استفاده کن؛ `is` هویت را می‌سنجد.
- استفاده از `list.pop(0)` به‌عنوان صف. از `collections.deque` استفاده کن.
- استفاده از `Counter` یا dict روی جریانی با cardinality بسیار بالا و انتظار حافظه‌ی ثابت.
- نوشتن `x == None` به‌جای `x is None`.

## ✅ خودت را بسنج (کوئیز)

1. کدام ساختار سریع‌ترین بررسی «این مقدار هست؟» را روی یک مجموعه‌ی بزرگ می‌دهد؟
   - A) `list`
   - B) `tuple`
   - C) `set`
   - D) `str`

<details><summary>جواب</summary>

**C** — set روی یک hash table بنا شده، پس عضویت به‌طور میانگین O(1) است. list، tuple یا رشته عنصر به عنصر گشته می‌شوند که O(n) است.

</details>

2. این کد چه چیزی چاپ می‌کند؟
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
   - D) `TypeError` می‌دهد

<details><summary>جواب</summary>

**B** — list پیش‌فرض یک بار ساخته و مشترک می‌شود، پس فراخوانی دوم عنصر فراخوانی اول را هم می‌بیند.

</details>

3. برای یک پنجره‌ی کشویی از ۱۰۰ رویداد آخر کدام ابزار مناسب است؟
   - A) `list` با `pop(0)` وقتی از ۱۰۰ بیشتر شد
   - B) `deque(maxlen=100)`
   - C) `set` از ۱۰۰ رویداد آخر
   - D) `tuple` که با هر رویداد دوباره ساخته می‌شود

<details><summary>جواب</summary>

**B** — `deque(maxlen=100)` قدیمی‌ترین آیتم را در O(1) خودکار حذف می‌کند. `list.pop(0)` از نوع O(n) است چون همه‌ی عناصر جابه‌جا می‌شوند.

</details>

4. **(سنیور)** یک جاب فایل CSV به حجم ۴۰ گیگابایت را با `rows = [parse(line) for line in f]` می‌خواند و قبل از تجمیع همه‌ی سطرها را نگه می‌دارد. ماشین کم‌حافظه می‌آورد. علت و راه‌حل چیست؟
   - A) `parse` باید decorator باشد
   - B) list comprehension همه‌ی سطرها را materialize می‌کند؛ آن را با generator عوض کن، یا فایل را مستقیم بخوان و در حین خواندن تجمیع کن
   - C) روی `parse` از `lru_cache` استفاده کن
   - D) به `tuple` برو که حافظه کمتری مصرف می‌کند

<details><summary>جواب</summary>

**B** — list همه‌ی سطرها را هم‌زمان نگه می‌دارد. پردازش جریانی (`for line in f: agg.update(parse(line))`) حافظه را به وضعیت تجمیع محدود می‌کند، نه اندازه‌ی ورودی.

</details>

5. **(سنیور)** یک سرویس میلیون‌ها آرگومان متمایز در روز دریافت می‌کند و تابعی را با `@lru_cache(maxsize=None)` کش کرده‌ای. ریسک چیست و چطور کنترلش می‌کنی؟
   - A) ریسکی نیست؛ cache خودکار تخلیه می‌شود
   - B) حافظه بی‌کران رشد می‌کند؛ یک `maxsize` محدود بگذار و نرخ hit را پایش کن
   - C) cache بین processها مشترک است و race ایجاد می‌کند
   - D) تابع غیرقطعی می‌شود

<details><summary>جواب</summary>

**B** — `maxsize=None` هیچ ورودی‌ای را حذف نمی‌کند. `maxsize` محدودی انتخاب کن که با working set جور باشد و با `cache_info()` بررسی کن که نرخ hit ارزش حافظه را دارد.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| قابل‌تغییر (mutable) در برابر غیرقابل‌تغییر | `list`، `dict`، `set` در جا عوض می‌شوند؛ `tuple`، `str`، `frozenset`، `int` نه |
| چرا کلید dict باید hashable باشد | محل ورودی از روی هَش پیدا می‌شود؛ تغییر هَش جست‌وجو را خراب می‌کند |
| `x in list` در برابر `x in set` | اسکن O(n) در برابر جست‌وجوی hash با میانگین O(1) |
| generator در برابر list comprehension | generator در هر لحظه یک آیتم می‌دهد؛ list همه را ذخیره می‌کند |
| `defaultdict(list)` | کلید گمشده با یک list خالی در اولین دسترسی ساخته می‌شود |
| `deque(maxlen=n)` | صف محدود؛ قدیمی‌ترین آیتم خودکار حذف می‌شود |
| `heapq.nlargest(k, items)` | top-k بدون مرتب‌کردن همه‌ی آیتم‌ها |
| شرط `groupby` | ورودی باید بر اساس کلید مرتب باشد (فقط کلیدهای مجاور را گروه می‌کند) |

## 🔗 ادامه

- **python-dataframes** همین ایده‌ها را با pandas نشان می‌دهد؛ جایی که ظرف یک ستون است و مدل هزینه تغییر می‌کند.
- **python-large-files** generator و حافظه‌ی محدود را برای فایل‌های بزرگ‌تر از RAM به کار می‌گیرد.
- **python-concurrency** threadها، processها و asyncio را توضیح می‌دهد و این‌که GIL کی مهم است.
- **python-production** بسته‌بندی، logging و تست pipelineها را پوشش می‌دهد.
- مستندات رسمی `collections`، `itertools` و `dataclasses` همه‌ی توابع را فهرست کرده‌اند؛ جزئیات مخصوص نسخه‌ی Python خودت را هم چک کن.
