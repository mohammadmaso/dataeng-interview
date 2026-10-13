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
summary: "سطح الگوریتم مورد انتظار در مصاحبه‌ی داده معمولاً Easy تا Medium است. تمرکز روی hash map، heap، sorting، two pointers، sliding window، intervals، گراف و topological sort (که پایه‌ی DAGهاست)."
covers: ["Big-O","arrays/strings","hashing","heaps & top-K","sorting & external merge sort","intervals","BFS/DFS","topological sort","union-find (basic)","LRU cache"]
interview_qs: ["Top-K frequent items","Merge overlapping intervals","Detect cycle in a DAG of jobs","Implement an LRU cache"]
extras: []
prerequisites: []
related: ["sql-window-functions", "python-core", "kafka-architecture", "airflow"]
---

## 🎯 خلاصه

> مصاحبه‌های مهندسی داده معمولاً تئوری الگوریتم را به خاطر خودش امتحان نمی‌کنند. می‌خواهند ببینند آیا برای یک مسئله‌ی استریم یا batch **ساختار داده** درست را انتخاب می‌کنی، و آیا می‌توانی بگویی روی داده‌ای که در حافظه جا نمی‌شود چقدر سریع است.

- **Big-O** نشان می‌دهد هزینه با اندازه‌ی ورودی `n` چطور رشد می‌کند. حلقه‌ی غالب و هزینه‌ی هر lookup را حساب کن.
- **Hash map** میانگین O(1) برای lookup می‌دهد. ابزار پیش‌فرض برای شمارش، deduplication و join در حافظه است.
- **Heap** بالاترین K عضو یک استریم را در زمان O(n log k) و حافظه‌ی O(k) نگه می‌دارد.
- **Sorting** راه را برای two pointers، ادغام بازه‌ها و dedup باز می‌کند. برای داده‌ای بزرگ‌تر از حافظه، همان ایده به external merge sort تبدیل می‌شود.
- **گراف** pipelineها را مدل می‌کند. topological sort ترتیب اجرای jobs را می‌دهد و cycle را پیدا می‌کند، دقیقاً کاری که orchestrator با یک DAG می‌کند.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 Analogy:** یک کتابخانه. **hash map** مثل کاتالوگ کارت‌هاست: مستقیم به قفسه‌ی عنوان کتاب می‌روی. **لیست** مثل تلی از کتاب‌هاست: برای پیدا کردن یکی ممکن است همه را نگاه کنی. **heap** مثل جدول مسابقات حذفی است: همیشه بهترین بازیکن را می‌دانی، بدون اینکه همه را مرتب کنی.

Big-O در یک جدول (n = تعداد آیتم‌ها):

| عملیات | hash map (dict) | لیست / آرایه | لیست مرتب |
|---|---|---|---|
| lookup با کلید | O(1) به‌طور میانگین | O(n) اسکن | O(log n) جستجوی دودویی |
| درج در انتها | O(1) | O(1) سرشکن‌شده | O(n) برای حفظ ترتیب |
| اسکن همه | O(n) | O(n) | O(n) |

شمردن اینکه هر مقدار چند بار آمده، اولین مسئله‌ای است که با hash map حل می‌کنی:

```python
from collections import Counter

events = ["click", "view", "click", "buy", "click", "view"]
counts = Counter(events)
print(counts)                  # Counter({'click': 3, 'view': 2, 'buy': 1})
print(counts["buy"])           # 1
print(counts["refund"])        # 0  (در Counter کلید گمشده صفر حساب می‌شود)
```

نسخه‌ی ساده‌لوحانه یک حلقه‌ی تودرتوست: برای هر رویداد کل لیست را برای تطابق بشماری. این O(n²) است. با hash map یک بار پیمایش کافی است و O(n) می‌شود.

```python
# O(n^2): برای یک میلیون رویداد کند است
def count_naive(events):
    return {e: events.count(e) for e in set(events)}

# O(n): یک پیمایش
def count_fast(events):
    counts = {}
    for e in events:
        counts[e] = counts.get(e, 0) + 1
    return counts
```

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### Top-K با heap

«ده آیتم پرتکرار» یک مسئله‌ی کلاسیک است. مرتب کردن همه‌ی شمارش‌ها برای d آیتم متمایز هزینه‌ی O(d log d) دارد. یک min-heap با اندازه‌ی K هزینه‌ی O(d log K) دارد: بزرگ‌ترین K مورد دیده‌شده را نگه می‌دارد و وقتی عدد بزرگ‌تری می‌رسد کوچک‌ترین را کنار می‌گذارد.

تابع `heapq.nlargest` پایتون همین کار را داخلی انجام می‌دهد:

```python
import heapq
from collections import Counter

def top_k_frequent(items, k):
    counts = Counter(items)                          # O(n) برای شمارش
    return heapq.nlargest(k, counts, key=counts.get) # O(d log k)

print(top_k_frequent(["a", "b", "a", "c", "a", "b"], 2))   # ['a', 'b']
```

وقتی داده یک استریم است و نمی‌توانی همه‌ی شمارش‌ها را نگه داری، از ساختارهای تقریبی مثل Count-Min sketch یا الگوریتم space-saving استفاده کن. مشکل [[hot-key]] در partitionهای Kafka یک مثال واقعی است که این موضوع در آن مهم می‌شود.

### بازه‌ها و sorting

بسیاری از مسئله‌های DE بازه‌اند: session، پنجره‌ی زمانی، دوره‌های فعال یک job. ترفند تقریباً همیشه این است: **اول بر اساس شروع مرتب کن، بعد یک بار جاروب کن**.

```python
def merge_intervals(intervals):
    merged = []
    for start, end in sorted(intervals):             # O(n log n)
        if merged and start <= merged[-1][1]:        # با آخرین بازه همپوشانی دارد
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged

print(merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]]))
# [[1, 6], [8, 10], [15, 18]]
```

بازه‌های مماس، مثل `[1, 3]` و `[3, 5]`، با `<=` ادغام می‌شوند. اگر قانون کسب‌وکار می‌گوید مماس بودن همپوشانی نیست، از `<` استفاده کن.

### Two pointers و sliding window

Two pointers روی ورودی مرتب یا بازه‌های پیوسته کار می‌کند و O(n²) را به O(n) تبدیل می‌کند.

```python
def max_sum_window(values, k):
    """بزرگ‌ترین مجموع k مقدار پشت‌سرهم، O(n)."""
    window = sum(values[:k])
    best = window
    for right in range(k, len(values)):
        window += values[right] - values[right - k]  # جابه‌جایی پنجره: اضافه و حذف
        best = max(best, window)
    return best

print(max_sum_window([2, 1, 5, 1, 3, 2], 3))   # 9  (5 + 1 + 3)
```

پنجره‌ی با اندازه‌ی متغیر همین‌طور کار می‌کند: `right` را جلو ببر و `left` را وقتی پنجره قانون را می‌شکند جمع کن. مثال: طولانی‌ترین زیررشته بدون کاراکتر تکراری، در O(n).

### گراف و topological sort

یک pipeline یک گراف جهت‌دار بدون cycle (DAG) است: یال `a -> b` یعنی «a باید قبل از b تمام شود». **ترتیب توپولوژیک** یک ترتیب اجرای معتبر است. **الگوریتم Kahn** آن را با شمارش in-degree می‌سازد؛ اگر بعضی گره‌ها هرگز به in-degree صفر نرسند، یک **cycle** وجود دارد.

```python
from collections import defaultdict, deque

def topo_sort(n_jobs, edges):
    """edges: (a, b) یعنی job a باید قبل از job b تمام شود. jobها 0 تا n_jobs-1 هستند."""
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

# extract -> clean -> load و extract -> validate -> load
print(topo_sort(4, [(0, 1), (1, 3), (0, 2), (2, 3)]))   # [0, 1, 2, 3]
```

این الگوریتم به سؤال «کدام کارها می‌توانند موازی اجرا شوند» هم جواب می‌دهد: همه‌ی گره‌های یک موج `ready` مستقل از هم‌اند.

### LRU cache

کش LRU (کمترین استفاده‌ی اخیر) ورودی‌ای را حذف می‌کند که مدت بیشتری دست نخورده. راه‌حل تمیز در پایتون `OrderedDict` است که ترتیب درج را نگه می‌دارد و جابه‌جایی به انتها را در O(1) انجام می‌دهد.

```python
from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.data = OrderedDict()

    def get(self, key):
        if key not in self.data:
            return -1
        self.data.move_to_end(key)          # به عنوان تازه‌ترین علامت بزن
        return self.data[key]

    def put(self, key, value):
        if key in self.data:
            self.data.move_to_end(key)
        self.data[key] = value
        if len(self.data) > self.capacity:
            self.data.popitem(last=False)   # حذف کم‌استفاده‌ترین

cache = LRUCache(2)
cache.put("a", 1); cache.put("b", 2)
cache.get("a")          # a الان تازه‌ترین است
cache.put("c", 3)       # b را حذف می‌کند
print(cache.get("b"))   # -1
```

### External merge sort و k-way merge

وقتی داده از حافظه بزرگ‌تر است، تکه‌تکه مرتب کن، هر تکه‌ی مرتب را روی دیسک بنویس، و تکه‌ها را با یک heap به اندازه‌ی k (یک سر برای هر تکه) ادغام کن. دستور `sort` در لینوکس همین کار را می‌کند و موتورهای دیتابیس برای `ORDER BY` روی جدول‌های بزرگ همین را انجام می‌دهند.

```python
import heapq

def merge_sorted_streams(*streams):
    """ادغام iterableهای از قبل مرتب با زمان O(total * log k) و حافظه‌ی O(k)."""
    yield from heapq.merge(*streams)

chunk_a = iter([1, 4, 9])     # تصور کن: هر کدام یک فایل مرتب است که خط‌به‌خط خوانده می‌شود
chunk_b = iter([2, 3, 10])
chunk_c = iter([5, 6])
print(list(merge_sorted_streams(chunk_a, chunk_b, chunk_c)))   # [1, 2, 3, 4, 5, 6, 9, 10]
```

## 🧠 عمق و trade-offها (سنیور)

- **Big-O کل هزینه نیست.** lookup در hash map از نظر تئوری O(1) است، ولی یک cache miss در یک hash table ۱۰۰ گیگی روی دیسک خیلی گران‌تر از یک مقایسه در یک آرایه‌ی کوچک مرتب است. در مهندسی داده I/O و shuffle معمولاً غالب‌اند. هزینه را بر حسب تعداد پیمایش‌های داده فکر کن: یک پیمایش ارزان است، sort یعنی یک پیمایش به‌علاوه‌ی ریختن روی دیسک، و join ممکن است یک shuffle روی شبکه باشد. ببین [[shuffle]].
- **اول بودجه‌ی حافظه را بسنج.** قبل از انتخاب ساختار بپرس چه چیزی در حافظه جا می‌شود. dict با ۵۰۰ میلیون کلید روی یک worker با ۱۶ گیگ جا نمی‌شود؛ جواب می‌شود partition کردن بر اساس کلید (تا هر partition جا شود)، یا یک sketch (HyperLogLog برای شمارش یکتا، Count-Min برای فراوانی)، یا سپردن کار به موتور.
- **Skew میانگین را خراب می‌کند.** hash partitioning فقط وقتی کلیدها را یکنواخت پخش می‌کند که خود کلیدها یکنواخت باشند. یک کلید داغ کل کارش را روی یک partition می‌گذارد و «O(1) میانگین» وقتی یک worker همه‌ی کار را می‌کند بی‌فایده است. ببین [[data-skew]] و [[salting]].
- **Topological sort همان کاری است که orchestrator می‌کند.** Airflow وابستگی taskها را به صورت DAG حل می‌کند و cycle را زمان‌بندی نمی‌کند. وقتی pipeline طراحی می‌کنی، cycle یعنی یک وابستگی که متوجه‌اش نشده‌ای؛ آن را هنگام parse پیدا کن، نه ساعت ۳ صبح.
- **Streaming سؤال را عوض می‌کند.** برای ورودی بی‌کران نمی‌توانی همه‌چیز را مرتب کنی. الگوریتم‌های پنجره‌ای، state محدود و سیاست حذف (ایده‌ی LRU) لازم است و باید بگویی با داده‌ی دیرِرسیده چه می‌شود. ببین [[watermark]].
- **کی خودت ننویس.** از `heapq`، `collections`، `sort` یا توابع داخلی موتور برای top-N و شمارش یکتا استفاده کن. ساختار داده‌ی دست‌نوشته در production ریسک است. در مصاحبه انتخاب کتابخانه را بگو و بعد نشان بده الگوریتم پشتش را می‌فهمی.
- **درباره‌ی پیچیدگی چه بگویی.** زمان و حافظه، فرض (hashing میانگین، sort مقایسه‌ای)، و گلوگاه در مقیاس بزرگ. «زمان O(n log k)، حافظه‌ی O(k)، و برای استریم هم کار می‌کند» یک جواب کامل است.

## 🧪 امتحان کن

۱. با فهرستی از sessionهای `(user_id, start, end)` تعداد کاربرانی را برگردان که حداقل یک session با `[t0, t1]` همپوشانی داشته. از set و یک پیمایش استفاده کن.
۲. با `n` کار و فهرستی از جفت‌های وابستگی، `True` برگردان اگر همه‌ی کارها بتوانند تمام شوند (cycle نباشد)، وگرنه `False`. از `topo_sort` همین درس استفاده کن.
۳. سه کلمه‌ی پرتکرار را از یک لیست پیدا کن؛ در حالت تساوی، کلمه‌ای را ترجیح بده که زودتر در ورودی آمده. هزینه‌ی زمان و حافظه را توضیح بده.

<details><summary>راه‌حل‌ها را نشان بده</summary>

**۱.**
```python
def users_in_window(sessions, t0, t1):
    users = set()
    for user_id, start, end in sessions:
        if start <= t1 and end >= t0:      # تست همپوشانی بازه‌ها
            users.add(user_id)
    return len(users)

print(users_in_window([(1, 0, 5), (2, 10, 12), (1, 20, 25)], 4, 11))   # 2
```
زمان O(s) برای `s` session و حافظه‌ی O(u) برای `u` کاربر. تست همپوشانی `start <= t1 and end >= t0` است.

**۲.**
```python
def can_finish(n_tasks, deps):
    try:
        topo_sort(n_tasks, deps)
        return True
    except ValueError:
        return False
```
الگوریتم Kahn زمان O(V + E) دارد. بررسی cycle همان `len(order) != n_jobs` است.

**۳.** با شاخص «اولین دیدن» برای شکستن تساوی:
```python
from collections import Counter

def top3_words(words):
    counts = Counter(words)                      # در CPython از ۳.۷ به بعد، Counter ترتیب درج را نگه می‌دارد
    return sorted(counts, key=lambda w: -counts[w])[:3]
```
زمان O(n + d log d) با sort کامل، یا O(n + d log 3) با `heapq.nlargest(3, ...)`. حافظه O(d). توجه کن که `sorted` پایدار است، پس تساوی‌ها ترتیب اولین دیدن را حفظ می‌کنند. بگو همین پایداری است که قانون تساوی را درست می‌کند.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «Top-K پرتکرارترین آیتم‌ها.»** جواب متوسط: «با hash map در O(n) بشمار، بعد با یک min-heap به اندازه‌ی K در O(d log K) بالاترین‌ها را بگیر.» جواب سنیور: «نسخه‌ی دقیق زمان O(n + d log K) و حافظه‌ی O(d) دارد. اگر استریم در حافظه جا نمی‌شود، از space-saving یا Count-Min sketch استفاده می‌کنم و جواب تقریبی با کران خطای اعلام‌شده را می‌پذیرم. اگر یک کلید غالب است، قبل از اعتماد به میانگین، skew را چک می‌کنم.»

**سؤال: «بازه‌های همپوشان را ادغام کن.»** جواب متوسط: «بر اساس شروع مرتب کن، بعد جاروب کن و وقتی بازه‌ی بعدی قبل از پایان آخرین شروع می‌شود، آخرین را گسترش بده.» جواب سنیور: «زمان O(n log n) به خاطر sort. اول روشن می‌کنم بازه‌های مماس ادغام شوند یا نه. می‌گویم همین الگوی sort-and-sweep است که یک سیستم استریم هم برای گروه‌بندی رویدادها در session با timeout فاصله استفاده می‌کند.»

**سؤال: «cycle را در DAG از jobs پیدا کن.»** جواب متوسط: «الگوریتم Kahn: اگر ترتیب توپولوژیک گره‌های کمتری از گراف دارد، cycle وجود دارد.» جواب سنیور: «این هم O(V + E) است. گره‌هایی را که پردازش نشدند گزارش می‌کنم، چون cycle در بین آن‌هاست، و این بررسی را در CI روی تعریف DAG اجرا می‌کنم تا scheduler هرگز آن را نبیند.»

**سؤال: «یک LRU cache پیاده‌سازی کن.»** جواب متوسط: «یک hash map به‌علاوه‌ی لیست پیوندی دوطرفه، یا در پایتون `OrderedDict` برای get و put در O(1).» جواب سنیور: «هر دو O(1) هستند. درباره‌ی thread safety هم می‌گویم: نسخه‌ی پایتون زیر concurrency به lock نیاز دارد، و در throughput بالا یک cache شاردشده را در نظر می‌گیرم تا lock گلوگاه نشود.»

> **🎤 Say this in the interview:** «اول پیچیدگی را می‌گویم، بعد ساختار داده را: hash map برای lookup، heap برای top-K، sort و sweep برای بازه‌ها، الگوریتم Kahn برای ترتیب وابستگی. همچنین می‌گویم الگوریتم کجا دیگر گلوگاه نیست، که در مقیاس بزرگ معمولاً I/O و skew است.»

## ⚠️ اشتباهات رایج

> **⚠️ Watch out:** وقتی فقط top-K لازم است کل داده را sort کنی. sort کامل O(n log n) است؛ heap به اندازه‌ی K برابر O(n log K). برای K = 10 و n = 10⁹ تفاوت مهم است.

- استفاده از `list.count` یا `in` روی لیست داخل حلقه. این یک الگوریتم O(n²) را پشت یک خط ساده پنهان می‌کند.
- فراموش کردن اینکه ترتیب پیمایش dict قبل از پایتون ۳.۷ تضمین نبود. امروز ترتیب درج است، ولی اگر برای درستی به آن تکیه می‌کنی، صریح بگو.
- استفاده از `heapq` به‌عنوان max-heap بدون منفی کردن مقادیر. `heapq` پایتون min-heap است.
- ادغام بازه‌ها بدون sort کردن اول.
- فراموش کردن بررسی cycle در topological sort، که باعث می‌شود حلقه بی‌صدا یک ترتیب ناقص برگرداند.
- فراموش کردن اینکه join روی کلید داغ در Spark مشکل skew است، نه مشکل الگوریتم.
- گفتن «O(n)» بدون گفتن `n` چیست، یا نادیده گرفتن حافظه وقتی داده جا نمی‌شود.

## ✅ خودت را بسنج

۱. زمان شمردن فراوانی `n` آیتم با dictionary چقدر است و چرا؟
   - A) O(n²)، چون هر درج dict را اسکن می‌کند
   - B) O(n)، چون هر درج و به‌روزرسانی به‌طور میانگین O(1) است
   - C) O(log n)، چون dictionaryها مرتب‌اند
   - D) O(1)، چون کار در hash پنهان است

<details><summary>جواب</summary>

**B** — هر به‌روزرسانی dict به‌طور میانگین O(1) است، پس n به‌روزرسانی O(n) هزینه دارد. گزینه‌ی A اسکن لیست را توصیف می‌کند، نه dict؛ گزینه‌ی C یک dict را با ساختار مرتب اشتباه گرفته.

</details>

۲. ۱۰ آیتم پرتکرار را از میان ۵۰۰ میلیون رویداد می‌خواهی، با چند گیگ RAM. کدام رویکرد بهتر است؟
   - A) همه‌ی رویدادها را مرتب کن، بعد ۱۰ تای اول را بردار
   - B) برای هر کلید در یک hash map که در حافظه جا می‌شود بشمار (در صورت نیاز partition کن)، بعد با heap به اندازه‌ی ۱۰ بالاترین‌ها را انتخاب کن
   - C) یک حلقه‌ی تودرتو روی رویدادها بزن
   - D) رویدادها را در لیست بریز و برای هر نامزد `in` صدا بزن

<details><summary>جواب</summary>

**B** — شمردن یک پیمایش است و heap به اندازه‌ی ۱۰ بالاترین‌ها را ارزان انتخاب می‌کند. اگر فضای کلیدهای متمایز در حافظه جا نمی‌شود، اول بر اساس کلید partition کن یا از sketch تقریبی استفاده کن. مرتب کردن ۵۰۰ میلیون رویداد (گزینه‌ی A) خیلی گران‌تر از لازم است.

</details>

۳. با وابستگی‌های `a -> b`، `b -> c`، `c -> a`، یک topological sort چه باید گزارش کند؟
   - A) ترتیب `a, b, c`
   - B) یک cycle، چون هیچ گره‌ای نمی‌تواند اول شروع کند
   - C) ترتیب `c, b, a`
   - D) هیچ؛ topological sort یال‌ها را نادیده می‌گیرد

<details><summary>جواب</summary>

**B** — همه‌ی گره‌ها in-degree برابر ۱ دارند، پس هیچ‌کدام در شروع آماده نیست و الگوریتم Kahn صفر گره پردازش می‌کند. نتیجه تعداد گره‌های کمتری از گراف دارد، یعنی cycle.

</details>

۴. **(سنیور)** ماکسیمم پنجره‌ی لغزان روی یک استریم ۱۰⁹ مقداری با اندازه‌ی پنجره‌ی ۱۰⁴ با sort کردن هر پنجره پیاده شده. مشکل اصلی چیست و چه تغییری می‌دهی؟
   - A) sort در پایتون مجاز نیست
   - B) هر پنجره O(w log w) هزینه دارد و مجموع خیلی کند است؛ از sliding window با یک monotonic deque استفاده کن تا مجموع O(n) شود
   - C) پنجره باید توانی از ۲ باشد
   - D) برای هر پنجره یک heap بساز و از نو بساز

<details><summary>جواب</summary>

**B** — sort مجدد هر پنجره کاری را تکرار می‌کند که تقریباً کاملاً هم‌پوشان است. monotonic deque کاندیدهای ماکسیمم را هنگام لغزیدن پنجره نگه می‌دارد و کل کار O(n) می‌شود. ساختن heap برای هر پنجره (گزینه‌ی D) همان تکرار را دارد.

</details>

۵. **(سنیور)** DAG scheduler یک job روزانه برای ۱۰۰۰ جدول اجرا می‌کند و یک جدول داغ ۸۰٪ زمان را می‌گیرد چون همه‌ی ردیف‌هایش در یک partition افتاده‌اند. چرا تحلیل O(n log n) این را پیش‌بینی نمی‌کند و چه چیزی را چک می‌کنی؟
   - A) تحلیل همیشه درست است؛ جدول داغ باگ داده است
   - B) Big-O فرض می‌کند کار به‌طور یکنواخت پخش شده؛ skew بیشتر کار را روی یک worker می‌گذارد، پس زمان ساعت دیواری تابع بزرگ‌ترین partition است نه میانگین. توزیع کلیدها را چک کن و salting یا broadcast join را در نظر بگیر
   - C) topological sort از ۱۰۰۰ گره پشتیبانی نمی‌کند
   - D) hash map در سیستم‌های توزیع‌شده قابل استفاده نیست

<details><summary>جواب</summary>

**B** — پیچیدگی تعداد کل عملیات را می‌شمارد، ولی یک job موازی وقتی تمام می‌شود که کندترین partition تمام شود. توزیع کلیدها را اندازه بگیر (مثلاً پرتکرارترین کلیدها)، بعد کلید داغ را تکه کن (salting) یا طرف کوچک join را broadcast کن. ببین [[data-skew]].

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| هزینه‌ی lookup در hash map | میانگین O(1)؛ بدترین حالت O(n) با برخورد زیاد |
| Top-K با heap | زمان O(n + d log K) و حافظه‌ی O(d) برای d آیتم متمایز |
| چرا بازه‌ها را اول sort کنیم؟ | تا همپوشان‌ها کنار هم بیفتند؛ بعد یک جاروب ادغامشان می‌کند |
| الگوریتم Kahn در یک جمله | مکرراً گره‌های با in-degree صفر را حذف کن؛ گره‌های باقی‌مانده یعنی cycle |
| LRU cache در پایتون | `OrderedDict` با `move_to_end` هنگام دسترسی و `popitem(last=False)` هنگام سرریز |
| external merge sort | تکه‌های جاشونده را مرتب کن، روی دیسک بنویس، بعد k-way merge با heap |
| ماکسیمم پنجره‌ی لغزان | monotonic deque مجموع O(n) می‌دهد |
| چرا skew «O(1) میانگین» را خراب می‌کند؟ | زمان موازی تابع کندترین partition است؛ یک کلید داغ یک worker را overload می‌کند |

## 🔗 ادامه

- **sql-window-functions** همان ایده‌های رتبه‌بندی و sliding window را در SQL نشان می‌دهد، که در مصاحبه‌های DE هم زیاد امتحان می‌شود.
- **python-core** ساختارهای داخلی (`dict`، `deque`، `heapq`، `OrderedDict`) را که این درس استفاده کرد، با پیچیدگی‌شان توضیح می‌دهد.
- **kafka-architecture** partitionها و کلیدهای داغ را توضیح می‌دهد، که نسخه‌ی استریمی مشکل skew است.
- **airflow** زمان‌بندی DAG از taskها را نشان می‌دهد، که topological sort در production است.
- ابتدا مسئله‌های LeetCode سطح Easy و Medium با برچسب‌های «heap»، «intervals» و «graph» را تمرین کن؛ بیشتر الگوهای این درس را پوشش می‌دهند.
