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
summary: "GIL و اثرش، تفاوت threading، multiprocessing و asyncio، کار I/O-bound در برابر CPU-bound، و مدیریت حافظه."
covers: ["concurrent.futures","asyncio for API ingestion","reference counting & GC"]
interview_qs: []
extras: []
prerequisites: ["python-core"]
related: ["python-production", "python-dataframes", "python-large-files", "api-ingestion", "backpressure", "os-jvm-essentials"]
---

## 🎯 خلاصه

> CPython یک **قفل سراسری مفسر (GIL)** دارد. در عمل، thread وقتی کمک می‌کند که برنامه **منتظر** چیزی باشد (شبکه، دیسک)، process وقتی کمک می‌کند که برنامه **محاسبه** کند (CPU)، و `asyncio` وقتی که **هزاران انتظار** داری و می‌خواهی یک thread همه را مدیریت کند.

- **I/O-bound**: کار بیشترش صرف انتظار برای چیزی بیرون از CPU می‌شود. از thread یا asyncio استفاده کن.
- **CPU-bound**: کار بیشترش محاسبه‌ی پایتونی است. از process (`multiprocessing`) استفاده کن، یا بخش سنگین را به کد C/Rust بسپار (مثل NumPy و Polars).
- **شمارش ارجاع (reference counting)** بیشتر اشیاء را بلافاصله آزاد می‌کند؛ یک **garbage collector چرخه‌ای** حلقه‌های ارجاع را پاک می‌کند.
- بیشتر باگ‌های این حوزه سرعت نیستند؛ مسدود کردن event loop، هم‌زمانی بی‌حد، و کپی شدن اشیاء بزرگ بین processها هستند.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** یک صندوق‌دار (هسته‌ای که bytecode پایتون را اجرا می‌کند) را در یک فروشگاه تصور کن.
> - **Threadها** مثل چند مشتری‌اند که منتظر پیک هستند. وقتی یکی منتظر است، صندوق‌دار به نفر بعدی می‌رسد. وقتی همه منتظرند عالی است.
> - **Processها** مثل چند صندوق‌دارند که هرکدام صندوق خودشان را دارند. هم‌زمان کار می‌کنند، اما هرکدام به نسخه‌ی جداگانه‌ای از موجودی فروشگاه نیاز دارند.
> - **asyncio** یک صندوق‌دار با کلیپ‌بورد است: کاری را شروع می‌کند، وقتی منتظر می‌ماند کنارش می‌گذارد و سراغ کار بعدی می‌رود. فقط وقتی جواب می‌دهد که کسی با محاسبه‌ی طولانی صندوق‌دار را مسدود نکند.

بهترین راه فهمیدنش اندازه‌گیری زمان است. این برنامه هشت صفحه را «دانلود» می‌کند و هر دانلود نیم ثانیه طول می‌کشد:

```python
import time
from concurrent.futures import ThreadPoolExecutor

def fetch(page: int) -> str:
    time.sleep(0.5)  # جای یک فراخوانی شبکه؛ thread دارد منتظر می‌ماند، نه محاسبه
    return f"page {page} done"

start = time.perf_counter()
with ThreadPoolExecutor(max_workers=8) as pool:
    results = list(pool.map(fetch, range(8)))

print(results[0], f"in {time.perf_counter() - start:.1f}s")
# page 0 done in 0.5s   (به صورت ترتیبی ۴ ثانیه طول می‌کشید)
```

هشت انتظار با هم هم‌پوشانی شدند، پس کل زمان تقریباً برابر یک انتظار است. این ایده‌ی اصلی هم‌زمانی برای I/O است.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### GIL

CPython (مفسر استاندارد) از یک **قفل سراسری مفسر** استفاده می‌کند. در هر لحظه فقط یک thread می‌تواند bytecode پایتون را اجرا کند. این قفل آزاد می‌شود:

- وقتی thread منتظر I/O است (سوکت، فایل، `time.sleep`)،
- داخل بسیاری از extensionهای C که کار سنگین انجام می‌دهند بدون دست زدن به اشیاء پایتون (مثلاً عملیات NumPy روی آرایه‌های بزرگ).

پس thread موازی‌سازی واقعی برای «انتظار» می‌دهد، اما برای محاسبه‌ی خالص پایتونی نه. پایتون 3.13 یک build آزمایشی «بدون GIL» (free-threaded، PEP 703) اضافه کرد. قبل از استفاده در production، مستندات نسخه‌ی دقیقت را ببین.

### سه ابزار

| | `threading` / thread pool | `multiprocessing` / process pool | `asyncio` |
|---|---|---|---|
| واحد کار | thread سیستم‌عامل | process سیستم‌عامل | coroutine (`async def`) روی یک event loop |
| بهترین کاربرد | I/O-bound، کتابخانه‌های blocking | محاسبه‌ی CPU-bound با پایتون خالص | فراخوانی‌های شبکه‌ای هم‌زمان زیاد |
| CPU موازی؟ | خیر (GIL) | بله | خیر (GIL) |
| حافظه‌ی مشترک | بله (بدون lock خطرناک است) | خیر؛ داده pickle و کپی می‌شود | بله، یک thread |
| هزینه‌ی هر واحد | کم (چند MB پشته) | زیاد (process جدید، pickle) | خیلی کم |
| حالت خرابی | race condition، deadlock | کپی حافظه، شروع کند | یک فراخوانی blocking همه را متوقف می‌کند |

### کار CPU-bound به process نیاز دارد

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

if __name__ == "__main__":  # برای process pool لازم است (روش spawn)
    with ProcessPoolExecutor() as pool:
        totals = list(pool.map(count_primes, [200_000] * 4))
    print(totals)  # [17984, 17984, 17984, 17984]
```

همین لیست را با `ThreadPoolExecutor` اجرا کن: زمان کل تقریباً برابر یک اجرای ترتیبی است، چون threadها نوبتی از GIL استفاده می‌کنند.

### asyncio برای فراخوانی‌های شبکه‌ای زیاد

`asyncio` یک event loop اجرا می‌کند. هر coroutine در هر `await` کنترل را پس می‌دهد. یک **semaphore** تعداد درخواست‌های در پرواز را محدود می‌کند؛ این همان کاری است که باعث می‌شود با API مؤدب بمانی و زیر محدودیت نرخ (rate limit) باشی:

```python
import asyncio

async def fetch(sem: asyncio.Semaphore, i: int) -> str:
    async with sem:                  # حداکثر ۳ درخواست در پرواز
        await asyncio.sleep(0.5)     # جای: await http_client.get(...)
        return f"ok {i}"

async def main() -> None:
    sem = asyncio.Semaphore(3)
    results = await asyncio.gather(*(fetch(sem, i) for i in range(9)))
    print(len(results), results[-1])  # 9 ok 8   (حدود ۱.۵ ثانیه: سه موج سه‌تایی)

asyncio.run(main())
```

اگر مجبوری از کد async یک تابع **blocking** را صدا بزنی (یک درایور دیتابیس قدیمی، خواندن فایل)، با `await asyncio.to_thread(blocking_fn, arg)` (از پایتون 3.9) آن را به یک thread ببر. صدا زدنش مستقیم، بقیه‌ی taskها را یخ می‌زند.

### حافظه: شمارش ارجاع و GC چرخه‌ای

هر شیء CPython یک شمارنده‌ی ارجاع دارد. وقتی به صفر برسد، حافظه بلافاصله آزاد می‌شود:

```python
import sys

a = []
print(sys.getrefcount(a))  # معمولاً ۲: نام `a` به‌اضافه‌ی آرگومان تابع
b = a
print(sys.getrefcount(a))  # یکی بیشتر
del b
```

شمارش ارجاع نمی‌تواند **چرخه‌ها** (اشیائی که به هم ارجاع دارند) را آزاد کند. garbage collector چرخه‌ای آن‌ها را پیدا می‌کند:

```python
import gc

class Node:
    def __init__(self) -> None:
        self.other: "Node | None" = None

x, y = Node(), Node()
x.other, y.other = y, x   # یک چرخه
del x, y                  # شمارنده‌ها به صفر نمی‌رسند...
print(gc.collect())       # ...ولی collector پیدایشان می‌کند و آزادشان می‌کند (> 0)
```

دو پیامد عملی: `del` تضمین نمی‌کند حافظه به سیستم‌عامل برگردد (allocator پایتون ممکن است آن را برای استفاده‌ی مجدد نگه دارد)، و فرایندی که لیست بزرگی از اشیاء ریز نگه داشته، حتی بعد از حذف بیشترشان می‌تواند بزرگ بماند.

process pool در Linux در نسخه‌های قدیمی‌تر پایتون به‌طور پیش‌فرض از `fork` استفاده می‌کرد، و فرزندان حافظه را به‌صورت copy-on-write با والد شریک می‌شدند. اما شمارش ارجاع به هر شیئی که می‌خواند **می‌نویسد**، و این یعنی صفحات حافظه عملاً کپی می‌شوند. پس ساختار بزرگ و فقط‌خواندنی پایتون که به workerها داده شود، برای هر process تکرار می‌شود. آرایه‌های NumPy، جدول‌های Arrow یا مسیر فایل را ترجیح بده.

## 🧠 عمق و trade-offها (سنیور)

- **جایی که thread هنوز برنده است.** خیلی از کلاینت‌های داده‌ی واقعی (`requests`، درایورهای دیتابیس با C، `boto3`) هنگام انتظار GIL را آزاد می‌کنند. یک thread pool با ۱۶ تا ۶۴ worker اغلب ساده‌ترین راه درست برای ingestion است، وقتی کتابخانه‌ها synchronous هستند.
- **حالت خرابی asyncio مسدودسازی است، نه سرعت.** یک `requests.get` همگام یا یک حلقه‌ی CPU داخل coroutine، همه‌ی درخواست‌های دیگر را متوقف می‌کند. تأخیر event loop را در متریک‌ها رصد کن و کار CPU را از loop دور نگه دار.
- **Backpressure یک تصمیم هم‌زمانی است.** `asyncio.gather` بی‌حد روی یک میلیون URL یعنی یک میلیون task و اوج حافظه. از semaphore، صف محدود (`asyncio.Queue(maxsize=...)`) یا chunk استفاده کن. به [[backpressure]] نگاه کن.
- **Process یک تصمیم حافظه و serialization است.** pickle کردن یک DataFrame دو گیگابایتی برای ارسال به worker اغلب کندتر از خود محاسبه است. مسیر فایل یا کلید partition بده تا هر worker بخش خودش را بخواند، یا از موتور ستونی استفاده کن که خودش موازی‌سازی می‌کند.
- **موتورها خودشان موازی‌سازی می‌کنند.** Spark، Polars، DuckDB و Dask کار را در کد native توزیع و multi-thread می‌کنند. اگر حلقه‌ی پایتونی‌ات گلوگاه است، اول بپرس آیا یک عبارت برداری یا SQL آن را حذف می‌کند.
- **retry هم‌زمانی را ضرب می‌کند.** هشت worker که هرکدام سه بار بدون jitter دوباره تلاش می‌کنند، سرویس در حال مشکل را موج‌وار زیر فشار می‌گذارند. محدودیت هم‌زمانی را با backoff ترکیب کن ([[retry]]).
- **چه چیزی را رصد کنی.** طول صف workerها، صدک‌های تأخیر taskها، تأخیر event loop، RSS هر worker و تعداد اتصال‌های باز. pool اشباع با صف کوتاه یعنی هم‌زمانی کم است؛ صف رو به رشد با CPUهای بیکار یعنی فراخوانی blocking.
- **کی اصلاً هم‌زمانی نمی‌خواهی.** اگر یک job چهار دقیقه‌ای روزی یک بار اجرا می‌شود، یک job ترتیبی ساده با retry بهتر از یک طراحی async باهوش قابل‌اعتمادتر است.

## 🧪 امتحان کن

1. مثال `fetch` را با `ProcessPoolExecutor` به‌جای thread اجرا کن. قبل از اجرا زمان کل را پیش‌بینی کن. بعد نتیجه را توضیح بده (راهنمایی: شروع process و GIL هیچ‌کدام اینجا مهم نیستند، چون `sleep` از CPU استفاده نمی‌کند).
2. داخل `fetch` یک حلقه با یک میلیون جمع اضافه کن و هر دو pool را دوباره اجرا کن. کدام‌یک از worker بیشتر سود می‌برد، و چرا؟
3. مثال asyncio را طوری تغییر بده که یکی از coroutineها به‌جای `await asyncio.sleep(2)` از `time.sleep(2)` استفاده کند. رفتار بقیه‌ی coroutineها را پیش‌بینی کن، بعد بررسی کن.

<details><summary>راه‌حل‌ها را نشان بده</summary>

**۱.** با `ProcessPoolExecutor(max_workers=8)` کل زمان هنوز حدود ۰.۵ ثانیه به‌علاوه‌ی شروع process (اغلب ۰.۱ تا ۰.۵ ثانیه‌ی دیگر) است. processها کار می‌کنند، اما شروع و جابه‌جایی داده‌شان هزینه دارد، و `sleep` به CPU نیاز ندارد؛ پس سودی ندارد.

**۲.** با حلقه‌ی پایتون خالص، thread pool سرعتی نمی‌گیرد (threadها یک GIL مشترک دارند)، در حالی که process pool با تعداد هسته‌های CPU مقیاس می‌گیرد. این تقسیم CPU-bound در برابر I/O-bound در یک آزمایش است.

**۳.** تابع `time.sleep(2)` کل event loop را مسدود می‌کند: تا برنگردد هیچ coroutine دیگری اجرا نمی‌شود، پس کل اجرا طولانی‌تر می‌شود و بقیه دیرتر تمام می‌شوند. از `await asyncio.sleep(2)` یا `await asyncio.to_thread(time.sleep, 2)` استفاده کن.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «GIL چیست، و آیا threadهای پایتون را بی‌فایده می‌کند؟»**
جواب متوسط: «GIL اجازه می‌دهد در هر لحظه فقط یک thread bytecode پایتون اجرا کند. threadها هنوز برای I/O مفیدند، چون قفل وقتی منتظری آزاد می‌شود.»
جواب سنیور: «GIL در CPython باعث می‌شود bytecode پایتون ترتیبی اجرا شود، پس threadها برای I/O هم‌زمانی می‌دهند اما محاسبه‌ی موازی نه. برای CPU-bound از process یا کتابخانه‌های native استفاده می‌کنم. یادم هست که processها داده را کپی می‌کنند، پس partition یا مسیر فایل می‌دهم، نه اشیاء بزرگ. نسخه‌ی پایتون را هم چک می‌کنم، چون build بدون GIL این را برای نسخه‌های جدیدتر عوض می‌کند.»

**سؤال: «برای ingestion کی asyncio را به thread pool ترجیح می‌دهی؟»**
متوسط: «وقتی فراخوانی‌های شبکه‌ای هم‌زمان زیاد باشد.»
سنیور: «وقتی کلاینت async-native است و به هزاران اتصال هم‌زمان با حافظه‌ی کم نیاز دارم. هم‌زمانی را با semaphore محدود می‌کنم، هیچ کد blocking داخل loop نمی‌گذارم و تأخیر loop را رصد می‌کنم. اگر کلاینت synchronous باشد، یک thread pool محدود ساده‌تر است و به همان سرعت می‌رسد.»

> **🎤 این را در مصاحبه بگو:** «thread برای انتظار، process برای محاسبه، و asyncio برای انتظارهای زیاد با یک thread. دلیلش GIL است، و اولین خرابی‌ای که دنبالش می‌گردم کد blocking در جایی است که نباید باشد.»

## ⚠️ اشتباهات رایج

> **⚠️ هشدار:** صدا زدن تابع blocking (`requests.get`، فراخوانی همگام دیتابیس، `time.sleep`) داخل `async def` کل event loop را مسدود می‌کند. از کلاینت async استفاده کن، یا `asyncio.to_thread`.

> **⚠️ هشدار:** process pool بدون `if __name__ == "__main__":` می‌تواند ماژول را در هر فرزند دوباره اجرا کند (روی سیستم‌هایی با روش `spawn`، مثل ویندوز و macOS) و خطا بدهد.

- انتظار داشتن که thread محاسبه‌ی حسابی خالص پایتون را سریع کند.
- ساختن تعداد نامحدودی task یا thread، و در نتیجه تمام شدن حافظه یا file descriptor.
- به اشتراک گذاشتن وضعیت قابل تغییر بین threadها بدون `Lock`، و بعد دیباگ کردن یک race condition نادر.
- فرستادن DataFrameهای خیلی بزرگ به workerهای process به‌جای مسیر یا partition.
- فرض کردن اینکه `del big_list` فوراً مصرف حافظه‌ی process را کم می‌کند.
- گرفتن همه‌ی خطاها در worker و از دست دادن بی‌صدای taskهای خراب. خطاها را جمع کن و دوباره raise کن، یا شمارش کن.

## ✅ خودت را بسنج

۱. یک job ده هزار فایل JSON کوچک را از HTTP دانلود می‌کند. کدام روش مناسب‌تر است؟
   - A) یک process pool با یک process برای هر فایل
   - B) یک thread pool محدود یا asyncio با semaphore
   - C) یک thread تنها با timeout بلندتر
   - D) regex پایتون خالص روی بدنه‌ها در threadهای جدا

<details><summary>جواب</summary>

**B** — کار I/O-bound است، پس threadها یا asyncio انتظارها را هم‌پوشانی می‌کنند. محدود کردن هم‌زمانی حافظه را پایدار و job را مؤدب نگه می‌دارد. process برای هر فایل (A) گران و بی‌دلیل است.

</details>

۲. چرا یک `ThreadPoolExecutor` با ۸ worker معمولاً جستجوی اعداد اول با پایتون خالص را سریع‌تر نمی‌کند؟
   - A) threadها نمی‌توانند تابع صدا بزنند
   - B) GIL اجازه می‌دهد در هر لحظه فقط یک thread bytecode پایتون اجرا کند
   - C) سیستم‌عامل تعداد threadها را به یکی per core محدود می‌کند
   - D) جستجوی اعداد اول به `await` نیاز دارد

<details><summary>جواب</summary>

**B** — کد CPU-bound پایتون نوبتی از GIL استفاده می‌کند، پس موازی‌سازی سرعتی نمی‌دهد. از `ProcessPoolExecutor` یا کتابخانه‌ی native استفاده کن.

</details>

۳. اگر یک coroutine وسط کار `time.sleep(5)` صدا بزند، در حالی که نه coroutine دیگر منتظر `await asyncio.sleep(1)` هستند، چه می‌شود؟
   - A) هیچ؛ `time.sleep` همکاری‌جو (cooperative) است
   - B) بقیه حدود ۵ ثانیه مسدودند، چون event loop یخ کرده است
   - C) coroutine خوابیده خودکار به thread دیگری می‌رود
   - D) asyncio یک `TimeoutError` می‌دهد

<details><summary>جواب</summary>

**B** — `time.sleep` یک فراخوانی blocking است. thread تنهای event loop داخلش منتظر می‌ماند و هیچ taskی نمی‌تواند اجرا شود. از `await asyncio.sleep(5)` استفاده کن.

</details>

۴. **(سنیور)** یک `asyncio.gather` روی ۲ میلیون URL اجرا می‌کنی و بعد از چند دقیقه کانتینر به‌خاطر کمبود حافظه کشته می‌شود. محتمل‌ترین علت چیست و چطور رفعش می‌کنی؟

   - A) `gather` بیش از ۱۰۰۰ task را نمی‌تواند؛ از `threading` استفاده کن
   - B) همه‌ی taskها و نتایج معلقشان یک‌جا ساخته می‌شوند، پس حافظه بی‌حد رشد می‌کند؛ کارِ در پرواز را با semaphore محدود کن، یا URLها را chunk کن / با صف محدود پردازش کن
   - C) asyncio روی هر `await` حافظه نشت می‌کند؛ به `multiprocessing` برو
   - D) راه‌حل، CPU بیشتر است

<details><summary>جواب</summary>

**B** — `gather` همه‌ی coroutineها را فوراً زمان‌بندی می‌کند، پس اشیاء task و نتایجشان با هم در حافظه می‌مانند. هم‌زمانی را با semaphore محدود کن یا یک صف محدود تغذیه کن، و نتایج را در حین کار بنویس.

</details>

۵. **(سنیور)** یک pool کارگر یک دیکشنری فقط‌خواندنی ۳ گیگابایتی (یک `dict` پایتون با میلیون‌ها رشته) را می‌خواند و هشت process fork می‌کند. مصرف حافظه به هشت کپی می‌رسد، با اینکه فرزندان فقط می‌خوانند. چرا، و چه کار می‌کنی؟

   - A) `fork` همیشه حافظه را کپی می‌کند، پس راه‌حل thread است
   - B) شمارش ارجاع هنگام خواندن هدر اشیاء را می‌نویسد (و صفحه‌ها را کپی می‌کند)؛ از ساختار فشرده و بدون شمارش ارجاع مثل آرایه‌ی NumPy یا جدول Arrow استفاده کن، یا یک فایل را memory-map کن و بگذار workerها بخش‌هایی از آن را بخوانند
   - C) دیکشنری باید pickle شود، پس از `json` استفاده کن
   - D) این روی Linux اتفاق نمی‌افتد

<details><summary>جواب</summary>

**B** — اشتراک copy-on-write با اشیاء CPython می‌شکند، چون هر خواندن شمارنده‌ی ارجاع را به‌روز می‌کند و صفحه‌ها کثیف می‌شوند. ذخیره‌ی داده در قالب تخت و فقط‌خواندنی این نوشتن‌ها را حذف می‌کند.

</details>

## 🗂️ فلش‌کارت

| جلوی کارت | پشت کارت |
|---|---|
| GIL چیست؟ | قفل CPython که اجازه می‌دهد در هر لحظه فقط یک thread bytecode پایتون اجرا کند |
| آیا GIL هنگام I/O آزاد می‌شود؟ | بله، وقتی thread منتظر سوکت، فایل یا `sleep` است |
| بهترین ابزار برای CPU-bound با پایتون خالص | `multiprocessing` / `ProcessPoolExecutor` |
| بهترین ابزار برای هزاران انتظار شبکه‌ای هم‌زمان | `asyncio` با semaphore محدود |
| چه چیزی event loop را مسدود می‌کند؟ | هر فراخوانی همگامی که منتظر بماند یا محاسبه کند (از `asyncio.to_thread` استفاده کن) |
| چرا شمارش ارجاع با fork شریک‌سازی را خراب می‌کند؟ | خواندن اشیاء، شمارنده‌ها را به‌روز می‌کند و صفحات حافظه کپی می‌شوند |
| چه چیزی چرخه‌های ارجاع را آزاد می‌کند؟ | garbage collector چرخه‌ای (`gc`) |
| چرا برای process pool از `if __name__ == "__main__":` استفاده می‌کنیم؟ | فرزندان ممکن است ماژول را دوباره import کنند (روش spawn) |

## 🔗 ادامه

- **python-core** ساختار اشیاء پایتون را پوشش می‌دهد، که پایه‌ی شمارش ارجاع است.
- **python-large-files** نشان می‌دهد چطور داده را chunk به chunk بخوانی، که جایگزین بسیاری از طراحی‌های «همه را لود کن و بعد موازی کن» است.
- **api-ingestion** الگوهای semaphore و retry را روی pipelineهای واقعی API اعمال می‌کند.
- **backpressure** توضیح می‌دهد چرا صف‌های محدود از تعداد خام worker مهم‌ترند.
- بخش‌های `concurrent.futures` و `asyncio` مستندات کتابخانه‌ی استاندارد نسخه‌ی پایتون خودت را بخوان؛ پیش‌فرض‌های روش شروع process بین نسخه‌ها عوض شده‌اند.
