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
summary: "چون Spark، Kafka، Flink و Hadoop روی JVM اجرا می‌شوند، باید heap، GC، off-heap و page cache را بفهمی. Kafka سرعتش را مدیون page cache و zero-copy است."
covers: ["processes vs threads","virtual memory","page cache","sequential vs random I/O","mmap","zero-copy (sendfile)","JVM heap & GC (G1, ZGC)","GC pauses in long-running jobs"]
interview_qs: []
extras: []
prerequisites: ["linux-shell"]
related: ["kafka-architecture","spark-memory-tuning","spark-architecture","networking-essentials"]
---
## 🎯 خلاصه

> Spark، Kafka، Flink و Hadoop **process**هایی هستند که روی **JVM** اجرا می‌شوند و فایل‌ها را از طریق **سیستم‌عامل** می‌خوانند و می‌نویسند. سرعت و توقف‌هایشان از سه لایه می‌آید: سیستم‌عامل (thread، حافظه‌ی مجازی، page cache و I/O)، JVM (heap، garbage collection و حافظه‌ی off-heap)، و مسیر داده بین آن‌ها.

- یک **process** فضای آدرس مخصوص خودش را دارد؛ **threadها** آن فضا را شریک‌اند. یک JVM یک process است با threadهای زیاد.
- **page cache** یعنی سیستم‌عامل RAM آزاد را برای نگه داشتن داده‌ی فایل استفاده می‌کند. سرعت Kafka به آن وابسته است.
- I/O **ترتیبی (sequential)** خیلی سریع‌تر از I/O **تصادفی (random)** است، روی دیسک و حتی روی SSD.
- **zero-copy** (`sendfile`) بایت‌ها را از page cache مستقیم به socket می‌فرستد، بدون کپی در فضای کاربر.
- **heap ی JVM** با یک garbage collector مدیریت می‌شود. توقف‌های طولانی GC همه‌ی کارهای JVM را متوقف می‌کند، و این به شکل lag، timeout و taskهای کند دیده می‌شود.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** یک دفتر کار را تصور کن. یک **process** کل دفتر است، با اتاق‌ها و پرونده‌های قفل‌دار خودش. **threadها** کارمندان همان دفترند که میز و پرونده‌ها را شریک‌اند. **page cache** یک تخته‌سفید است که سیستم‌عامل نزدیک در نگه می‌دارد: اسناد پرمصرف روی آن می‌مانند، پس هر بار کسی تا بایگانی پیاده نمی‌شود. **JVM** دفتری است با نظافتچی (garbage collector) که گاهی همه را چند ثانیه نگه می‌دارد تا مرتب کند.

بدون این‌که متوجه باشی، این مدل را هر روز استفاده می‌کنی:

- وقتی یک job در Spark اجرا می‌کنی، driver و هر executor **process**های جدا هستند.
- داخل یک executor، **threadهای** زیادی همزمان taskها را اجرا می‌کنند.
- وقتی یک فایل را دو بار می‌خوانی، بار دوم اغلب خیلی سریع‌تر است، چون سیستم‌عامل داده را در RAM نگه داشته.
- وقتی یک job بزرگ Spark هر دقیقه چند ثانیه کند می‌شود و بعد برمی‌گردد، یک **توقف GC** دلیل رایجی است.

یک قاعده‌ی ساده برای مبتدی: **سریع‌ترین I/O آن است که هرگز به دیسک نمی‌رسد**، و بدترین توقف‌ها آن‌هایی هستند که کل JVM متوقف می‌شود.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### Process، thread و حافظه‌ی مجازی

هر process یک **فضای آدرس مجازی** می‌گیرد. CPU و سیستم‌عامل آدرس‌های مجازی را به RAM فیزیکی در **صفحه‌ها (page)** نگاشت می‌کنند، که معمولاً ۴ کیلوبایت در Linux روی x86 است. صفحه‌ها تنبلانه بار می‌شوند: اولین دسترسی یک **page fault** ایجاد می‌کند و سیستم‌عامل صفحه را از دیسک پر یا تخصیص می‌دهد.

- threadهای یک process فضای آدرس را شریک‌اند، پس اشیای heap را بدون کپی به اشتراک می‌گذارند. به همین دلیل یک JVM صدها task را ارزان اجرا می‌کند، و همین باعث می‌شود یک باگ بتواند وضعیت مشترک را خراب کند.
- ساختن process در مقایسه با thread گران است و هر context switch هزینه دارد. برای همین بسیاری از سیستم‌های داده، threadها را داخل چند process بزرگ اجرا می‌کنند.
- **RSS** (resident set size) بخشی از فضای آدرس است که در RAM است. یک process می‌تواند حجم مجازی بزرگ و RSS کوچک داشته باشد.

### page cache

وقتی یک فایل را می‌خوانی، هسته‌ی سیستم‌عامل صفحه‌ها را در RAM نگه می‌دارد. خواندن بعدی همان بازه از حافظه سرو می‌شود. وقتی می‌نویسی، نوشتن‌ها در page cache بافر می‌شوند و بعداً (یا با `fsync`) روی دیسک می‌روند.

اسکریپت کوتاه پایتون زیر این اثر را نشان می‌دهد. یک فایل ۲۵۶ مگابایتی می‌نویسد، از کرنل می‌خواهد صفحه‌های کش‌شده‌اش را پاک کند، و بعد بلوک‌های ۴ کیلوبایتی تصادفی را سرد و گرم می‌خواند:

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

روی یک لپ‌تاپ معمولی Linux، خواندن تصادفی سرد چند برابر کندتر از ترتیبی سرد است، و دور دوم گرم باز هم خیلی سریع‌تر. عددهای دقیق به دیسک و RAM تو بستگی دارند، پس الگو را ببین، نه ارقام را.

### I/O ترتیبی در برابر تصادفی

هد دیسک (HDD) یا کانال flash یک SSD بهترین کارایی را با خواندن و نوشتن‌های بلند و پیوسته دارد. خواندن‌های تصادفی ۴ کیلوبایتی هزینه‌ی هر درخواست را می‌دهند. قاعده‌ی مهندسی داده همان قاعده‌ی اسکریپت است: **داده را طوری سازمان بده که موتور بازه‌های بزرگ و پیوسته را بخواند.** فایل‌های ستونی، row groupهای بزرگ و نوشتن‌های log-structured همگی از این قاعده پیروی می‌کنند. Kafka هر partition را به‌صورت یک log فقط‌الحاقی (append-only) می‌نویسد، و به همین دلیل سریع است.

### mmap

`mmap` یک فایل را در فضای آدرس نگاشت می‌کند. خواندن یک بایت از نگاشت، صفحه‌ی آن را از طریق page cache بار می‌کند، بدون فراخوانی صریح `read`. راحت است و چند موتور ذخیره‌سازی و JVM در بعضی دسترسی‌های فایل از آن استفاده می‌کنند. هزینه‌اش این است که page faultها و سیاست حذف cache حالا دست سیستم‌عامل است، پس فشار حافظه می‌تواند جهش‌های latency ایجاد کند که شبیه کندی تصادفی به نظر می‌رسند.

### zero-copy با sendfile

ارسال ساده‌ی شبکه داده را کپی می‌کند: دیسک به page cache، page cache به بافر کاربر، بافر کاربر به بافر socket، و بعد به NIC. `sendfile` به کرنل اجازه می‌دهد صفحه‌های page cache را مستقیم به socket بفرستد و کپی‌های فضای کاربر را حذف کند. Kafka این را برای fetch مصرف‌کننده‌ها روی داده‌ی بدون رمز استفاده می‌کند، از طریق فراخوانی JVM به نام `FileChannel.transferTo`. این یکی از دلایلی است که Kafka با CPU کم و throughput بالا از page cache سرو می‌کند.

trade-off: وقتی TLS فعال است، بایت‌ها باید در فضای کاربر رمز شوند، پس مسیر ساده‌ی zero-copy به همان شکل قابل استفاده نیست. ظرفیت شبکه و CPU را با این در نظر بگیر.

### heap ی JVM و garbage collection

JVM اشیا را روی **heap** تخصیص می‌دهد و با یک **garbage collector** (GC) آزاد می‌کند. کار آن پیدا کردن اشیای غیرقابل دسترس و بازپس‌گیری حافظه‌شان است. collectorهای اصلی که خواهی دید:

- **G1** (collector پیش‌فرض در JDKهای جدید). heap را به region تقسیم می‌کند و با علامت‌گذاری همزمان و evacuation تدریجی، هدف زمان توقف را دنبال می‌کند.
- **ZGC**. برای توقف‌های خیلی کوتاه حتی با heapهای بزرگ طراحی شده، و بیشتر کارش همزمان با threadهای برنامه اجرا می‌شود.

توقف‌های stop-the-world در همه‌ی collectorها اتفاق می‌افتند. با collector و اندازه‌ی heap مناسب کوتاه‌تر می‌شوند، ولی هرگز کاملاً حذف نمی‌شوند. یک توقف در executor یا broker، threadهای آن را متوقف می‌کند، پس taskها گیر کرده به نظر می‌رسند، heartbeatها از دست می‌روند، و گاهی coordinator گره را مرده اعلام می‌کند.

گزینه‌های مفید JVM (نام‌ها و پیش‌فرض‌ها را برای نسخه‌ی JDK خودت بررسی کن):

```bash
# Spark executor example: fixed heap, G1 collector, GC logging
spark-submit \
  --conf spark.executor.memory=8g \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -Xlog:gc*:file=/var/log/executor-gc.log:time,uptime,level,tags" \
  my_job.py
```

در سرورها `-Xms` را برابر `-Xmx` بگذار تا heap تغییر اندازه ندهد، و قبل از هر تنظیمی لاگ GC را بخوان.

### حافظه‌ی off-heap

همه‌ی بایت‌ها روی heap نیستند. بافرهای مستقیم (NIO، Netty)، حافظه‌ی unified در Spark وقتی off-heap فعال است، و کتابخانه‌های native، حافظه‌ی **off-heap** تخصیص می‌دهند که garbage collector مدیریتش نمی‌کند. حافظه‌ی off-heap به سقف container یا ماشین حساب می‌شود، نه به `-Xmx`. executorی که بیرون heap کم بیاورد توسط runtime کانتینر کشته می‌شود، و لاگ‌های JVM چیز غلطی نشان نمی‌دهند. هر دو را حساب کن.

heapهای بالاتر از حدود ۳۲ گیگابایت اشاره‌گرهای فشرده‌ی شیء را از دست می‌دهند، پس یک heap ۳۴ گیگابایتی ممکن است شیء کمتری از یک heap ۳۰ گیگابایتی جا دهد. بزرگ‌ترین heapی را استفاده کن که اشاره‌گرها هنوز فشرده بمانند، مگر این‌که دلیل اندازه‌گیری‌شده‌ای برای غیر آن داشته باشی.

## 🧠 عمق و trade-offها (سنیور)

- **heap در برابر page cache.** brokerهای Kafka heap را متوسط نگه می‌دارند و بیشتر RAM را به page cache می‌دهند، چون cache داده‌ی log را نگه می‌دارد که مصرف‌کننده‌ها می‌خوانند. روی یک میزبان اشتراکی، اگر page cache کوچک باشد، داده‌ی داغ جا نمی‌شود و هر خواندن مصرف‌کننده به دیسک می‌رود. RAM را برای working set حساب کن، نه فقط برای heap.
- **executorهای Spark.** Spark حافظه‌ی executor را بین execution، storage و overhead تقسیم می‌کند. داده‌ی cache شده، بافرهای shuffle و بافرهای off-heap همگی با heap رقابت می‌کنند. jobی که به دیسک spill می‌کند یا GC طولانی دارد، اغلب مشکل پیکربندی حافظه است، نه مشکل کد. [[spark-memory-tuning]] را ببین.
- **بودجه‌ی توقف.** برای یک job streaming، توقف ۲ ثانیه‌ای GC می‌تواند از هدف latency یا timeout جلسه بگذرد. زمان توقف‌ها را با لاگ GC اندازه بگیر، و collector و اندازه‌ی heap را بر اساس بودجه‌ی latency انتخاب کن. برای batch، توقف‌های طولانی G1 روی heap بزرگ معمولاً یعنی اشیای زنده‌ی زیاد، که مشکل کد است (cache کردن مجموعه‌های بزرگ، ساخت شیء برای هر سطر).
- **تعداد شیء و سریالایز.** هزینه‌ی JVM با تعداد اشیای زنده تعیین می‌شود. یک `HashMap` از `Long`های boxed برای هر سطر میلیون‌ها شیء و کار GC می‌سازد. نمایش ستونی یا باینری (مثل Tungsten در Spark و بافرهای Arrow) تعداد اشیا را کم و بزرگ نگه می‌دارد، و به همین دلیل سریع‌اند.
- **کانتینر.** در Kubernetes یا YARN، JVM باید سقف حافظه‌ی کانتینر را ببیند. heap را کسری از سقف کانتینر تنظیم کن و جا برای off-heap و سیستم‌عامل بگذار. OOM kill با heap کوچک معمولاً یعنی رشد off-heap.
- **fsync و دوام.** نوشتن وقتی برمی‌گردد که page cache آن را نگه داشته باشد، مگر کد `fsync` صدا بزند یا producer منتظر replication بماند. قبل از قول دادن دوام، معنی «نوشتن موفق» در سیستمت را بدان.
- **پایش.** زمان و تعداد توقف‌های GC، مصرف heap و off-heap، page faultها (به‌خصوص major)، throughput خواندن دیسک و iowait، و الگوی cache hit مصرف‌کننده‌ها را دنبال کن. جهش در major page faultها اغلب قبل از یک incident تأخیر می‌آید.

## 🧪 امتحان کن

از Linux با Python نسخه‌ی ۳.۱۰ به بعد استفاده کن. اسکریپت بخش متوسط حدود ۳۰۰ مگابایت فضای خالی دیسک لازم دارد.

1. **اندازه‌گیری cache.** اسکریپت را دو بار اجرا کن. تفاوت «تصادفی سرد» و «تصادفی گرم» را در یک جمله برای هر کدام توضیح بده.
2. **ترتیبی در برابر تصادفی.** `N_READS` را به ۱۰۰٬۰۰۰ تغییر بده و زمان ترتیبی سرد و تصادفی سرد را مقایسه کن. نسبت را یادداشت کن و توضیح بده این نسبت برای مصرف‌کننده‌ی Kafka که یک partition را از ابتدا می‌خواند در برابر مصرف‌کننده‌ای که به offsetهای تصادفی می‌رود، چه معنایی دارد.
3. **خواندن لاگ GC.** هر برنامه‌ی کوچک JVM (یا یک job محلی Spark) را با `-Xlog:gc` اجرا کن، و در خروجی یک young collection و یک collection کامل یا mixed پیدا کن. زمان توقف‌هایشان را یادداشت کن.

<details><summary>جواب‌ها را نشان بده</summary>

**۱.** خواندن‌های تصادفی سرد از page cache رد می‌شوند، پس هر خواندن به دیسک می‌رود. خواندن‌های تصادفی گرم به RAM می‌خورند، پس دور دوم از I/O دیسک دوری می‌کند.

**۲.** نسبت معمولاً روی دیسک زیاد و روی SSD هم محسوس است. مصرف‌کننده‌ای که از ابتدا ترتیبی می‌خواند از read-ahead و segmentهای بزرگ و پیوسته‌ی log بهره می‌برد. مصرف‌کننده‌ای که به offsetهای تصادفی می‌رود این مزیت را از دست می‌دهد، و اگر داده در page cache نباشد، هر fetch یک seek دیسک هزینه دارد.

**۳.** جواب به اجرای تو بستگی دارد. یک young collection باید کوتاه و پرتکرار باشد؛ یک mixed یا full collection طولانی‌تر و کم‌تکرارتر است. اگر full collectionها زیاد اتفاق می‌افتند، اندازه‌ی داده‌ی زنده و نرخ تخصیص شیء را بررسی کن.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «با این‌که Kafka همه‌چیز را روی دیسک می‌نویسد، چرا سریع است؟»**
جواب متوسط: «به‌صورت ترتیبی در log فقط‌الحاقی می‌نویسد و مصرف‌کننده‌ها از page cache سیستم‌عامل می‌خوانند. zero-copy با `sendfile` بایت‌ها را بدون کپی در JVM به socket می‌فرستد.»
جواب سنیور: «سه دلیل دارد. الحاق ترتیبی، نوشتن‌های تصادفی را به جریانی تبدیل می‌کند که دیسک خوب مدیریتش می‌کند. خواندن‌ها از page cache سرو می‌شوند، پس داده‌ی داغ هرگز RAM را ترک نمی‌کند، و به همین دلیل heap بروکر کوچک می‌ماند. fetch هم وقتی TLS خاموش است zero-copy است، پس بروکر CPU کمی به هر بایت می‌دهد. ریسک‌ها حذف شدن page cache زیر فشار حافظه و TLS هستند که مسیر zero-copy را برمی‌دارد.»

**سؤال: «یک job در Spark هر دقیقه چند ثانیه کند می‌شود. کجا را نگاه می‌کنی؟»**
جواب متوسط: «لاگ GC executorها را برای توقف‌های طولانی نگاه می‌کنم.»
جواب سنیور: «توقف‌های لاگ GC را با زمان‌بندی stageها تطبیق می‌دهم، بررسی می‌کنم executorها نزدیک سقف حافظه (heap یا off-heap) هستند یا نه، و دنبال اشیای cache شده‌ی بزرگ یا partitionهای نامتوازن می‌گردم که مجموعه‌ی زنده‌ی بزرگ می‌سازند. بعد collector و اندازه‌ی heap را بر اساس هدف latency انتخاب می‌کنم، و اگر علت تعداد شیء است، آن را در کد اصلاح می‌کنم.»

> **🎤 این را در مصاحبه بگو:** «سیستم‌های داده روی JVM با سه چیز محدود می‌شوند: این‌که سیستم‌عامل بایت‌ها را چطور جابه‌جا می‌کند، چقدر در RAM کش شده، و garbage collector چقدر process را متوقف می‌کند. اول الگوی I/O را نگاه می‌کنم، بعد page cache، و بعد لاگ GC.»

## ⚠️ اشتباهات رایج

> **⚠️ توجه:** یک توقف GC برنامه‌ی threadها را متوقف می‌کند. taskی که «کند» است ولی CPU مصرف نمی‌کند ممکن است منتظر توقف باشد، نه منتظر I/O.

> **⚠️ توجه:** تنظیم `-Xmx` تقریباً به کل ماشین، جایی برای off-heap، سیستم‌عامل یا page cache باقی نمی‌گذارد. آن‌وقت process کشته می‌شود یا swap می‌کند.

- فرض کردن این‌که خواندن یک فایل کند است، بدون بررسی این‌که آیا page cache از قبل آن را دارد.
- یکی گرفتن I/O تصادفی و ترتیبی، و طراحی الگوی دسترسی تصادفی روی سیستم log-structured.
- تنظیم heap قبل از خواندن لاگ GC.
- فراموش کردن حافظه‌ی off-heap وقتی executor بدون خطای heap کشته می‌شود.
- بزرگ کردن heap برای رفع OOM، که توقف‌های GC را طولانی‌تر می‌کند.
- فرض کردن این‌که zero-copy وقتی TLS روشن است هم کار می‌کند.

## ✅ خودت را بسنج

1. تفاوت اصلی process و thread چیست؟
   - A) process حافظه‌ی heap را با threadهایش شریک است
   - B) process فضای آدرس مخصوص خودش را دارد؛ threadهای داخلش آن فضا را شریک‌اند
   - C) thread می‌تواند جدول file descriptor مخصوص خودش را داشته باشد و process نمی‌تواند
   - D) threadها همیشه روی ماشین‌های جدا اجرا می‌شوند

<details><summary>جواب</summary>

**B** — process فضای آدرس را مالک است و threadهایش آن را شریک‌اند. این اشتراک، ارتباط را ارزان می‌کند و در عین حال امکان data race را هم می‌دهد. گزینه‌ی C هم غلط است، چون threadها file descriptorهای process را شریک‌اند.

</details>

2. چرا مسیر خواندن مصرف‌کننده در Kafka وقتی TLS خاموش است سریع است؟
   - A) Kafka هر خواندن را فشرده می‌کند
   - B) صفحه‌ها از page cache می‌آیند و با zero-copy `sendfile` به socket می‌روند
   - C) Kafka همه‌ی داده را روی heap JVM نگه می‌دارد
   - D) مصرف‌کننده‌ها داده را با I/O تصادفی می‌خوانند

<details><summary>جواب</summary>

**B** — داده‌ی داغ از قبل در page cache است، و `sendfile` آن را بدون کپی در فضای کاربر به socket می‌فرستد. heap (C) عمداً کوچک است و I/O تصادفی (D) درست برعکس الگویی است که Kafka استفاده می‌کند.

</details>

3. یک executor در Spark توسط runtime کانتینر کشته می‌شود، ولی لاگ GC نشان می‌دهد مصرف heap عادی است. محتمل‌ترین علت چیست؟
   - A) یک توقف GC که خیلی طولانی بود
   - B) رشد حافظه‌ی off-heap که متریک‌های heap آن را نشان نمی‌دهند
   - C) فایل کلاس JVM خراب
   - D) page cache خیلی بزرگ

<details><summary>جواب</summary>

**B** — حافظه‌ی off-heap (بافرهای مستقیم، کتابخانه‌های native، Netty) به سقف کانتینر حساب می‌شود، ولی به heap نه. راه‌حل این است که آن را در اندازه‌ی کانتینر لحاظ کنی و منبع تخصیص را پیدا کنی.

</details>

4. (سنیور) یک تیم heap یک job استریمینگ را دو برابر می‌کند تا فرکانس GC کم شود، و زمان توقف‌ها طولانی‌تر می‌شود. چرا، و به جای آن چه می‌کنی؟

<details><summary>جواب</summary>

heap بزرگ‌تر یعنی collector فضای زنده و مرده‌ی بیشتری را پردازش می‌کند. زمان توقف بعضی collectorها با اندازه‌ی heap یا با حجم داده‌ی زنده زیاد می‌شود، پس هر توقف، هرچند کم‌تکرارتر، طولانی‌تر است. حجم داده‌ی زنده را در لاگ GC اندازه بگیر، churn شیء و مجموعه‌های بزرگ cache شده در کد را کم کن، و collector و اندازه‌ی heap را بر اساس هدف latency انتخاب کن، نه این‌که heap را به‌صورت پیش‌فرض بزرگ کنی.

</details>

5. چرا برای یک بروکر Kafka ارزش دارد RAM را برای page cache نگه داری؟
   - A) کلاس‌های JVM را ذخیره می‌کند
   - B) داده‌ی داغ log از RAM سرو می‌شود و خواندن از دیسک برای مصرف‌کننده‌هایی که داده‌ی اخیر را می‌خوانند حذف می‌شود
   - C) جای replication را می‌گیرد
   - D) log را برای producer فشرده می‌کند

<details><summary>جواب</summary>

**B** — مصرف‌کننده‌هایی که داده‌ی اخیر را می‌خوانند از طریق page cache به RAM می‌خورند. replication (C) مکانیزم جدایی است و فشرده‌سازی (D) تنظیم producer یا broker است، نه ویژگی page cache.

</details>

## 🗂️ فلش‌کارت

| جلو | پشت |
|---|---|
| Process در برابر thread | process فضای آدرس خودش را دارد؛ threadهای داخلش آن فضا را شریک‌اند |
| صفحه‌ی حافظه‌ی مجازی | واحد ثابت (معمولاً ۴ کیلوبایت)، با اولین دسترسی توسط page fault بار می‌شود |
| page cache | RAM ای که کرنل برای داده‌ی فایل استفاده می‌کند؛ خواندن و نوشتن از آن عبور می‌کنند |
| I/O ترتیبی در برابر تصادفی | خواندن و نوشتن ترتیبی خیلی سریع‌تر است؛ برای دسترسی پیوسته طراحی کن |
| `sendfile` / zero-copy | کرنل داده‌ی page cache را بدون کپی در فضای کاربر به socket می‌فرستد |
| چرا TLS برای fetch در Kafka مهم است | رمزنگاری در فضای کاربر مسیر ساده‌ی zero-copy را برمی‌دارد |
| توقف stop-the-world | همه‌ی threadهای برنامه در یک فاز GC متوقف می‌شوند |
| حافظه‌ی off-heap | حافظه‌ی بیرون heap ی GC، که به سقف کانتینر حساب می‌شود |

## 🔗 ادامه

- درس‌های مرتبط: [[spark-memory-tuning]] برای چیدمان حافظه‌ی executor، [[kafka-architecture]] برای این‌که بروکرها log و page cache را چطور استفاده می‌کنند، و [[networking-essentials]] برای مسیر socket که zero-copy از آن رد می‌شود.
- مرتبط: [[spark-architecture]] برای processهای driver و executor، و [[jvm-languages-for-de]] برای سمت زبان JVM.
- تمرین: یک job محلی Spark را با لاگ GC فعال اجرا کن، طولانی‌ترین توقف را پیدا کن و با Spark UI مشخص کن در کدام stage رخ داده.

