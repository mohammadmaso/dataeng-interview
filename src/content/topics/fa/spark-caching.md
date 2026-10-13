---
order: 78
tid: "spark-caching"
track: "7"
section: null
num: "7.9"
title: "Caching, Persistence & Checkpointing"
type: "concept"
level: "intermediate"
priority: "P1"
est_hours: 1
summary: "کی cache کنیم و کی نه، سطوح persist، و checkpoint برای قطع lineage طولانی."
covers: []
interview_qs: []
extras: []
prerequisites: ["spark-architecture"]
related: ["spark-memory-tuning", "spark-aqe", "spark-ui-debugging", "spark-shuffle", "spark-structured-streaming"]
---

## 🎯 خلاصه

> **Caching** نتیجه‌ی یک محاسبه را در حافظه‌ی executor یا روی دیسک نگه می‌دارد تا action‌های بعدی به‌جای محاسبه‌ی دوباره از منبع، از آن استفاده کنند. **Checkpointing** نتیجه را روی storage مطمئن می‌نویسد و **lineage** را قطع می‌کند، تا خرابی تاریخچه‌ی کامل را دوباره اجرا نکند.

- `cache()` و `persist()` **lazy** هستند: تا وقتی یک action روی داده اجرا نشود چیزی ذخیره نمی‌شود.
- سطح پیش‌فرض ذخیره `MEMORY_ONLY` برای RDD و `MEMORY_AND_DISK` برای DataFrame و Dataset است. پیش از نقل‌کردنش، مستندات نسخه‌ات را چک کن.
- فقط داده‌ای را cache کن که توسط دو یا چند action یا شاخه **دوباره استفاده** می‌شود، و در حافظه جا می‌شود یا spill به دیسک آن قابل قبول است.
- وقتی **lineage** خیلی بلند است (حلقه‌های iterative، زنجیره‌ی طولانی transformation) یا محاسبه‌ی دوباره گران است و recovery باید مستقل از executorها باشد، checkpoint کن.
- داده‌ی cache شده منبع حقیقت نیست: اگر فایل‌های زیرین عوض شوند، باید refresh یا دوباره بخوانی.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** داری یک شام بزرگ درست می‌کنی. خرد‌کردن پیاز ۲۰ دقیقه طول می‌کشد. اگر پیاز را در سه غذای مختلف لازم داری، یک‌بار خردش می‌کنی، در کاسه‌ای روی میز می‌گذاری و سه بار از همان کاسه استفاده می‌کنی. کاسه همان cache است. اگر کاسه پر شود، بعضی پیازها را در یخچال (دیسک) می‌گذاری. Checkpoint یعنی عکس پیازهای آماده‌شده بگیری و دستور را طوری بنویسی که «از این عکس شروع کن»، تا اگر غذا خراب شد لازم نباشد از اول خرد کنی.

در Spark یک DataFrame یک **دستور پخت** است، نه داده. هر action (`count`، `write`، `collect`) دستور را از اول اجرا می‌کند، از جمله خواندن دوباره‌ی فایل‌ها. اگر دو action از یک نتیجه‌ی میانی گران استفاده کنند و cache نکرده باشی، Spark آن را دوبار محاسبه می‌کند.

```python
from pyspark import StorageLevel

events = spark.read.parquet("/data/events").filter("country = 'IR'")

events.persist(StorageLevel.MEMORY_AND_DISK)   # علامت می‌زند، هنوز محاسبه نمی‌کند
events.count()                                  # اولین action کش را پر می‌کند

daily = events.groupBy("day").count()           # از سطرهای cache شده استفاده می‌کند
users = events.select("user_id").distinct().count()

events.unpersist()                              # وقتی تمام شد حافظه را آزاد کن
```

بدون خط `persist`، `count()` فایل‌های Parquet را می‌خواند و دو کوئری بعدی دوباره می‌خوانند. با آن، فایل‌ها یک بار خوانده می‌شوند. روی داده‌ی بزرگ این می‌تواند بیشترین صرفه‌جویی در job باشد.

میان‌بر `events.cache()` همان `persist()` با سطح پیش‌فرض است.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

**علامت‌گذاری lazy.** `persist()` فقط درخواست را در plan ثبت می‌کند. اولین action هر partition را محاسبه می‌کند، در block manager همان executor ذخیره می‌کند، و action‌های بعدی blockها را می‌خوانند. partitionی که هرگز محاسبه نشده، وقتی لازم شود محاسبه می‌شود.

**سطوح ذخیره.** سطح به سه سؤال جواب می‌دهد: حافظه یا دیسک، سریالایز‌شده یا نه، و چند replica.

| سطح | کجا | نکته |
|---|---|---|
| `MEMORY_ONLY` | heap، آبجکت‌های deserialize‌شده | partitionهایی که جا نشوند، وقتی لازم شوند دوباره محاسبه می‌شوند |
| `MEMORY_AND_DISK` | heap، spill به دیسک | معمولاً امن‌ترین پیش‌فرض برای DataFrame |
| `MEMORY_ONLY_SER` / `MEMORY_AND_DISK_SER` | heap، بایت‌های سریالایز‌شده | حافظه‌ی کمتر، CPU بیشتر برای خواندن (APIهای Java و Scala) |
| `DISK_ONLY` | دیسک محلی | وقتی داده خیلی بزرگ است و محاسبه‌ی دوباره از I/O گران‌تر است |
| نسخه‌های `_2` | دو replica | recovery سریع‌تر بعد از از دست رفتن executor، با دو برابر فضا |
| `OFF_HEAP` | حافظه‌ی off-heap | نیاز به تنظیم حافظه‌ی off-heap دارد |

برای DataFrame در API پایتون، پیش‌فرض `MEMORY_AND_DISK` است. برای RDD، `cache()` از `MEMORY_ONLY` استفاده می‌کند. API مرجع نسخه‌ای را که اجرا می‌کنی چک کن، چون پیش‌فرض‌ها بین APIهای RDD، DataFrame و streaming فرق داشته‌اند.

**مدیریت حافظه.** blockهای cache شده در بخش storage از unified memory در Spark زندگی می‌کنند. وقتی execution (join، sort، aggregation) فضا لازم دارد، Spark می‌تواند blockهای cache را بیرون بیندازد، معمولاً کم‌استفاده‌ترین‌ها اول. block بیرون‌انداخته‌شده دوباره محاسبه (یا اگر سطح اجازه دهد از دیسک خوانده) می‌شود. پس cache می‌تواند با گرفتن حافظه، عملگرهای دیگر را کند کند، و cache خیلی بزرگ باعث churn در eviction می‌شود.

**استفاده از cache.** بعد از cache شدن یک DataFrame، کوئری بعدی که logical plan آن با plan کش‌شده منطبق باشد، داده‌ی cache را می‌خواند نه منبع را. تب **Storage** در Spark UI DataFrameهای کش‌شده، اندازه‌شان در حافظه و دیسک، و درصد partitionهای کش‌شده را نشان می‌دهد. اگر ۴۰٪ partitionها کش شده باشند، بقیه دوباره محاسبه می‌شوند.

**SQL caching.** `CACHE TABLE t` و `CACHE SELECT ...` در Spark SQL نتیجه را در قالب ستونی درون‌حافظه‌ی Spark (فشرده) ذخیره می‌کنند. برای آزادسازی از `UNCACHE TABLE` استفاده کن. فرم SQL برای کار تعاملی راحت است؛ فرم DataFrame کنترل صریح سطح ذخیره را می‌دهد.

**Checkpointing.** checkpoint داده‌ی DataFrame یا RDD را در یک پوشه‌ی checkpoint روی storage مطمئن (HDFS، S3، ABFS و مشابه) می‌نویسد و سپس lineage آن را با خواندن از همان پوشه جایگزین می‌کند:

```python
spark.sparkContext.setCheckpointDir("hdfs:///tmp/spark-checkpoints")

df = df.checkpoint()  # پیش‌فرض eager است: همین حالا اجرا می‌شود و lineage را قطع می‌کند
```

`localCheckpoint()` به‌جایش در storage مربوط به executor می‌نویسد. سریع‌تر است، ولی داده با executor از بین می‌رود؛ پس برای lineage بلند قابل‌اعتماد نیست. فقط وقتی استفاده کن که ریسک از دست رفتن executor را قبول کرده‌ای.

## 🧠 عمق و trade-offها (سنیور)

- **کی cache کمک می‌کند.** استفاده‌ی مجدد محرک است: یک نتیجه‌ی میانی مشترک بین چند action، یک الگوریتم iterative که همان training set را بارها می‌خواند، یا نتیجه‌ی join گران که چند خروجی از آن استفاده می‌کنند. cache کردن DataFrameای که فقط یک بار استفاده می‌شود، هزینه‌ی نوشتن و فشار حافظه اضافه می‌کند و هیچ سودی ندارد.
- **کی cache ضرر می‌زند.** اگر داده‌ی cache شده بزرگ‌تر از حافظه‌ای باشد که می‌توانی بدهی، Spark spill یا eviction می‌کند و هر دو هزینه را می‌پردازی. cache کردن قبل از فیلتری که ۱٪ داده را نگه می‌دارد، ۱۰۰ برابر بیشتر از نیاز ذخیره می‌کند. cache را بعد از فیلتر انتخابی و projection بگذار و فقط ستون‌هایی را نگه دار که استفاده می‌کنی.
- **cache کردن خروجی shuffle با cache کردن نتیجه یکی نیست.** خروجی shuffle از قبل در فایل‌های shuffle materialize شده، پس اگر همان exchange در همان کوئری دوباره ظاهر شود، Spark بدون cache دوباره استفاده می‌کند. cache بیشتر برای نتیجه‌ی transformationهایی ارزش دارد که قبل از shuffle یا بعد از کار narrow گران می‌آیند.
- **Lineage و خرابی.** Spark partitionهای از دست‌رفته را با محاسبه‌ی دوباره از lineage بازیابی می‌کند. برای partition کش‌شده‌ی از دست‌رفته، از منبع دوباره محاسبه می‌کند، نه از replica (مگر این‌که `_2` استفاده کرده باشی). زنجیره‌ی بلند transformation زمان recovery را بالا می‌برد، و در کد iterative lineage در هر iteration بزرگ‌تر می‌شود. checkpoint هر N iteration هم هزینه‌ی recovery را محدود می‌کند و هم اندازه‌ی plan‌ای را که درایور باید تحلیل کند.
- **Checkpoint هزینه‌ی نوشتن دارد.** checkpoint یک job کامل به‌علاوه نوشتن روی storage مطمئن است و یک barrier است. checkpoint را در نقاط طبیعی بگذار (بعد از join گران، هر ۱۰ iteration حلقه)، نه بعد از هر مرحله.
- **داده‌ی کهنه.** cache یک کپی است. اگر pipeline دیگری فایل‌های منبع را حین اجرای job بازنویسی کند، cache همچنان داده‌ی قدیمی را نشان می‌دهد. برای منابع فایلی، با `REFRESH TABLE` یا خواندن دوباره refresh کن. در streaming، cache را state حساب نکن؛ از مدیریت state درست استفاده کن (ببین [[state-store]]).
- **AQE و cache.** یک intermediate کش‌شده بعد از materialize شدن آمار واقعی دارد، که به AQE کمک می‌کند برای stageهای بعدی join بهتری انتخاب کند. cache کردن طرف کوچک و فیلترشده‌ی یک join می‌تواند در کوئری‌های بعدی sort-merge join را به broadcast join تبدیل کند. ببین [[spark-aqe]].
- **اندازه‌گیری.** تب Storage، پیام‌های eviction در لاگ executor، زمان GC در تب Executors، و تعداد taskها در stageهایی که cache را دوباره استفاده می‌کنند را ببین. cache فقط وقتی مفید است که stage استفاده‌کننده سریع‌تر از محاسبه‌ی دوباره باشد.

## 🧪 امتحان کن

این‌ها را در PySpark 3.x یا `spark-sql` اجرا کن. از داده‌ی تولیدشده استفاده کن (مثلاً `spark.range(10_000_000)` با چند ستون محاسبه‌شده) تا زمان‌ها را مقایسه کنی.

1. یک DataFrame با transformation گران بساز (یک UDF یا زنجیره‌ی سنگین `withColumn`). سه action از نوع `count()` با و بدون `cache()` اجرا کن. زمان action دوم و سوم را در Spark UI مقایسه کن.
2. با `StorageLevel.DISK_ONLY` persist کن، یک action اجرا کن و تب Storage را ببین. بعد `unpersist()` کن و تأیید کن که ورودی از بین رفته.
3. یک حلقه‌ی iterative بساز که در هر iteration یک `withColumn` اضافه می‌کند و ۵۰ بار تکرار می‌شود. زمان آخرین `count()` را بدون checkpoint، و بعد با `df.checkpoint()` هر ۱۰ iteration بسنج.

<details><summary>پاسخ‌ها را ببین</summary>

**1.** بدون cache، هر action زنجیره را دوباره محاسبه می‌کند، پس هر `count()` تقریباً یک زمان می‌برد. با cache، اولین action کندتر است (محاسبه و ذخیره) و دو تای بعدی خیلی سریع‌ترند، چون از block storage می‌خوانند. بررسی کن که تب Storage اندازه‌ی کش را نشان دهد و stage از حافظه بخواند.

**2.** بعد از `persist(StorageLevel.DISK_ONLY)` و یک action، تب Storage، DataFrame را با اندازه‌ی دیسک و بدون اندازه‌ی حافظه نشان می‌دهد. بعد از `unpersist()`، ورودی وقتی Spark blockها را پاک کند حذف می‌شود، که ممکن است کمی طول بکشد.

**3.** بدون checkpoint، plan برای iteration پنجاه lineage بلندی دارد و زمان برنامه‌ریزی با آن بالا می‌رود. با checkpoint هر ۱۰ iteration، lineage قطع می‌شود و برنامه‌ریزی کوتاه‌تر است. خود checkpoint یک نوشتن هزینه دارد، پس در حلقه‌ی کوتاه زمان کل ممکن است زیاد کم نشود؛ سود در زنجیره‌های بلندتر و در recovery بعد از خرابی دیده می‌شود.

</details>

## 💬 گوشه‌ی مصاحبه

> **🎤 این را در مصاحبه بگو:** «Cache یک DataFrame محاسبه‌شده را در حافظه یا دیسک executor نگه می‌دارد تا action‌های بعدی دوباره محاسبه نکنند، ولی lazy است و با execution memory رقابت می‌کند. فقط داده‌ی دوباره‌استفاده‌شونده را cache می‌کنم و cache را بعد از فیلترهای انتخابی می‌گذارم. Checkpoint روی storage مطمئن می‌نویسد و lineage را قطع می‌کند؛ در jobهای iterative بلند از آن استفاده می‌کنم تا هزینه‌ی recovery و برنامه‌ریزی پایین بماند.»

**سؤال‌های رایج**

- *تفاوت cache و checkpoint چیست؟* cache یک کپی برای استفاده‌ی مجدد نگه می‌دارد و lineage را حفظ می‌کند؛ checkpoint روی storage مطمئن می‌نویسد و lineage را حذف می‌کند.
- *چرا `cache()` انگار کاری نمی‌کند؟* lazy است. برای materialize کردن یک action اجرا کن و تب Storage را چک کن.
- *کی از `DISK_ONLY` استفاده می‌کنی؟* وقتی داده برای حافظه بزرگ است و خواندن از دیسک محلی ارزان‌تر از محاسبه‌ی دوباره از منبع کند است.
- *چرا یک job با cache هنوز کند است؟* cache خیلی بزرگ بود، توسط execution memory بیرون رانده شد، یا قبل از فیلتری گذاشته شد که فقط بخش کمی از داده را نگه می‌دارد.

**جواب mid:** lazy بودن، سطوح پیش‌فرض، زمان unpersist، و تأیید cache در UI را توضیح می‌دهد.

**جواب senior:** رفتار eviction زیر unified memory، محل checkpoint در حلقه‌های iterative، trade-off بین `checkpoint` و `localCheckpoint`، ریسک داده‌ی کهنه، و تعامل با آمار AQE را اضافه می‌کند.

## ⚠️ اشتباهات رایج

- **cache کردن همه‌چیز «برای احتیاط».** هر cache حافظه می‌گیرد و می‌تواند بقیه‌ی job را کند کند. فقط چیزی را cache کن که دوباره استفاده می‌شود.
- **فراموش کردن unpersist.** notebookها و jobهای طولانی داده‌ی cache شده‌ای را نگه می‌دارند که دیگر کاربردی ندارد.
- **انتظار این‌که cache از restart جان سالم به در ببرد.** cache در حافظه و دیسک executor اپلیکیشن فعلی است. اپلیکیشن جدید با cache خالی شروع می‌کند.
- **cache کردن بعد از یک مرحله‌ی غیرانتخابی و قبل از فیلتر انتخابی.** cache را بعد از فیلتر و projectionی بگذار که واقعاً استفاده می‌کنی.
- **استفاده از `localCheckpoint` برای pipeline بلندی که باید از خرابی executor جان سالم ببرد.** روی storage مطمئن نیست. از `checkpoint()` با پوشه‌ی تنظیم‌شده استفاده کن.
- **فرض این‌که داده‌ی cache شده وقتی فایل‌ها عوض می‌شوند refresh می‌شود.** نمی‌شود. جدول را دوباره بخوان یا refresh کن.

## ✅ خودت را بسنج

۱. وقتی `df.cache()` و بعد `df.count()` را صدا می‌زنی چه اتفاقی می‌افتد؟

   - A) هیچ؛ `cache()` یک action است
   - B) `cache()` DataFrame را علامت می‌زند؛ `count()` partitionها را محاسبه و ذخیره می‌کند، پس action‌های بعدی دوباره استفاده می‌کنند
   - C) داده به‌عنوان checkpoint در HDFS نوشته می‌شود
   - D) cache فقط در حافظه‌ی درایور ساخته می‌شود

<details><summary>پاسخ</summary>

**B** — `cache()` lazy است و فقط درخواست را ثبت می‌کند. اولین action، partitionها را در storage executor materialize می‌کند. checkpoint متفاوت است و cache روی درایور ساخته نمی‌شود.

</details>

۲. یک DataFrame کش‌شده در تب Storage نشان می‌دهد ۴۰٪ partitionها cache شده‌اند. این برای action بعدی یعنی چه؟

   - A) job شکست می‌خورد
   - B) ۶۰٪ بدون cache وقتی لازم شوند از طریق lineage دوباره محاسبه می‌شوند، پس action کندتر از حالت کاملاً cache شده است
   - C) Spark خودکار cache را روی دیسک replicate کرده
   - D) cache نامعتبر است و باید حذف شود

<details><summary>پاسخ</summary>

**B** — partitionهایی که در storage نیستند از منبع و از طریق lineage محاسبه می‌شوند. نتیجه درست است ولی کندتر.

</details>

۳. **(متوسط)** چه زمانی `checkpoint()` انتخاب بهتری از `cache()` است؟

   - A) همیشه، چون سریع‌تر است
   - B) وقتی lineage بلند است (مثلاً یک حلقه‌ی iterative) یا وقتی recovery باید مستقل از حافظه‌ی executor باشد؛ checkpoint روی storage مطمئن می‌نویسد و lineage را قطع می‌کند
   - C) وقتی داده در حافظه جا می‌شود و یک بار استفاده می‌شود
   - D) وقتی فقط در درایور داده لازم است

<details><summary>پاسخ</summary>

**B** — checkpoint داده را روی storage مطمئن می‌نویسد و وابستگی به lineage قبلی را برمی‌دارد. cache راه‌اندازی سریع‌تری دارد ولی lineage را کوتاه نمی‌کند و از خرابی executor جان سالم به در نمی‌برد.

</details>

۴. **(سنیور)** یک job یک DataFrame ۲۰۰ گیگابایتی را با `MEMORY_AND_DISK` cache می‌کند و joinهای بعدی کندتر از حالت بدون cache می‌شوند. محتمل‌ترین علت چیست؟

   - A) cache همیشه joinها را کند می‌کند
   - B) cache حافظه‌ای را می‌گیرد که execution لازم دارد، پس joinها بیشتر spill می‌کنند و blockهای cache بیرون رانده و دوباره محاسبه می‌شوند؛ نسخه‌ی کوچک‌تر و فیلترشده و projectionشده را cache کن
   - C) `MEMORY_AND_DISK` برای join پشتیبانی نمی‌شود
   - D) job باید از `MEMORY_ONLY` استفاده کند تا سریع‌تر شود

<details><summary>پاسخ</summary>

**B** — داده‌ی cache شده تحت unified memory با execution رقابت می‌کند. کاهش اندازه‌ی cache (فیلتر، انتخاب ستون‌های لازم، یا cache بعد از shuffle که داده را کم می‌کند) فشار را برمی‌دارد. تغییر به `MEMORY_ONLY` احتمال eviction و محاسبه‌ی دوباره را بیشتر می‌کند.

</details>

۵. **(سنیور)** چرا checkpoint می‌تواند زمان برنامه‌ریزی یک job iterative را کم کند، نه فقط زمان recovery را؟

   - A) checkpointها باعث می‌شوند درایور تحلیل را رد کند
   - B) plan‌ای که درایور برای هر action تحلیل می‌کند به lineage وابسته است؛ checkpoint زنجیره‌ی بلند را با خواندن از داده‌ی ذخیره‌شده جایگزین می‌کند، پس plan کوتاه می‌شود
   - C) checkpointها plan کوئری را فشرده می‌کنند
   - D) checkpointها فقط روی درایور اجرا می‌شوند

<details><summary>پاسخ</summary>

**B** — lineage همان چیزی است که logical plan را می‌سازد و با هر iteration بزرگ‌تر می‌شود. قطع‌کردن آن کار برنامه‌ریزی و recovery را محدود می‌کند. به همین دلیل حلقه‌های بلند هر چند iteration checkpoint می‌زنند.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| آیا `cache()` lazy است؟ | بله. فقط علامت می‌زند؛ یک action آن را materialize می‌کند |
| سطح پیش‌فرض ذخیره‌ی DataFrame؟ | `MEMORY_AND_DISK` (نسخه‌ات را چک کن)؛ `cache()` روی RDD از `MEMORY_ONLY` استفاده می‌کند |
| `_2` در سطح ذخیره یعنی چه؟ | دو replica از هر partition کش‌شده، برای recovery سریع‌تر با دو برابر فضا |
| زیر فشار حافظه اول چه بیرون می‌رود؟ | blockهای cache شده، معمولاً کم‌استفاده‌ترین‌ها، تا جا برای execution باز شود |
| checkpoint روی lineage چه می‌کند؟ | داده را روی storage مطمئن می‌نویسد و lineage را قطع می‌کند |
| cache در برابر checkpoint در یک خط؟ | cache = استفاده‌ی سریع مجدد، lineage را نگه می‌دارد؛ checkpoint = recovery مطمئن، lineage را قطع می‌کند |
| چرا cache منبع حقیقت نیست؟ | یک کپی است؛ اگر منبع عوض شود، cache کهنه است تا refresh شود |
| cache کجا باید بگذاری؟ | بعد از فیلتر و projection انتخابی، قبل از نقاط استفاده‌ی مجدد |

## 🔗 ادامه

- **spark-memory-tuning** مدل unified memory را توضیح می‌دهد که تعیین می‌کند چقدر cache جا می‌شود.
- **spark-aqe** نشان می‌دهد چطور intermediateهای materialize شده آمار واقعی به AQE برای joinهای بعدی می‌دهند.
- **spark-ui-debugging** تب Storage و خواندن cache hit و miss در stageها را پوشش می‌دهد.
- **spark-structured-streaming** توضیح می‌دهد چرا state در streaming با cache معمولی مدیریت نمی‌شود.
- واژه‌نامه: [[checkpoint]]، [[lineage]]، [[state-store]].
