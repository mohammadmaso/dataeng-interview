---
order: 96
tid: "spark-structured-streaming"
track: "8"
section: null
num: "8.12"
title: "Spark Structured Streaming"
type: "technology"
level: "advanced"
priority: "P1"
est_hours: 4
summary: "مدل micro-batch، output modeها (append، update، complete)، watermark و stateful ops، checkpoint location، foreachBatch برای sink سفارشی و idempotent."
covers: []
interview_qs: []
extras: []
prerequisites: ["spark-architecture", "stream-concepts"]
related: ["streaming-state", "stream-joins", "time-and-ordering", "spark-shuffle", "pattern-late-data", "kafka-exactly-once"]
---

## 🎯 خلاصه

> Structured Streaming یک stream را مثل یک **جدول در حال رشد** می‌بیند. همان DataFrame یا SQLی را می‌نویسی که برای batch می‌نوشتی، و Spark آن را مکرراً روی **micro-batch**های داده‌ی جدید اجرا می‌کند.

- مدل پیش‌فرض **micro-batch** است: هر trigger داده‌ی جدید را می‌خواند، نتیجه را به‌روز می‌کند و در sink می‌نویسد.
- **output mode** (`append`، `update`، `complete`) تعیین می‌کند هر trigger چه چیزی نوشته شود.
- **watermark** به Spark می‌گوید داده تا چقدر دیر می‌تواند برسد، تا state قدیمی را پاک کند و پنجره‌ها را نهایی کند.
- **checkpoint location** offsetها و state را نگه می‌دارد. آن چیزی است که restart و بازیابی را ممکن می‌کند. بدون آن، query آماده‌ی production نیست.
- **foreachBatch** هر micro-batch را به کد خودت می‌دهد. برای sinkهایی که writer استریمی ندارند استفاده‌اش کن، ولی نوشتن را خودت idempotent کن.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 Analogy:** یک **اتاق نامه** را تصور کن که هر ۱۰ ثانیه کیسه‌ی نامه را باز می‌کند. هر چه از بار قبل رسیده، مرتب می‌کند، شمارش روی تخته را به‌روز می‌کند و گزارش را می‌نویسد. نامه‌ها را یکی‌یکی پردازش نمی‌کند؛ کیسه‌به‌کیسه کار می‌کند.

یک word count ساده‌ی استریمی، با خواندن از socket برای نمایش:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("wordcount").getOrCreate()

lines = (spark.readStream.format("socket")
         .option("host", "localhost").option("port", 9999).load())

words = lines.select(F.explode(F.split("value", " ")).alias("word"))
counts = words.groupBy("word").count()

query = (counts.writeStream
         .outputMode("complete")
         .format("console")
         .option("checkpointLocation", "/tmp/chk/wordcount")
         .start())

query.awaitTermination()
```

از این مثال سه چیز یاد بگیر:

- کد تقریباً همان کد batch است. `read` را به `readStream` و `write` را به `writeStream` تبدیل می‌کنی.
- حالت `complete` در هر trigger کل جدول را چاپ می‌کند، چون شمارش هر کلمه ممکن است عوض شود.
- دایرکتوری checkpoint برای بیشتر sinkها لازم است. اگر پاکش کنی، query از اول شروع می‌شود.

> **⚠️ Watch out:** استریم یعنی «لحظه‌ای» نیست. هر micro-batch سربار دارد. latency معمولاً ثانیه‌هاست، نه میلی‌ثانیه.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### حلقه‌ی micro-batch

هر trigger این مراحل را انجام می‌دهد:

1. از source داده‌ی جدید را می‌خواهد (برای Kafka: بازه‌ی offset از آخرین batch commit شده تا آخرین offset).
2. بازه‌ی offset را **قبل** از پردازش در checkpoint (offset log) می‌نویسد.
3. query را روی آن بازه اجرا می‌کند و اگر query stateful باشد، state store را می‌خواند و به‌روز می‌کند.
4. نتیجه را در sink می‌نویسد.
5. batch را در commit log داخل checkpoint ثبت می‌کند.

اگر driver بین مراحل کرش کند، Spark هنگام restart از offset log می‌خواند و همان بازه و همان ورودی را دوباره اجرا می‌کند. تضمین Spark از این‌جا می‌آید: یک **source قابل replay** (Kafka، فایل) به‌علاوه‌ی **sinkی که تکرار را نادیده می‌گیرد**، نتیجه‌ی exactly-once می‌دهد.

### triggerها

| Trigger | رفتار | استفاده وقتی |
|---|---|---|
| پیش‌فرض (بدون trigger) | batch بعدی بلافاصله بعد از تمام شدن batch قبلی | latency پایین، ظرفیت کافی کلاستر |
| `processingTime="10 seconds"` | فاصله‌ی ثابت | بار و هزینه‌ی قابل پیش‌بینی |
| `availableNow=True` | همه‌ی داده‌ی موجود را در چند batch پردازش می‌کند، بعد متوقف می‌شود | jobهای افزایشی شبیه batch |
| Continuous | آزمایشی، latency خیلی پایین، عملگرهای کمی پشتیبانی می‌شوند | docs نسخه‌ی خودت را ببین؛ به‌ندرت انتخاب پیش‌فرض |

### output modeها

| Mode | چه چیزی هر trigger نوشته می‌شود | مجاز برای |
|---|---|---|
| `append` | فقط ردیف‌های جدیدی که دیگر تغییر نمی‌کنند | selectهای ساده، و aggregationهای همراه watermark |
| `update` | ردیف‌هایی که از trigger قبلی تغییر کرده‌اند | aggregationها؛ sinkهایی که upsert دارند |
| `complete` | کل جدول نتیجه | فقط aggregationها؛ نتیجه باید در حافظه جا شود |

برای یک aggregation پنجره‌ای با `append`، Spark صبر می‌کند تا watermark از انتهای پنجره بگذرد، و بعد پنجره را می‌نویسد. پنجره در آن لحظه نهایی است و فقط یک بار append می‌شود. با `update` همان پنجره ممکن است چند بار نوشته شود، چون داده‌ی دیررسیده آن را عوض می‌کند.

### watermark و event time

event time همان timestamp داخل رکورد است. processing time زمانی است که Spark رکورد را می‌بیند. رکوردها دیر می‌رسند، پس Spark به یک حد نیاز دارد.

```python
from pyspark.sql import functions as F

clicks = (events
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"), "user_id")
    .count())
```

watermark تقریباً این است: `max(event_time دیده‌شده تا الان) − ۱۰ دقیقه`. پنجره وقتی بسته می‌شود که watermark از انتهایش بگذرد. state قدیمی‌تر از watermark حذف می‌شود و رکوردهای دیرتر از watermark دور ریخته می‌شوند.

یک مثال عددی:

```text
window = [10:00, 10:05)
watermark delay = 10 min
largest event_time seen = 10:16  → watermark = 10:06
→ window [10:00, 10:05) is closed; state is dropped
→ a record with event_time 10:03 that arrives now is discarded
```

> **💡 Analogy:** watermark **ساعت آخر پذیرش نامه‌ی دیررسیده** است. نامه‌هایی که قبل از این ساعت مهر خورده‌اند پذیرفته می‌شوند؛ بقیه به سبد نامه‌های گم‌شده می‌روند.

### عملیات stateful

aggregationها، deduplication (`dropDuplicates`)، stream-stream joinها و `mapGroupsWithState` بین batchها state نگه می‌دارند. state در **state store**ها زندگی می‌کند، مثل یک shuffle پارتیشن‌بندی می‌شود و در checkpoint ذخیره می‌شود. بدون watermark، state مربوط به `groupBy` روی یک کلید بی‌کران بی‌نهایت رشد می‌کند. این رایج‌ترین خرابی در production است.

می‌توانی پیاده‌سازی state store را انتخاب کنی. provider مبتنی بر RocksDB، state بزرگ را از heap مربوط به JVM بیرون نگه می‌دارد؛ کلید تنظیم دقیق را برای نسخه‌ی خودت در docs چک کن.

### foreachBatch

`foreachBatch` هر micro-batch را به‌صورت یک DataFrame معمولی batch به‌علاوه‌ی `batchId` به تو می‌دهد:

```python
def write_batch(batch_df, batch_id):
    # batch_df is a regular DataFrame; you can use any batch sink or MERGE
    (batch_df.write
        .mode("append")
        .parquet(f"/lake/clicks_counts/batch_{batch_id}"))  # the batch id names the output

query = (counts.writeStream
    .foreachBatch(write_batch)
    .outputMode("update")
    .option("checkpointLocation", "/checkpoints/clicks")
    .start())
```

به‌صورت پیش‌فرض این **at-least-once** است: اگر job بعد از نوشتن و قبل از commit شکست بخورد، همان `batch_id` دوباره اجرا می‌شود. برای idempotent کردنش:

- در مسیری که با `batch_id` کلید خورده بنویس و overwrite کن، یا
- یک `MERGE` در جدولی با کلید کسب‌وکار انجام بده (یک retry دیرهنگام همان ردیف را به‌روز می‌کند)، یا
- آخرین `batch_id` commit شده را در همان transaction داده ذخیره کن.

مثال بالا فایل‌ها را با batch id نام‌گذاری می‌کند، ولی اجرای دوم یک کپی دوم می‌نویسد. یک pipeline واقعی یا آن مسیر را overwrite می‌کند یا از `MERGE` استفاده می‌کند.

### خواندن از Kafka

```python
events = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "clicks")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 2_000_000)   # backpressure: cap per batch
    .load()
    .selectExpr("CAST(value AS STRING) AS json"))
```

`maxOffsetsPerTrigger` راه محدود کردن اندازه‌ی batch است. بدون آن، یک backlog تبدیل به یک micro-batch عظیم می‌شود که شاید در حافظه جا نشود.

## 🧠 عمق و trade-offها (سنیور)

**watermark یک trade-off بین latency و کامل بودن است.** watermark بلندتر داده‌ی دیررس بیشتری می‌پذیرد، ولی state بیشتری نگه می‌دارد و نتایج نهایی را دیرتر می‌کند. یک تأخیر ۱ ساعته برای پنجره‌ی ۵ دقیقه‌ای حدود ۱۲ پنجره‌ی فعال برای هر کلید نگه می‌دارد. برای کلیدهای با کاردینالیتی بالا این اصلی‌ترین هزینه‌ی حافظه است. تأخیر را از توزیع دیررسی اندازه‌گیری‌شده‌ی منبعت انتخاب کن، نه از حدس.

**تخمین اندازه‌ی state.**

```text
keys per window          = 2,000,000 active users
windows kept at once     = 10-minute watermark / 5-minute windows → about 3
state rows               = 2,000,000 × 3 = 6,000,000
bytes per row (approx.)  = 100 B  → about 600 MB of state
```

آن را در تعداد state partitionها ضرب کن و حاشیه اضافه کن. قبل از deploy با metricهای progress مربوط به query اندازه بگیر.

**تعداد partitionهای shuffle.** عملیات stateful داده را shuffle می‌کنند. `spark.sql.shuffle.partitions` (پیش‌فرض ۲۰۰) تعداد partition این shuffle را تعیین می‌کند. تعداد کم، فایل‌های بزرگ state برای هر task می‌سازد؛ تعداد زیاد، فایل‌های ریز و سربار زیاد. برای استریم معمولاً یک مقدار ثابت که برای اوج توان عملیاتی اندازه شده رایج است. [[spark-shuffle]] را ببین.

**backpressure و اندازه‌گذاری.** source در هر batch تا `maxOffsetsPerTrigger` می‌خواند. اگر زمان پردازش هر batch از فاصله‌ی trigger بیشتر شود، batchها صف می‌کشند و latency بالا می‌رود. مدت batch را در برابر فاصله‌ی trigger رصد کن. دو قاعده‌ی سرانگشتی:

- زمان پردازش هر batch را راحت زیر فاصله‌ی trigger نگه دار.
- کلاستر را برای اوج ورودی اندازه بگیر، نه میانگین، وگرنه lag در هر اوج جمع می‌شود.

**سازگاری schema و checkpoint.** checkpoint طرح query و چیدمان state را ثبت می‌کند. تغییر query (اضافه کردن کلید گروه، تغییر aggregation، تغییر عملگر state) معمولاً نمی‌گذارد از checkpoint قدیمی ادامه دهی. مهاجرت را برنامه‌ریزی کن: یک query جدید با checkpoint جدید شروع کن، در صورت نیاز backfill کن، بعد consumerها را سوئیچ کن.

**کلیدهای داغ (skewed).** یک کلید داغ کل state و کارش را روی یک task می‌گذارد. با یک aggregation دو مرحله‌ای کم‌اثرش کن (کلید را salt کن، تجمیع کن، بعد ترکیب کن). [[hot-key]] را ببین.

**exactly-once.** تضمین Spark برای state خود query و برای sinkهایی که پشتیبانی‌اش را دارند برقرار است (file sink با metadata log خودش، Delta، Iceberg). یک sink سفارشی در `foreachBatch` مگر این‌که idempotent باشد at-least-once است. بدون نام بردن sink، «exactly-once از ابتدا تا انتها» قول نده.

**stream-stream joinها** برای join خروجی باید watermark روی هر دو ورودی و یک شرط بازه‌ی زمانی داشته باشند. بدون آن‌ها، state مربوط به join بی‌نهایت رشد می‌کند. [[stream-joins]] را ببین.

**حالت‌های خرابی.**

- state بی‌کران به‌خاطر نبود watermark، که به فشار حافظه‌ی executor یا checkpoint کند ختم می‌شود.
- تغییر query و بعد restart از checkpoint قدیمی، که با خطای state ناسازگار شکست می‌خورد.
- یک sink کند (مثلاً دیتابیسی تحت بار) که هر batch را طولانی‌تر می‌کند تا از فاصله‌ی trigger بگذرد.
- پاک کردن checkpoint «برای حل» یک خطا، که بسته به retention منبع، داده را دوباره پخش یا رد می‌کند.

**چه چیزی را پایش کنی.** نرخ ورودی در برابر نرخ پردازش، مدت batch، `numInputRows` در هر batch، تعداد ردیف‌ها و حافظه‌ی state برای هر عملگر، مقدار watermark و فاصله‌اش از زمان دیوار، و offset lag منبع. Spark این‌ها را در `query.lastProgress` و از طریق `StreamingQueryListener` نشان می‌دهد.

**کی نباید استفاده شود.** اگر به latency زیر ثانیه برای هر رکورد نیاز داری، یک موتور record-at-a-time مثل Flink مناسب‌تر است. اگر job یک بارگذاری روزانه است و نیاز به تازگی ندارد، یک job batch ساده‌تر و ارزان‌تر است.

## 🧪 امتحان کن

مثال Kafka را محلی اجرا کن (Spark به‌همراه connector مربوط به Kafka برای نسخه‌ی خودت). بعد:

1. ۱۰ رویداد با timestamp در همان پنجره‌ی ۵ دقیقه‌ای بفرست، یکی با `event_time` ۳۰ دقیقه قبل.
2. شمارش پنجره‌ای را با `withWatermark("event_time", "10 minutes")` و `outputMode("append")` اجرا کن. پیش‌بینی کن کدام ردیف‌ها چاپ می‌شوند و کی.
3. query را متوقف کن و با **همان** checkpoint دوباره شروع کن. تأیید کن batchهای قدیمی را دوباره پردازش نمی‌کند.
4. کلید گروه را عوض کن و با همان checkpoint restart کن. خطا را بخوان. بعد با یک checkpoint جدید restart کن.
5. نوشتن `foreachBatch` را idempotent کن: در مسیری که با batch id کلید خورده بنویس، بعد آن batch را دوباره اجرا کن و تأیید کن خروجی تغییری نکرده.

```python
# Quick check: inspect the last batch's metrics
print(query.lastProgress)   # input rows, batch duration, watermark, state operators
```

> **🎤 Say this in the interview:** «من یک query استریمی را مثل یک query batch می‌بینم که روی micro-batch اجرا می‌شود، با offset و state که در checkpoint ذخیره شده‌اند. روی هر عملگر stateful همیشه watermark می‌گذارم، چون بدون آن state بی‌کران رشد می‌کند. برای sinkها در صورت امکان از writer آگاه از استریم استفاده می‌کنم، و با foreachBatch نوشتن را با batch id یا یک MERGE idempotent می‌کنم. کلاستر را برای اوج ورودی اندازه می‌گیرم و مدت batch را در برابر فاصله‌ی trigger پایش می‌کنم.»

## 💬 گوشه‌ی مصاحبه

**سؤال: تفاوت append، update و complete چیست؟**
- **مبتدی:** append فقط ردیف‌های جدید اضافه می‌کند. update ردیف‌های تغییرکرده را می‌نویسد. complete هر بار کل نتیجه را می‌نویسد.
- **متوسط:** append روی aggregation به watermark نیاز دارد، پس هر پنجره وقتی نهایی شد یک بار نوشته می‌شود. complete کل نتیجه را در حافظه لازم دارد و فقط برای aggregationهای کوچک جواب می‌دهد.
- **سنیور:** حالتی را انتخاب کن که sink پشتیبانی می‌کند. append به یک lake ساده و نهایی است. update به sinkی نیاز دارد که upsert دارد. complete برای داشبورد کوچک است. هر انتخاب رفتار تکرار و اصلاح را در پایین‌دست تغییر می‌دهد.

**سؤال: watermark چه کار می‌کند؟**
- **متوسط:** حد دیررسی داده را تعیین می‌کند. Spark state را حذف می‌کند و رکوردهای قدیمی‌تر از watermark را رد می‌کند، و پنجره‌ها را وقتی watermark از آن‌ها گذشت نهایی می‌کند.
- **سنیور:** این یک trade-off بین کامل بودن و حافظه است. watermark را از دیررسی اندازه‌گیری‌شده انتخاب می‌کنم و تعداد رکوردهای حذف‌شده را گزارش می‌دهم تا کسب‌وکار بداند چه چیزی جا افتاده.

**سؤال: با Spark و Kafka چطور exactly-once بگیری؟**
- **متوسط:** offsetهای قابل replay به‌علاوه‌ی checkpoint، در خرابی replay می‌دهند. sink نباید تکرار کند. file sink و فرمت‌های جدولی مثل Delta یا Iceberg این را هندل می‌کنند؛ یک foreachBatch ساده به نوشتن idempotent نیاز دارد.
- **سنیور:** اول sink را نام ببر. تضمین برای پروتکل commit آن sink برقرار است و بقیه‌ی pipeline at-least-once است. برای تصویر گسترده‌تر [[kafka-exactly-once]] را ببین.

**جواب نمونه (سنیور):** Structured Streaming یک query batch را روی micro-batch اجرا می‌کند، با offset و state در checkpoint. روی هر عملگر stateful watermark می‌گذارم تا state محدود بماند، output mode‌ای را انتخاب می‌کنم که sink پشتیبانی کند، و نوشتن در foreachBatch را idempotent می‌کنم. مدت batch را در برابر فاصله‌ی trigger و اندازه‌ی state پایش می‌کنم، و تغییر query را به‌عنوان یک مهاجرت با checkpoint جدید در نظر می‌گیرم.

## ⚠️ اشتباهات رایج

- **aggregation stateful بدون watermark.** state بی‌نهایت رشد می‌کند.
- **فرض این‌که `complete` پیش‌فرض داشبورد است.** هر trigger کل نتیجه را دوباره می‌نویسد.
- **استفاده‌ی دوباره از checkpoint قدیمی بعد از تغییر query.** restart شکست می‌خورد یا state اشتباه خوانده می‌شود.
- **پاک کردن checkpoint برای حل یک خطا** بدون دانستن این‌که چه چیزی دوباره پخش یا رد می‌شود.
- **گفتن «exactly-once» برای foreachBatch.** این at-least-once است، مگر این‌که نوشتن idempotent باشد.
- **نادیده گرفتن `maxOffsetsPerTrigger`**، که باعث می‌شود یک backlog تبدیل به یک batch عظیم شود.
- **اشتباه گرفتن event time و processing time** در پنجره‌ها، که بعد از یک قطعی بی‌صدا رویدادها را به پنجره‌ی اشتباه می‌برد.

## ✅ خودت را بسنج

1. کدام output mode برای یک select استریمی بدون aggregation و بدون watermark معتبر است؟
   - A) complete
   - B) update
   - C) append
   - D) هیچ‌کدام

   <details><summary>Answer</summary>

   **C** — بدون aggregation، ردیف‌ها وقتی برسند نهایی‌اند، پس append حالت طبیعی است. complete به aggregation نیاز دارد و update برای ردیف‌هایی است که تغییر می‌کنند.

   </details>

2. اگر event time یک رکورد قدیمی‌تر از watermark فعلی باشد چه می‌شود؟
   - A) بلافاصله در sink نوشته می‌شود
   - B) از عملیات stateful کنار گذاشته می‌شود، پس پنجره‌های نهایی‌شده را تغییر نمی‌دهد
   - C) query را restart می‌کند
   - D) به زمان فعلی منتقل می‌شود

   <details><summary>Answer</summary>

   **B** — رکوردهای پشت watermark برای عملیات stateful دور ریخته می‌شوند، چون پنجره‌شان بسته در نظر گرفته می‌شود و state آن حذف شده است.

   </details>

3. **سناریو:** بعد از اضافه کردن یک کلید گروه، هنگام restart query با خطایی درباره‌ی state ناسازگار شکست می‌خورد. راه‌حل چیست؟
   - A) افزایش حافظه‌ی executor
   - B) شروع query با یک checkpoint location جدید، و backfill یا مهاجرت خروجی در صورت نیاز
   - C) تغییر به حالت complete
   - D) حذف topic منبع

   <details><summary>Answer</summary>

   **B** — checkpoint طرح state قدیمی را نگه می‌دارد. query تغییریافته نمی‌تواند از آن استفاده کند، پس از صفر شروع می‌کنی و تاریخچه‌ی خروجی را جداگانه مدیریت می‌کنی.

   </details>

4. **trade-off سنیور:** چرا watermark بلندتر هزینه را بالا می‌برد؟
   - A) تعداد partitionها را زیاد می‌کند
   - B) state پنجره‌ها را طولانی‌تر نگه می‌دارد، پس state بیشتری در حافظه و checkpoint باقی می‌ماند
   - C) checkpoint را غیرفعال می‌کند
   - D) حالت continuous را اجباری می‌کند

   <details><summary>Answer</summary>

   **B** — state هر پنجره‌ی باز تا وقتی watermark از آن بگذرد باقی می‌ماند. تأخیر بلندتر یعنی پنجره‌های باز بیشتر و state بیشتری برای ذخیره و checkpoint کردن.

   </details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| Micro-batch | هر trigger بازه‌ی جدید offset را به‌صورت یک batch کوچک پردازش می‌کند |
| Append mode | ردیف‌هایی را می‌نویسد که دیگر تغییر نمی‌کنند؛ aggregationها بعد از watermark نهایی می‌شوند |
| Update mode | ردیف‌های تغییرکرده از trigger قبلی را می‌نویسد |
| Complete mode | کل جدول نتیجه را می‌نویسد؛ فقط برای aggregation |
| Watermark | حد دیررسی: `max(event_time) − delay`؛ state قدیمی حذف می‌شود |
| Checkpoint | offset log، commit log و state؛ برای بازیابی لازم است |
| foreachBatch | هر micro-batch و `batchId` آن را به کد تو می‌دهد؛ به‌صورت پیش‌فرض at-least-once |
| `maxOffsetsPerTrigger` | سقف رکورد در هر batch از منبعی مثل Kafka |
| `availableNow` | داده‌ی موجود را در چند batch پردازش می‌کند، بعد متوقف می‌شود |

## 🔗 ادامه

- [[streaming-state]] — چگونگی ذخیره و بازیابی state
- [[stream-joins]] — joinها با watermark روی هر دو طرف
- [[pattern-late-data]] — مدیریت داده‌ی دیررس و خارج از ترتیب از ابتدا تا انتها
- [[kafka-exactly-once]] — معنای exactly-once در Kafka و sinkها
- [[spark-shuffle]] — چرا عملگرهای stateful shuffle می‌کنند و partitionها چه اثری دارند
- راهنمای Spark Structured Streaming برای نسخه‌ی خودت (triggerها، تنظیمات state store و metricها)
