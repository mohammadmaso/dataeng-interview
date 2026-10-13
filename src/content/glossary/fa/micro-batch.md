---
term: "پردازش میکروبچ (Micro-batch)"
term_en: "Micro-batch processing"
aliases: ["micro-batch", "micro-batching", "میکروبچ"]
category: streaming
short: "استریم به‌صورت دنباله‌ای از jobهای batch کوچک که هر چند ثانیه یا دقیقه اجرا می‌شود؛ latency را با throughput و سادگی معاوضه می‌کند."
related: ["checkpoint", "backpressure", "exactly-once"]
---
## به زبان ساده (مبتدی)
اتوبوسی که هر ۵ دقیقه حرکت می‌کند هر کس را که در ایستگاه جمع شده سوار می‌کند، در حالی که تاکسی برای هر مسافر فوراً حرکت می‌کند. micro-batch داده را برای لحظه‌ای کوتاه جمع و با هم پردازش می‌کند؛ کارآمد است ولی آنی نیست.

## نحوه‌ی کار (متوسط)
هر trigger offsetها/فایل‌های جدید از batch قبلی را می‌خواند، یک plan batch معمولی روی آن اجرا می‌کند، خروجی را می‌نویسد و پیشرفت را در checkpoint commit می‌کند. چون هر batch قطعی است و از offsetهای ثبت‌شده قابل replay است، exactly-once با sink خودتوان یا تراکنشی ساده است. latency حداقل برابر فاصله‌ی trigger به‌علاوه‌ی سربار job است (معمولاً صدها میلی‌ثانیه تا چند ثانیه).

```
# Spark Structured Streaming: micro-batch every 30 s
(spark.readStream.format("kafka").option("subscribe", "events").load()
  .writeStream.format("iceberg")
  .trigger(processingTime="30 seconds")
  .option("checkpointLocation", "s3://b/ckpt/events")
  .start("db.events"))
# vs Flink: record-at-a-time (true streaming), latency in ms
```

موتورهای استریم بومی (Flink، Kafka Streams) هر رکورد را با عملگرهای پیوسته و state هنگام رسیدن پردازش می‌کنند و latency کمتر (میلی‌ثانیه) می‌دهند. Spark micro-batch (پیش‌فرض) و حالت continuous با پشتیبانی محدود دارد.

## نگاه سنیور و نکات مصاحبه
مصالحه‌ها: micro-batch throughput بالا، بهینه‌سازی‌های سبک batch، استفاده‌ی مجدد آسان از کد batch و بازیابی ساده‌تر می‌دهد؛ هزینه‌ها کف latency و سربار هر batch (زمان‌بندی، فایل‌های ریز در sink) است. trigger کوتاه فایل ریز می‌سازد و بلند latency اضافه می‌کند؛ بر اساس SLA انتخاب کنید و compact کنید. پاک‌سازی state به واترمارک وابسته است. پنجره‌های event-time مثل موتورهای رکورد‌به‌رکورد کار می‌کنند. بیشتر نیازهای «realtime» کسب‌وکار با latency چند ثانیه تا چند دقیقه برآورده می‌شود؛ نیازهای واقعاً کم‌تأخیر (fraud، alert در میلی‌ثانیه) موتورهای شبیه Flink را توجیه می‌کنند.

سؤال مصاحبه: «Spark Structured Streaming در برابر Flink؟» پاسخ: Spark micro-batch روی موتور batch است که برای throughput، کد یکپارچه‌ی batch/stream و latency متوسط مناسب است؛ Flink موتور استریم بومی با مدیریت دقیق‌تر state، قابلیت‌های event-time و latency میلی‌ثانیه‌ای است؛ بر اساس SLA و مهارت تیم انتخاب می‌کنم.
