---
term: "چک‌پوینت (Checkpoint)"
term_en: "Checkpoint"
aliases: ["checkpoint", "چک‌پوینت", "savepoint"]
category: streaming
short: "snapshot سازگار از state و موقعیت ورودی‌های یک job که برای بازیابی بعد از خرابی بدون گم‌شدن یا دوبار شمارش استفاده می‌شود."
related: ["exactly-once", "wal", "backpressure"]
---
## به زبان ساده (مبتدی)
ذخیره‌ی بازی ویدیویی را فرض کنید. اگر بازی crash کند آخرین ذخیره را بارگذاری می‌کنید نه از مرحله‌ی اول. checkpoint نقطه‌ی ذخیره‌ی یک job استریم است: ثبت می‌کند تا کجا خوانده و چه چیزی به خاطر دارد تا بعد از خرابی از همان‌جا ادامه دهد.

## نحوه‌ی کار (متوسط)
checkpoint، state عملگرها و offsetهای source را در یک نقطه‌ی منطقاً سازگار ثبت می‌کند. در Flink یک coordinator barrier در جریان‌ها تزریق می‌کند؛ هر عملگر وقتی barrier می‌رسد از state خود snapshot می‌گیرد (به سبک Chandy-Lamport) و ack می‌دهد؛ پس snapshot بدون توقف job سازگار است. state در ذخیره‌سازی پایدار (S3، HDFS) و با RocksDB به‌صورت افزایشی ذخیره می‌شود.

```
# Flink
execution.checkpointing.interval: 60s
execution.checkpointing.mode: EXACTLY_ONCE
state.checkpoints.dir: s3://bucket/ckpt
# Spark Structured Streaming
df.writeStream.option("checkpointLocation", "s3://bucket/ckpt/job1").start()
```

هنگام خرابی job دوباره بالا می‌آید، state را از آخرین checkpoint تکمیل‌شده بازیابی می‌کند، sourceها را به offsetهای ذخیره‌شده برمی‌گرداند و replay می‌کند. همراه با sink تراکنشی یا خودتوان اثر exactly-once حاصل می‌شود. savepoint نسخه‌ی دستی و قابل‌حمل checkpoint برای upgrade و migration است. Spark offsetها و state هر query را در دایرکتوری checkpoint نگه می‌دارد.

## نگاه سنیور و نکات مصاحبه
checkpoint سربار و latency را با زمان بازیابی معاوضه می‌کند: فاصله‌ی کوتاه‌تر یعنی replay کمتر ولی I/O و فشار بیشتر؛ فاصله‌ی طولانی یعنی بازیابی طولانی‌تر و خروجی تکراری بیشتر برای sinkهای غیرتراکنشی. خروجی در sinkهای two-phase-commit فقط هنگام commit دیده می‌شود پس latency انتها به انتها تقریباً برابر checkpoint interval است. حالت‌های خرابی: timeout در backpressure، state عظیم و checkpoint کند، state ناسازگار پس از تغییر کد (uid عملگر مهم است) و دایرکتوری checkpoint که با منطق متفاوت قابل‌استفاده مجدد نیست.

سؤال مصاحبه: «Flink چطور از crash نود بازیابی می‌شود؟» پاسخ: state عملگرها را از آخرین checkpoint کامل بازیابی می‌کند، sourceها را به offset ذخیره‌شده برمی‌گرداند و رکوردهای پس از آن را replay می‌کند؛ با sink خودتوان یا تراکنشی خروجی اثر exactly-once دارد.
