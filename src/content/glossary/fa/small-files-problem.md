---
term: "مشکل فایل‌های کوچک (Small files problem)"
term_en: "Small files problem"
aliases: ["small files", "فایل‌های کوچک", "مشکل فایل کوچک", "tiny files"]
category: lakehouse
short: "وجود تعداد عظیمی فایل ریز در data lake که متادیتا و list را زیر فشار می‌گذارد و هر خواندنی را کند می‌کند."
related: ["compaction", "partition", "parquet", "iceberg"]
---
## به زبان ساده (مبتدی)
کتابخانه‌ای را تصور کنید که هر صفحه‌ی هر کتاب روی کاغذ جداگانه‌ای در کشوی متفاوتی نگه‌داری می‌شود. برای خواندن یک کتاب باید صدها کشو را باز کنید. وقتی داده به میلیون‌ها فایل ریز تکه‌تکه شود data lake همین مشکل را دارد: باز کردن و ردیابی آن‌ها از خواندن خود داده گران‌تر است.

## نحوه‌ی کار (متوسط)
هر فایل سربار ثابت دارد: یک درخواست list/open روی object storage، یک ورودی متادیتا (حافظه‌ی NameNode در HDFS، manifestهای table format)، خواندن footer برای Parquet و اغلب یک task جدا در موتور. وقتی فایل‌ها بسیار کوچک‌تر از اندازه‌ی خواندن کارآمد (ده‌ها تا صدها مگابایت) باشند سربار غالب می‌شود و throughput فرو می‌ریزد.

```
-- Symptoms
-- * 1 TB table made of 5 million files of ~200 KB
-- * Spark job spends minutes listing S3 and launching tiny tasks
-- * NameNode memory pressure on HDFS (each file/block = metadata object)

-- Causes: streaming micro-batches, over-partitioning, high spark.sql.shuffle.partitions
-- Fix: compact to ~128 MB-1 GB, fewer partitions, bigger commit interval
df.repartition(8).write.partitionBy("dt").parquet(path)   # control files per partition
```

علل: commitهای مکرر استریم، پارتیشن‌بندی بسیار ریز (مثلاً ساعت و کاربر)، تعداد زیاد پارتیشن shuffle که هرکدام یک فایل می‌نویسند، و بارگذاری‌های افزایشی فراوان. راه‌حل‌ها: نوشتن فایل کمتر و بزرگ‌تر (coalesce/repartition قبل از نوشتن، ادغام پارتیشن‌های shuffle تطبیقی)، job compaction (`OPTIMIZE`، `rewrite_data_files` در Iceberg)، پارتیشن درشت‌تر، trigger interval طولانی‌تر و auto-compaction در table formatهای مدیریت‌شده.

## نگاه سنیور و نکات مصاحبه
فایل‌های ریز متادیتا (manifest و snapshot) را هم متورم و query planning را کند می‌کنند، نه فقط خواندن را. compaction با writerها و با خودش رقابت می‌کند و تا snapshot expiry فایل‌های قدیمی را باقی می‌گذارد؛ پس زمان‌بندی و پاک‌سازی کنید. حد مقابل، فایل‌های غول‌پیکر، موازی‌سازی را می‌کُشد و update را گران می‌کند؛ بازه هدف بگیرید نه عدد. برای ingestion استریم، commit interval را به‌عنوان مصالحه‌ی latency و اندازه‌ی فایل انتخاب کنید، یا در بافر (Kafka، partهای ClickHouse) بنشانید و ناهمگام compact کنید. تعداد فایل و میانگین اندازه به‌ازای پارتیشن را به‌عنوان متریک سلامت جدول مانیتور کنید.

سؤال مصاحبه: «جدول بعد از رفتن به ingestion استریم کند شد؛ چرا؟» پاسخ: commitهای مکرر micro-batch فایل‌های ریز ساخته و list، متادیتا و تعداد taskها را باد کرده؛ به اندازه‌ی هدف compact می‌کنم، trigger interval را تنظیم می‌کنم و نگهداری زمان‌بندی‌شده با هشدار اندازه‌ی فایل می‌گذارم.
