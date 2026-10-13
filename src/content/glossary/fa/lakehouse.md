---
term: "لیک‌هاوس (Lakehouse)"
term_en: "Lakehouse"
aliases: ["lakehouse", "دیتا لیک‌هاوس", "لیک‌هاوس"]
category: lakehouse
short: "معماری ترکیب ذخیره‌سازی ارزان و فرمت‌باز data lake با قابلیت‌های انبار داده: جدول ACID، اعمال schema و SQL سریع."
related: ["data-lake", "data-warehouse", "iceberg", "medallion-architecture"]
---
## به زبان ساده (مبتدی)
یک انبار بزرگ و ارزان را تصور کنید که هر چیزی را می‌شود در آن ریخت؛ حالا با سیستم انبارداری خوب و قوانینی که پیدا کردن اقلام را سریع و اعتماد به قفسه‌ها را ممکن می‌کند. lakehouse به data lake همین نظم را می‌دهد تا یک جا هم پردازش داده‌ی بزرگ و هم تحلیل قابل‌اعتماد ارائه کند.

## نحوه‌ی کار (متوسط)
در گذشته data lake داشتید (ارزان و منعطف ولی «باتلاق» نامطمئن) و data warehouse (سریع و کنترل‌شده ولی انحصاری و گران) و ETL بین آن‌ها کپی می‌کرد. lakehouse یک table format تراکنشی (Iceberg، Delta Lake، Hudi) را روی فایل‌های Parquet در object storage می‌گذارد. لایه‌ی متادیتا commit ACID، snapshot و time travel، اعمال و تکامل schema و pruning کارآمد می‌دهد؛ ذخیره‌سازی و محاسبه جدا هستند و چند موتور از همان جدول‌ها استفاده می‌کنند.

```
Engines:   Spark | Trino | Flink | StarRocks | DuckDB ...
              │         (open table format API)
Table fmt: Iceberg / Delta / Hudi   (snapshots, ACID, schema evolution)
              │
Files:     Parquet / ORC on object storage (S3, GCS, ADLS, MinIO)
Catalog:   Hive Metastore / Glue / Nessie / REST catalog
```

جریان معمول: ingest به bronze، پالایش به silver و gold، سرو BI با موتور SQL و ML مستقیم از همان داده، با کاتالوگ و governance (کنترل دسترسی، lineage) روی آن.

## نگاه سنیور و نکات مصاحبه
مزایای واقعی: فرمت‌باز (قفل‌شدن کمتر به vendor)، یک نسخه داده برای BI و ML و استریم، و محاسبه‌ی elastic. مصالحه‌ها: نگهداری (compaction، snapshot expiry، پاک‌سازی orphan file) با شماست، همروندی خوش‌بینانه است (conflict در commit جدول‌های داغ)، latency اشیا در object store یعنی point lookup و داشبوردهای زیر ثانیه اغلب به لایه‌ی serving (ClickHouse، StarRocks) یا cache نیاز دارند، و انتخاب کاتالوگ برای سازگاری چندموتوره مهم است. «Lakehouse» تا حدی بازاریابی است: دقیق بگویید کدام فرمت، کاتالوگ و موتور.

سؤال مصاحبه: «Lakehouse در برابر warehouse؟» پاسخ: warehouse ذخیره و موتور را محکم جفت می‌کند و کارایی و governance آماده می‌دهد؛ lakehouse از فایل و table format باز استفاده می‌کند تا هر موتوری بخواند، به بهای مدیریت نگهداری، کاتالوگ و گاهی لایه‌ی serving سریع‌تر برای کوئری کم‌تأخیر.
