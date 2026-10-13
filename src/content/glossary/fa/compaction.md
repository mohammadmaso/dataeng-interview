---
term: "فشرده‌سازی و ادغام فایل‌ها (Compaction)"
term_en: "Compaction"
aliases: ["compaction", "کامپکشن", "ادغام فایل‌های کوچک"]
category: lakehouse
short: "ادغام فایل‌ها یا سگمنت‌های کوچک زیاد در تعداد کمتری فایل بزرگ‌تر برای سریع‌تر شدن خواندن و آزادکردن فضا."
related: ["small-files-problem", "iceberg", "lsm-tree", "parquet"]
---
## به زبان ساده (مبتدی)
کشوی پر از یادداشت‌های کاغذی ریز را تصور کنید؛ پیدا کردن هر چیز زمان زیادی می‌برد. پس می‌نشینید و یادداشت‌ها را در چند دفترچه‌ی منظم کپی می‌کنید. compaction همین کار را برای فایل‌های داده می‌کند: تکه‌های کوچک زیاد در چند فایل بزرگ ادغام می‌شوند تا خواندن سریع‌تر شود و سیستم متادیتای کمتری نگه دارد.

## نحوه‌ی کار (متوسط)
نوشتن‌های مکرر استریم یا micro-batch فایل‌های ریز زیادی (یا سگمنت LSM یا partهای ClickHouse) می‌سازند. هر فایل هزینه‌ی open/list، متادیتا و یک task در زمان خواندن اضافه می‌کند. compaction آن‌ها را در فایل‌هایی نزدیک به اندازه‌ی هدف بازنویسی می‌کند (برای Parquet روی object storage معمولاً ۱۲۸ مگابایت تا ۱ گیگابایت).

```
-- Iceberg (Spark)
CALL catalog.system.rewrite_data_files(
  table => 'db.events',
  options => map('target-file-size-bytes', '536870912'));

-- Delta Lake
OPTIMIZE events WHERE dt >= '2024-05-01';

-- ClickHouse: force merge of parts
OPTIMIZE TABLE events FINAL;
```

در storeهای مبتنی بر LSM (Cassandra، RocksDB، MergeTree در ClickHouse) compaction یک فرایند پس‌زمینه‌ی داخلی است که runهای مرتب را ادغام می‌کند، مقادیر بازنویسی‌شده و tombstoneها را حذف می‌کند و read amplification را محدود می‌سازد. در table formatها (Iceberg، Delta، Hudi) یک job نگهداری است که زمان‌بندی می‌کنید، اغلب همراه با sort یا clustering و snapshot expiry.

## نگاه سنیور و نکات مصاحبه
compaction هزینه‌ی write amplification و محاسبه را با سرعت خواندن معاوضه می‌کند. ریسک‌ها: رقابت با writerها (Iceberg از optimistic concurrency استفاده می‌کند و compaction طولانی ممکن است conflict و retry بخورد)، بازنویسی بی‌دلیل پارتیشن‌های بزرگ (فقط پارتیشن‌های جدید یا کثیف را compact کنید)، و ماندن فایل‌های قدیمی تا snapshot expiry و پاک‌سازی orphan file. در سیستم‌های LSM بدهی compaction باعث پرش latency و space amplification می‌شود؛ استراتژی (size-tiered یا leveled) را بر اساس workload انتخاب کنید.

سؤال مصاحبه: «job استریم هر دقیقه در Iceberg می‌نویسد و کوئری‌ها کند شده‌اند؛ چرا؟» پاسخ: مشکل small files است؛ compaction با اندازه‌ی هدف روی پارتیشن‌های داغ، snapshot expiry و بزرگ‌تر کردن commit interval.
