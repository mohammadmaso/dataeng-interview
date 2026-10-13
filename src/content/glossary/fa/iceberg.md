---
term: "آپاچی آیسبرگ (Apache Iceberg)"
term_en: "Apache Iceberg"
aliases: ["iceberg", "آیسبرگ", "apache iceberg"]
category: lakehouse
short: "table format متن‌باز که روی فایل‌های object storage، snapshot، commit ACID، hidden partitioning و schema evolution فراهم می‌کند."
related: ["lakehouse", "parquet", "compaction", "schema-evolution"]
---
## به زبان ساده (مبتدی)
فهرست یک کتابخانه را تصور کنید که دقیقاً مشخص می‌کند در هر لحظه کدام کتاب‌ها (فایل‌ها) جزو مجموعه‌اند. برای افزودن یا حذف کتاب نسخه‌ی جدید فهرست منتشر می‌کنید و خواننده‌ها از نسخه‌ای که باز کرده‌اند استفاده می‌کنند. Iceberg این فهرست هوشمند روی فایل‌های داده است تا ابزارهای مختلف بتوانند یک جدول را امن بخوانند و بنویسند.

## نحوه‌ی کار (متوسط)
جدول Iceberg متادیتا به‌علاوه‌ی فایل‌های داده است. هر commit فایل داده‌ی جدید، manifestهای جدید و یک فایل متادیتای جدید می‌نویسد و بعد اشاره‌گر کاتالوگ را اتمیک به آن عوض می‌کند (optimistic concurrency و retry در تعارض). هر commit یک snapshot است که خواندن ایزوله، time travel و rollback می‌دهد.

```
catalog ──► metadata.json (current snapshot pointer, schema, partition spec)
              └─► manifest list (per snapshot)
                    └─► manifest files (list of data files + column stats)
                          └─► data files (Parquet) + delete files

-- time travel / maintenance
SELECT * FROM events FOR VERSION AS OF 123456789;
CALL system.expire_snapshots('db.events', TIMESTAMP '2024-05-01 00:00:00');
```

manifestها آمار ستونی و مقدار پارتیشن هر فایل را نگه می‌دارند و بدون list کردن دایرکتوری، pruning سطح فایل را ممکن می‌کنند. قابلیت‌ها: hidden partitioning (تبدیل‌هایی مثل `day(ts)` و `bucket(16, id)` تا کوئری روی ستون خام فیلتر کند)، تکامل partition و schema بر اساس id ستون (rename و reorder امن)، حذف سطحی با position/equality delete file، و branch/tag. نگهداری: compaction (`rewrite_data_files`)، `expire_snapshots` و حذف orphan file.

## نگاه سنیور و نکات مصاحبه
نقاط قوت: بی‌طرف نسبت به موتور (Spark، Flink، Trino، StarRocks و ...)، تکامل امن schema/partition، مقیاس‌پذیری جدول‌های بزرگ با pruning متادیتا. تله‌ها: فایل‌های ریز و رشد متادیتا از commitهای مکرر (commit interval را تنظیم و compact کنید)، equality deleteهای ناشی از upsert استریمی که تا compaction خواندن را کند می‌کنند، تفاوت کاتالوگ‌ها در اتمیک بودن، و conflict commit با writerهای همزمان. copy-on-write (بازنویسی فایل، خواندن سریع) را در برابر merge-on-read (delete file، نوشتن سریع) بر اساس workload انتخاب کنید.

سؤال مصاحبه: «Iceberg چطور روی S3 بدون rename ACID می‌دهد؟» پاسخ: فایل‌های داده تغییرناپذیرند؛ commit اشاره‌گر متادیتای جدول را در کاتالوگ اتمیک (compare-and-swap) عوض می‌کند، پس خواننده یا snapshot قدیم یا جدید را می‌بیند و commitهای هم‌زمان متعارض retry یا شکست می‌خورند.
