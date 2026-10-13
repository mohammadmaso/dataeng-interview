---
term: "دلتا لیک (Delta Lake و Hudi)"
term_en: "Delta Lake (and Hudi)"
aliases: ["delta lake", "delta", "hudi", "دلتا لیک", "open table format"]
category: lakehouse
short: "table format متن‌باز که لاگ تراکنش روی فایل‌های Parquet می‌افزاید تا ACID، time travel، اعمال schema و upsert کارآمد بدهد."
related: ["iceberg", "lakehouse", "time-travel", "compaction"]
---
## به زبان ساده (مبتدی)
دفترچه‌ی بانکی را در نظر بگیرید: هر واریز و برداشت یک خط به ترتیب است و موجودی همان چیزی است که با خواندن خط‌ها به دست می‌آید. Delta Lake چنین لاگی از تغییرات فایل‌های جدول نگه می‌دارد تا محتوای فعلی جدول (و هر نسخه‌ی گذشته) به‌شکل قابل‌اعتماد بازسازی شود.

## نحوه‌ی کار (متوسط)
Delta داده را به‌صورت Parquet و یک لاگ تراکنش (`_delta_log`) از commitهای JSON که فایل‌های افزوده و حذف‌شده را فهرست می‌کنند به‌علاوه‌ی فایل‌های checkpoint دوره‌ای ذخیره می‌کند. خواننده لاگ را replay می‌کند تا بفهمد کدام فایل‌ها یک نسخه را می‌سازند. writer با ساخت اتمیک ورودی بعدی لاگ (put-if-absent) و optimistic concurrency با بررسی تعارض commit می‌کند. نتیجه: تراکنش ACID، snapshot isolation، time travel (خواندن نسخه‌ی N یا زمان T) و اعمال/تکامل schema.

```
my_table/
  _delta_log/
    00000000000000000000.json      <- commit 0: add file a.parquet
    00000000000000000001.json      <- commit 1: add b.parquet, remove a.parquet
    00000000000000000010.checkpoint.parquet   <- periodic snapshot of log state
  part-0001.parquet  part-0002.parquet ...

-- table state = replay of the log;  commit = atomically create the next log file
MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
OPTIMIZE t ZORDER BY (user_id);  VACUUM t RETAIN 168 HOURS;
```

قابلیت‌ها: `MERGE`/`UPDATE`/`DELETE`، deletion vector، Z-ordering/liquid clustering برای data skipping، compaction با `OPTIMIZE`، `VACUUM` برای حذف فایل‌های بدون ارجاع، change data feed و خواندن/نوشتن استریم. Apache Hudi برای upsert و pull افزایشی با جدول‌های copy-on-write و merge-on-read و timeline هدف‌گذاری شده؛ Iceberg طراحی درخت متادیتا دارد.

## نگاه سنیور و نکات مصاحبه
این سه فرمت در قابلیت به هم نزدیک می‌شوند ولی طراحی متفاوت دارند: لاگ خطی Delta (ساده، list سریع، پیوند قوی با Spark/Databricks هرچند پشتیبانی گسترده)، درخت snapshot/manifest در Iceberg (hidden partitioning، بی‌طرفی نسبت به موتور، partition evolution)، و ایندکس سطح رکورد و timeline در Hudi (upsert/CDC قوی). لایه‌های interop (UniForm، XTable) می‌توانند یکی را مثل دیگری عرضه کنند. ریسک‌های عملیاتی: رشد لاگ بدون checkpoint، retention در `VACUUM` کوتاه‌تر از نیاز خواننده‌ها/time travel که کوئری را می‌شکند، تعارض writerهای هم‌زمان روی فایل‌های هم‌پوشان، و فایل‌های ریز بدون OPTIMIZE. بر اساس اکوسیستم، پشتیبانی کاتالوگ و workload (upsert استریم یا append-only تحلیلی) انتخاب کنید.

سؤال مصاحبه: «Delta روی S3 چطور commit اتمیک می‌گیرد؟» پاسخ: فایل‌های داده تغییرناپذیرند؛ commit ساخت اتمیک یک فایل لاگ بعدی است (با conditional put یا سرویس قفل برای storeهای بدون آن) و خواننده‌ها state جدول را از لاگ می‌سازند، پس تراکنش یا کامل دیده می‌شود یا اصلاً.
