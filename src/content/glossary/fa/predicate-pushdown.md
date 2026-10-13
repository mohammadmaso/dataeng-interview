---
term: "پایین‌فرستادن شرط فیلتر (Predicate pushdown)"
term_en: "Predicate pushdown"
aliases: ["predicate pushdown", "push down", "data skipping"]
category: batch
short: "فرستادن فیلتر (و انتخاب ستون) تا لایه‌ی ذخیره‌سازی تا داده‌ی کمتری خوانده و پردازش شود."
related: ["parquet", "partition", "columnar-storage"]
---
## به زبان ساده (مبتدی)
اگر از کتابدار «کتاب‌های قرمز چاپ ۲۰۲۰» را بخواهید، خیلی سریع‌تر است که فقط قفسه‌ی ۲۰۲۰ و برچسب‌های قرمز را نگاه کند، به‌جای اینکه همه‌ی کتاب‌ها را بیاورد تا خودتان فیلتر کنید. predicate pushdown یعنی فیلتر تا حد ممکن نزدیک داده اعمال شود تا بیشتر داده اصلاً خوانده نشود.

## نحوه‌ی کار (متوسط)
به‌جای خواندن همه‌چیز و فیلتر در موتور، optimizer شرط‌های `WHERE` را تا scan پایین می‌فرستد. در لایه‌ی ذخیره‌سازی این ممکن می‌کند: partition pruning (رد کل دایرکتوری)، file skipping با متادیتای table format (min/max هر فایل در Iceberg/Delta)، رد row group یا page با آمار Parquet و بررسی Bloom filter. projection pushdown هم فقط ستون‌های ارجاع‌شده را می‌خواند.

```
EXPLAIN SELECT amount FROM events
WHERE dt = '2024-05-01' AND country = 'IR';

-- look for in the plan:
--  PartitionFilters: [dt = 2024-05-01]          (partition pruning)
--  PushedFilters:    [country = IR]             (pushed to Parquet reader)
--  ReadSchema:       struct<amount, country>    (projection pushdown)
```

فقط وقتی کار می‌کند که شرط قابل pushdown باشد: مقایسه‌های ساده روی ستون خام. پیچیدن ستون در تابع (`WHERE DATE(ts) = ...`، `CAST`) یا استفاده از UDF اغلب آن را می‌شکند و اثربخشی به چیدمان داده بستگی دارد: اگر ستون فیلتر تصادفی پخش باشد آمار min/max چیزی را رد نمی‌کند.

## نگاه سنیور و نکات مصاحبه
با sort یا cluster بر اساس ستون‌های فیلتر رایج (Z-order، کلیدهای مرتب، ORDER BY در ClickHouse)، نگه‌داشتن ستون پارتیشن در فیلترها و انتخاب نوع‌های ارزان برای مقایسه، pushdown را مؤثر کنید. pushdown به منابع JDBC هم می‌رسد (فیلتر را خود دیتابیس انجام می‌دهد) و از میان join هم رد می‌شود (dynamic partition pruning و runtime filter مقدارهای طرف build را به scan طرف probe می‌فرستند). با خواندن plan برای فیلترهای pushed و بایت‌های واقعاً خوانده‌شده دیباگ کنید، نه با اعتماد به متن کوئری.

سؤال مصاحبه: «چرا با فیلتر تاریخ کل جدول scan شد؟» پاسخ: احتمالاً فیلتر ستون پارتیشن را در تابع پیچیده یا type mismatch دارد، جدول روی آن پارتیشن یا cluster نشده، یا آمار نیست؛ plan را برای partition filters می‌بینم و شرط را روی ستون خام می‌نویسم.
