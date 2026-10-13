---
term: "پارکت (Parquet)"
term_en: "Parquet"
aliases: ["parquet", "پارکت", "فایل parquet"]
category: storage
short: "فرمت فایل ستونی و متن‌باز با فشرده‌سازی، encoding و آمار min/max؛ پیش‌فرض دریاچه‌های داده‌ی تحلیلی."
related: ["columnar-storage", "predicate-pushdown", "compaction", "schema-evolution"]
---
## به زبان ساده (مبتدی)
جدولی را تصور کنید که روی کاغذ چاپ شده. Parquet به‌جای ذخیره‌ی سطر به سطر، هر ستون را جدا نگه می‌دارد؛ مثل اینکه همه‌ی قیمت‌ها در یک برگه باشند و همه‌ی نام‌ها در برگه‌ای دیگر. اگر سؤالی فقط قیمت‌ها را بخواهد، فقط همان برگه را می‌خوانید. مقدارهای هم‌نوع کنار هم وقتی فشرده می‌شوند هم خوب کوچک می‌شوند.

## نحوه‌ی کار (متوسط)
فایل Parquet به row groupها تقسیم می‌شود؛ در هر کدام هر ستون به‌صورت column chunk و تقسیم به pageها ذخیره می‌شود. مقدارها encode می‌شوند (dictionary، run-length، delta) و سپس فشرده (Snappy، Zstd). footer شامل schema و آمار هر column chunk است.

```
File
 ├─ Row group 1 (~128 MB)
 │   ├─ Column chunk: user_id  -> pages (dictionary/RLE, compressed)
 │   ├─ Column chunk: amount   -> pages
 │   └─ stats per chunk/page: min, max, null_count
 ├─ Row group 2 ...
 └─ Footer: schema, row-group offsets, statistics
```

خواننده‌ها از projection pushdown (خواندن فقط ستون‌های لازم) و predicate pushdown (رد کردن row group یا pageهایی که min/max آن‌ها با فیلتر جور نیست) استفاده می‌کنند. داده‌ی تودرتو را با encoding سطح تکرار/تعریف مدل Dremel پشتیبانی می‌کند. فایل‌های Parquet تغییرناپذیرند؛ update یعنی بازنویسی فایل که table formatها (Iceberg، Delta، Hudi) با متادیتا و delete file مدیریت می‌کنند.

## نگاه سنیور و نکات مصاحبه
تنظیم عملی: فایل‌های حدود ۱۲۸ مگابایت تا ۱ گیگابایت و row group متناسب با موازی‌سازی خواننده؛ بر اساس ستون‌های فیلتر رایج sort یا cluster کنید تا آمار min/max گزینشی باشد؛ از متورم‌شدن dictionary با cardinality بسیار بالا و از فایل‌های ریز پرهیز کنید. محدودیت‌ها: برای point lookup و updateهای مکرر ضعیف است، appendهای کوچک فایل زیاد می‌سازند، و schema evolution به نحوه‌ی تطبیق ستون (با نام یا موقعیت) بستگی دارد که بین موتورها فرق می‌کند. ORC جایگزین نزدیک است؛ Arrow همتای درون‌حافظه‌ای.

سؤال مصاحبه: «چرا برای تحلیل Parquet و نه CSV؟» پاسخ: چیدمان ستونی فقط ستون‌های لازم را می‌خواند، encoding و فشرده‌سازی حجم و I/O را کم می‌کنند، آمار امکان data skipping می‌دهد و schema داخلی ابهام parse را از بین می‌برد.
