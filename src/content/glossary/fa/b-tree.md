---
term: "درخت B (B-tree)"
term_en: "B-tree"
aliases: ["b-tree", "b+tree", "btree", "درخت b"]
category: storage
short: "درخت متوازن، پهن و مرتب روی صفحه‌های دیسک که lookup نقطه‌ای، range scan و update درجا را با O(log n) می‌دهد؛ index پیش‌فرض دیتابیس‌ها."
related: ["database-index", "lsm-tree", "oltp"]
---
## به زبان ساده (مبتدی)
فهرست انتهای یک کتاب قطور را در نظر بگیرید: به‌جای ورق‌زدن همه‌ی صفحات، با چند قدم فصل درست و بعد صفحه‌ی درست را نشان می‌دهد. B-tree چنین فهرستی است که روی دیسک مرتب نگه داشته می‌شود؛ جست‌وجو را سریع می‌کند و اجازه می‌دهد «همه‌چیز بین ۴۰ تا ۶۰» را هم بخوانید.

## نحوه‌ی کار (متوسط)
داده در صفحه‌های با اندازه‌ی ثابت (مثلاً ۸ کیلوبایت) سازمان‌دهی می‌شود. هر صفحه‌ی داخلی کلیدهای مرتب و اشاره‌گر فرزند دارد؛ صفحه‌های برگ کلید و مقدار (یا اشاره‌گر سطر). lookup از ریشه تا برگ پیمایش می‌کند، یک خواندن صفحه در هر سطح؛ با صدها کلید در هر صفحه عمق حتی برای میلیاردها سطر ۳ تا ۴ است. B+tree مقدارها را فقط در برگ‌ها نگه می‌دارد و برگ‌ها را به هم لینک می‌کند تا range scan ترتیبی باشد. درج صفحه‌ی پر را split می‌کند؛ update صفحه را درجا تغییر می‌دهد و با WAL محافظت می‌شود.

```
                 [ 30 | 60 ]                    <- root page
               /      |      \
      [10|20]     [40|50]     [70|80|90]        <- internal pages (keys + child pointers)
       / | \       / | \        / | | \
     leaf pages: sorted keys + row pointers (B+tree: leaves linked for range scans)

Fanout ~ hundreds => 3-4 levels address billions of keys; each level = one page read.
```

ویژگی‌ها: latency خواندن قابل‌پیش‌بینی، کوئری بازه‌ای و پیمایش مرتب کارآمد، update درجا و کارایی خوب برای OLTP ترکیبی خواندن/نوشتن. index در PostgreSQL، InnoDB در MySQL، SQL Server، Oracle و بسیاری از سیستم‌فایل‌هاست.

## نگاه سنیور و نکات مصاحبه
ضعف‌ها: write amplification (تغییر کل صفحه برای تغییر کوچک)، I/O تصادفی در درج با کلید تصادفی (primary key از نوع UUIDv4 باعث page split و cache miss می‌شود؛ شناسه‌های مرتب زمانی کمک می‌کنند)، fragmentation که vacuum/reindex می‌خواهد و تناسب کمتر برای throughput نوشتن بسیار بالا در مقایسه با LSM-tree. ترتیب ستون‌ها در index مرکب مهم است (قاعده‌ی پیشوند چپ) و covering index از lookup جدول جلوگیری می‌کند. موتورهای OLAP ستونی عمدتاً B-tree ندارند؛ از sparse index، zone map و ترتیب sort استفاده می‌کنند.

سؤال مصاحبه: «B-tree در برابر LSM-tree؟» پاسخ: B-tree درجا به‌روز می‌کند و خواندن قابل‌پیش‌بینی و range scan خوب دارد ولی نوشتن تصادفی و write amplification؛ LSM نوشتن را بافر و فایل‌های مرتب را ادغام می‌کند و throughput نوشتن و فشرده‌سازی بهتر می‌دهد به بهای read amplification و سربار compaction.
