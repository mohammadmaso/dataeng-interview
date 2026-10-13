---
term: "join از نوع Sort-merge"
term_en: "Sort-merge join"
aliases: ["sort-merge join", "merge join", "hash join", "shuffle hash join"]
category: batch
short: "join که هر دو ورودی را بر اساس کلید join sort (و shuffle) می‌کند و در یک پیمایش ادغام می‌کند؛ برای دو جدول بزرگ مقیاس‌پذیر است."
related: ["shuffle", "broadcast-join", "data-skew"]
---
## به زبان ساده (مبتدی)
دو فهرست مرتب از نام‌ها، مثلاً شرکت‌کنندگان و پرداخت‌کنندگان، را می‌توان با راه‌رفتن هم‌زمان روی هر دو مقایسه کرد: اگر نام‌ها یکی بودند ثبت می‌کنید و گرنه آن که عقب‌تر است را جلو می‌برید. لازم نیست برای هر نام کل فهرست دوم را جست‌وجو کنید. این merge join است، بعد از مرتب‌کردن فهرست‌ها.

## نحوه‌ی کار (متوسط)
الگوریتم‌ها: nested loop (مقایسه‌ی هر جفت، برای ورودی بسیار کوچک یا با ایندکس)، hash join (ساخت hash table روی طرف کوچک و probe با دیگری، سریع ولی محدود به حافظه)، sort-merge (هر دو را روی کلید sort و سپس merge؛ داده‌ی بزرگ‌تر از حافظه را هم تحمل می‌کند چون sort می‌تواند spill کند). در موتورهای توزیع‌شده sort-merge ابتدا هر دو طرف را با hash بر اساس کلید پارتیشن می‌کند (shuffle) تا کلیدهای برابر کنار هم بیایند، سپس درون هر پارتیشن sort و merge می‌کند.

```
-- Spark physical plan
SortMergeJoin [user_id], [user_id], Inner
:- Sort [user_id ASC]
:  +- Exchange hashpartitioning(user_id, 200)      <- shuffle left
+- Sort [user_id ASC]
   +- Exchange hashpartitioning(user_id, 200)      <- shuffle right

-- Merge step: two sorted cursors advance together, emitting matches
```

پیش‌فرض Spark برای equi-joinهای بزرگ-بزرگ است. هزینه: shuffle و sort هر دو طرف؛ مزیت: مقاوم در برابر محدودیت حافظه و خروجی مرتب. اگر داده از قبل با همان روش بر اساس کلید join bucket و sort شده باشد، shuffle و sort حذف می‌شود.

## نگاه سنیور و نکات مصاحبه
انتخاب استراتژی: اگر یک طرف کوچک است broadcast hash join؛ shuffle hash join وقتی یک طرف قابل‌توجه کوچک‌تر ولی بزرگ‌تر از حد broadcast است و در هر پارتیشن در حافظه جا می‌شود؛ sort-merge برای دو طرف بزرگ یا joinهای بازه‌ای غیر equi (با کار اضافه). skew بر sort-merge ضربه می‌زند چون کلید داغ در یک پارتیشن می‌افتد (data skew، AQE skew join). bucketing هر دو جدول بر اساس کلید join برای joinهای تکرارشونده از shuffle مکرر جلوگیری می‌کند. joinهای غیر equi (نابرابری، `BETWEEN`) ممکن است به broadcast nested loop یا حاصل‌ضرب دکارتی برسند که خطرناک است.

سؤال مصاحبه: «Spark دو جدول ۱ ترابایتی را چطور join می‌کند؟» پاسخ: sort-merge join: هر دو طرف با کلید در تعداد پارتیشن یکسان shuffle، در هر پارتیشن sort و سپس merge می‌شوند؛ skew، تعداد پارتیشن را بررسی می‌کنم و اگر join تکرار می‌شود bucketing را در نظر می‌گیرم.
