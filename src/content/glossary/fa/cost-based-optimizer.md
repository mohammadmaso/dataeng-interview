---
term: "بهینه‌ساز مبتنی بر هزینه (CBO)"
term_en: "Cost-based optimizer"
aliases: ["cbo", "cost-based optimizer", "query planner", "بهینه‌ساز کوئری", "آمار جدول"]
category: sql
short: "بخش planner کوئری که با آمار جدول هزینه‌ها را تخمین می‌زند و ارزان‌ترین plan اجرا را شامل ترتیب و نوع join انتخاب می‌کند."
related: ["predicate-pushdown", "broadcast-join", "database-index"]
---
## به زبان ساده (مبتدی)
GPS مسیرهای ممکن را مقایسه می‌کند و بر اساس طول جاده‌ها و ترافیک سریع‌ترین را برمی‌گزیند. بهینه‌ساز کوئری همین را برای کوئری SQL انجام می‌دهد: راه‌های مختلف اجرا را بررسی می‌کند و با تخمین حجم داده‌ی هر مرحله ارزان‌ترین را انتخاب می‌کند.

## نحوه‌ی کار (متوسط)
SQL می‌گوید چه می‌خواهید نه چطور. optimizer planهای معادل منطقی را فهرست می‌کند: ترتیب join، الگوریتم join (nested loop، hash، sort-merge، broadcast)، مسیر دسترسی (ایندکس یا scan)، predicate و projection pushdown، partition pruning و محل aggregate. آن‌ها را با مدل هزینه‌ای که از آمار (تعداد سطر، distinct، histogram، min/max، کسر null) تغذیه می‌شود امتیاز می‌دهد و کمترین هزینه‌ی تخمینی را انتخاب می‌کند. بازنویسی‌های قاعده‌محور (pushdown، constant folding) پیش از انتخاب‌های مبتنی بر هزینه اجرا می‌شوند.

```
-- Collect statistics so the optimizer can estimate row counts
ANALYZE TABLE sales COMPUTE STATISTICS FOR ALL COLUMNS;     -- Spark
ANALYZE sales;                                              -- Postgres

EXPLAIN (ANALYZE, BUFFERS) SELECT ...;     -- compare estimated vs actual rows
-- Big gap between "rows=100" estimated and 10,000,000 actual => bad plan risk
```

تخمین cardinality نقطه‌ی ضعف است: خطاها در joinها انباشته می‌شوند، به‌ویژه با ستون‌های همبسته یا skew، و optimizer ممکن است nested loop را روی میلیون‌ها سطر یا broadcast را روی جدول بزرگ انتخاب کند. موتورها با re-optimization تطبیقی/زمان اجرا (AQE در Spark)، runtime filter و نمونه‌گیری جبران می‌کنند.

## نگاه سنیور و نکات مصاحبه
بیشتر داستان‌های «چرا کوئری کند است» از planهای بد به‌خاطر آمار کهنه یا مفقود، شرط‌های همبسته، UDFهای مات برای optimizer، یا joinهای زیاد برای جست‌وجوی کامل (heuristic جایگزین می‌شود) می‌آیند. ابزارها: `EXPLAIN`/`EXPLAIN ANALYZE`، مقایسه‌ی سطرهای تخمینی و واقعی، تازه‌سازی آمار بعد از بارگذاری بزرگ، hint با احتیاط، آمار گسترده یا histogram، بازنویسی کمکی (پرهیز از تابع روی ستون فیلتر). در جدول‌های lakehouse آمار سطح فایل در manifestها حتی بدون آمار CBO به pruning کمک می‌کند؛ Trino/Spark برای ترتیب join خوب باید آمار را صریح جمع کنند.

سؤال مصاحبه: «planner استراتژی join بدی انتخاب کرده؛ چه می‌کنی؟» پاسخ: سطرهای تخمینی و واقعی را در plan مقایسه می‌کنم، آمار را تازه یا اضافه می‌کنم، شرط‌های non-sargable را اصلاح می‌کنم و بعد سراغ hint یا بازنویسی می‌روم؛ در Spark AQE را هم فعال می‌کنم تا استراتژی در زمان اجرا تطبیق یابد.
