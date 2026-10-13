---
term: "پردازش تراکنشی آنلاین (OLTP)"
term_en: "OLTP (online transaction processing)"
aliases: ["oltp", "پردازش تراکنشی"]
category: storage
short: "بار کاری شامل خواندن و نوشتن‌های کوچک، همزمان و کم‌تأخیر روی داده‌ی جاری که با تراکنش ACID محافظت می‌شود."
related: ["olap", "mvcc", "wal"]
---
## به زبان ساده (مبتدی)
OLTP صندوق فروشگاه دنیای دیجیتال است: عملیات کوچک و سریع زیاد مثل ثبت سفارش، پرداخت یا تغییر آدرس که هرکدام باید همان لحظه درست انجام شود. مشتری‌های زیادی هم‌زمان این کار را می‌کنند و هر عملیات فقط چند رکورد را لمس می‌کند.

## نحوه‌ی کار (متوسط)
ویژگی‌ها: ذخیره‌سازی سطری، index از نوع B-tree، schema نرمال (3NF)، تراکنش ACID کوتاه، همزمانی بالا، latency میلی‌ثانیه‌ای و داده‌ی جاری (نه تاریخچه). سیستم‌ها: PostgreSQL، MySQL، Oracle، SQL Server و SQL توزیع‌شده مثل CockroachDB یا Spanner.

```
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;      -- atomic, durable (WAL), isolated (MVCC/locks)

-- typical: point lookups by PK, small indexed ranges, ms latency, thousands of TPS
```

ACID: atomicity (همه یا هیچ)، consistency (برقراری قیود)، isolation (تراکنش‌های هم‌زمان مزاحم هم نیستند، بسته به سطح isolation) و durability (داده‌ی commit‌شده با WAL از crash جان به در می‌برد). همروندی با MVCC و/یا قفل. دیتابیس‌های OLTP معمولاً منبع pipelineها هستند: استخراج تحلیل سنگین مستقیم از آن‌ها به اپلیکیشن آسیب می‌زند، پس از replica یا CDC به سیستم OLAP استفاده کنید.

## نگاه سنیور و نکات مصاحبه
برای مهندس داده OLTP مهم است چون منبع است: کوئری تحلیلی طولانی روی primary نزنید، از read replica یا CDC مبتنی بر لاگ استفاده کنید، سطوح isolation (read committed، repeatable read، serializable) و نیازهای استخراج snapshot سازگار را بشناسید، برای تغییرات schema که مالکش تیم اپلیکیشن است برنامه داشته باشید و حذف‌ها را مدیریت کنید. ناهنجاری‌ها بر اساس سطح isolation: dirty read، non-repeatable read، phantom، write skew. ذخیره‌ی سطری برای scan بزرگ ضعیف است و به همین دلیل تحلیل به OLAP ستونی می‌رود؛ سیستم‌های HTAP می‌کوشند هر دو را با مصالحه سرو کنند.

سؤال مصاحبه: «چرا تحلیل را مستقیم روی دیتابیس production اجرا نکنیم؟» پاسخ: scanهای سنگین با ترافیک تراکنشی برای CPU، I/O و قفل رقابت می‌کنند، ذخیره‌ی سطری برای aggregate ناکارآمد است و schema برای نوشتن نرمال شده؛ با CDC به store ستونی بهینه برای scan تکثیر می‌کنیم.
