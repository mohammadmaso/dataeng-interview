---
term: "حذف تکراری (Deduplication)"
term_en: "Deduplication"
aliases: ["dedup", "deduplication", "dedupe", "حذف رکورد تکراری"]
category: quality
short: "حذف رکوردهای تکراری تا هر موجودیت یا رویداد یک بار بیاید، معمولاً بر اساس کلید و نگه‌داشتن جدیدترین یا اولین نسخه."
related: ["idempotency", "upsert", "window-function", "bloom-filter"]
---
## به زبان ساده (مبتدی)
اگر یک فاکتور دو بار برایتان فرستاده شود فقط یک بار می‌خواهید بپردازید. deduplication پاک‌سازی‌ای است که نسخه‌های تکراری یک چیز را پیدا می‌کند و یکی را نگه می‌دارد. اهمیتش به این است که pipelineها، retryها و سیستم‌های مبدأ اغلب یک رکورد را بیش از یک بار تحویل می‌دهند.

## نحوه‌ی کار (متوسط)
تکراری‌ها از تحویل at-least-once، retry، پنجره‌های افزایشی هم‌پوشان، replay/backfill، دابل‌کلیک و باگ منبع می‌آیند. اول تعریف کنید «یکسان» یعنی چه: برابری دقیق سطر، شناسه‌ی طبیعی/رویداد یا تطبیق فازی (entity resolution) با قواعد شباهت. برای کلید دقیق: `ROW_NUMBER()` به‌ازای هر کلید با نگه‌داشتن جدیدترین، `DISTINCT`، `GROUP BY` با aggregate یا `MERGE` در مقصد کلیددار.

```
-- Keep the latest version per key (deterministic tiebreaker!)
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
           PARTITION BY event_id ORDER BY updated_at DESC, ingested_at DESC) AS rn
  FROM raw_events) WHERE rn = 1;

-- Spark streaming dedup with bounded state
df.withWatermark("event_time", "1 hour").dropDuplicates(["event_id", "event_time"])
```

dedup استریمی کلیدهای دیده‌شده را در state نگه می‌دارد که با واترمارک یا TTL محدود می‌شود، پس تکراری‌هایی که بعد از پنجره برسند از دست می‌روند. فیلترهای احتمالاتی (Bloom filter) حافظه را کم می‌کنند ولی ممکن است رویداد جدید را اشتباهاً حذف کنند. در storeها upsert کلیددار یا موتورهایی مثل ReplacingMergeTree در زمان merge به‌صورت نهایی dedupe می‌کنند.

## نگاه سنیور و نکات مصاحبه
ترجیح بدهید نوشتن را خودتوان کنید (dedup در sink با کلید) به‌جای پاک‌سازی بعدی. همیشه در `ORDER BY` یک tiebreaker قطعی بگذارید وگرنه اجرای مجدد ممکن است بازمانده‌ی دیگری انتخاب کند. هزینه را بدانید: dedup سراسری روی داده‌ی بزرگ shuffle بر اساس کلید می‌خواهد؛ وقتی تکراری‌ها نزدیک هم‌اند با dedup محلی پارتیشن کمش کنید. درباره‌ی تکراری‌های دورریخته‌شده تصمیم بگیرید (جدول audit) و نرخ تکراری را به‌عنوان سیگنال کیفیت پایش کنید: جهش یعنی retry storm یا replay بالادست. برای entity resolution (یک مشتری با املاهای مختلف) از blocking key، امتیاز شباهت و قواعد survivorship استفاده کنید که بسیار سخت‌تر از dedup کلیدی است.

سؤال مصاحبه: «استریم Kafka را چطور در جدول dedupe می‌کنی؟» پاسخ: شناسه‌ی رویداد پایدار حمل می‌کنم، sink را upsert خودتوان روی همان شناسه می‌کنم تا replay و retry همگرا شوند و در صورت نیاز عملگر dedup استریمی کران‌دار با واترمارک برای حذف زودهنگام با state محدود.
