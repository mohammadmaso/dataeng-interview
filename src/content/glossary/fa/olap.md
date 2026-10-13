---
term: "پردازش تحلیلی آنلاین (OLAP)"
term_en: "OLAP (online analytical processing)"
aliases: ["olap", "پردازش تحلیلی", "مکعب olap"]
category: storage
short: "بار کاری شامل تجمیع‌ها و scanهای پیچیده روی حجم بزرگ داده‌ی تاریخی که برای throughput و سرعت خواندن بهینه می‌شود."
related: ["oltp", "columnar-storage", "star-schema"]
---
## به زبان ساده (مبتدی)
اگر OLTP صندوق فروشگاه است، OLAP حسابداری است که رسیدهای کل سال را تحلیل می‌کند: کدام محصول بیشتر فروخت، فروش بر اساس منطقه و ماه چطور تغییر کرد. افراد کمتری سؤال می‌پرسند ولی هر سؤال حجم عظیمی از داده را می‌بیند و ممکن است به‌جای میلی‌ثانیه چند ثانیه طول بکشد.

## نحوه‌ی کار (متوسط)
ویژگی‌ها: ذخیره‌سازی ستونی، مدل‌های ابعادی غیرنرمال (star schema)، scan بزرگ با aggregate، ingestion به‌صورت batch یا micro-batch، داده‌ی تاریخی، و کوئری‌های هم‌زمانِ نسبتاً کم ولی سنگین. سیستم‌ها: Snowflake، BigQuery، Redshift، ClickHouse، StarRocks، Druid، Pinot، DuckDB و موتورهای lakehouse روی Parquet.

```
SELECT d.month, c.region, p.category,
       SUM(f.net_amount) AS revenue, COUNT(DISTINCT f.customer_sk) AS buyers
FROM fct_sales f
JOIN dim_date d ON ... JOIN dim_customer c ON ... JOIN dim_product p ON ...
WHERE d.year = 2024
GROUP BY d.month, c.region, p.category;     -- scans millions/billions of rows
```

عملیات شناخته‌شده از مکعب‌های OLAP: slice، dice، drill-down، roll-up، pivot. موتورهای مدرن سرعت را با اجرای vectorized، فشرده‌سازی، پارتیشن/data skipping، materialized view و pre-aggregation و موازی‌سازی MPP بین نودها می‌گیرند. OLAP بلادرنگ (ClickHouse، Druid، Pinot، StarRocks) ingestion کم‌تأخیر و کوئری زیر ثانیه برای تحلیل رو به کاربر اضافه می‌کند.

## نگاه سنیور و نکات مصاحبه
جدول مقایسه‌ی OLTP و OLAP سؤال کلاسیک مصاحبه است؛ ظرافت اضافه کنید. داده با ETL/ELT یا CDC از OLTP به OLAP می‌رود؛ storeهای OLAP برای update تک‌سطری و نوشتن نقطه‌ای پرنرخ ضعیف‌اند پس تغییر با append به‌علاوه‌ی merge (ReplacingMergeTree، upsert در StarRocks/Iceberg) انجام می‌شود. انتخاب بر اساس latency هدف و همزمانی: داشبورد BI با کاربر کم به warehouse می‌خورد؛ تحلیل رو به کاربر با همزمانی بالا به موتور OLAP بلادرنگ با pre-aggregation. HTAP و «zero-ETL» ادعای محو کردن مرز دارند؛ محدودیت‌هایشان را بدانید.

سؤال مصاحبه: «تفاوت OLTP و OLAP؟» پاسخ: OLTP سطری، نرمال، تراکنش ACID کوتاه روی داده‌ی جاری با همزمانی بالاست؛ OLAP ستونی، غیرنرمال، scan تجمیعی بزرگ روی تاریخچه با کوئری سنگین کم است و معمولاً با ETL یا CDC از OLTP تغذیه می‌شود.
