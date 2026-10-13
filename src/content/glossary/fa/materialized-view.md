---
term: "ویوی مادی‌شده (Materialized view)"
term_en: "Materialized view"
aliases: ["materialized view", "mv", "ویو مادی‌شده", "pre-aggregation", "rollup"]
category: sql
short: "ویویی که نتیجه‌ی کوئری‌اش فیزیکی ذخیره و تازه‌سازی می‌شود؛ حافظه و نگهداری را با خواندن بسیار سریع‌تر معاوضه می‌کند."
related: ["olap", "incremental-processing", "cardinality"]
---
## به زبان ساده (مبتدی)
ویوی معمولی سؤالی ذخیره‌شده است که هر بار از صفر جواب داده می‌شود. materialized view جواب را هم ذخیره می‌کند، مثل جدول خلاصه‌ی از پیش محاسبه‌شده. خواندنش آنی است ولی وقتی داده‌ی زیرین عوض شود جواب ذخیره‌شده باید به‌روز شود.

## نحوه‌ی کار (متوسط)
view معمولی فقط یک کوئری ذخیره‌شده است. materialized view نتیجه را ذخیره می‌کند تا داشبوردها به‌جای اسکن میلیاردها سطر یک جدول کوچک از پیش تجمیع‌شده را بخوانند. راهبردهای refresh: کامل (محاسبه‌ی دوباره زمان‌بندی‌شده یا درخواستی)، افزایشی (فقط تغییرات) و نگهداری استریمی/بلادرنگ هنگام insert (MV در ClickHouse مثل trigger روی insert به جدول مقصد عمل می‌کند؛ StarRocks/Snowflake/BigQuery خودکار نگه می‌دارند با محدودیت در SQL پشتیبانی‌شده).

```
-- ClickHouse: incrementally maintained on insert
CREATE MATERIALIZED VIEW daily_revenue_mv
ENGINE = SummingMergeTree ORDER BY (dt, country) AS
SELECT toDate(ts) AS dt, country, sum(amount) AS revenue
FROM orders GROUP BY dt, country;

-- Postgres: full recompute on demand
CREATE MATERIALIZED VIEW daily_revenue AS SELECT ...;
REFRESH MATERIALIZED VIEW CONCURRENTLY daily_revenue;
```

موتورها می‌توانند کوئری را خودکار برای استفاده از MV منطبق بازنویسی کنند (StarRocks، BigQuery، Oracle). هزینه: فضا، محاسبه‌ی refresh و کهنگی بین refreshها.

## نگاه سنیور و نکات مصاحبه
جزئیات مهم: هر aggregateای افزایشی قابل نگهداری نیست (distinct به sketch نیاز دارد، میانه ترکیب‌پذیر نیست)؛ delete و update در جدول پایه نگهداری افزایشی را پیچیده می‌کند (MV در ClickHouse فقط insert می‌بیند پس اصلاح‌ها پردازش مجدد می‌خواهند)؛ مصالحه‌ی سازگاری در برابر تازگی؛ MV روی joinهای بزرگ refresh گران دارد؛ زنجیره‌ی MVها پیچیدگی عملیاتی می‌سازد. در ELT مدل‌های incremental در dbt و جدول‌های rollup از پیش تجمیع‌شده همین هدف را با کنترل صریح منطق و backfill دنبال می‌کنند.

سؤال مصاحبه: «داشبورد روی ۱۰ میلیارد سطر کند است؛ گزینه‌ها؟» پاسخ: pre-aggregate با materialized view یا جدول rollup در grain مورد نیاز داشبورد و نگهداری افزایشی، به‌علاوه‌ی تنظیم پارتیشن و sort key؛ داده‌ی خام را برای کوئری‌های ad hoc نگه می‌دارم و کهنگی محدود را می‌پذیرم.
