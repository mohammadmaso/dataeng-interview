---
term: "ETL در برابر ELT"
term_en: "ETL vs ELT"
aliases: ["etl", "elt", "استخراج تبدیل بارگذاری"]
category: ingestion
short: "در ETL داده پیش از بارگذاری تبدیل می‌شود؛ در ELT ابتدا خام بارگذاری و داخل warehouse یا lakehouse تبدیل می‌شود."
related: ["data-warehouse", "lakehouse", "dag"]
---
## به زبان ساده (مبتدی)
مثال آشپزی: در ETL مواد را در آشپزخانه‌ی مقدماتی جداگانه خرد و پخته می‌کنید و فقط غذای آماده را به رستوران می‌رسانید. در ELT مواد خام را مستقیم به آشپزخانه‌ی بزرگ رستوران می‌برید و همان‌جا می‌پزید. دومی وقتی ممکن است که آشپزخانه‌ی اصلی قدرتمند و ارزان مقیاس‌پذیر باشد.

## نحوه‌ی کار (متوسط)
ETL زمانی پدید آمد که warehouseها گران و محاسبه کم بود، پس داده در موتور بیرونی (Informatica، SSIS، Spark) پاک می‌شد و فقط داده‌ی پالایش‌شده بارگذاری می‌شد. ELT با warehouseها و lakehouseهای مقیاس‌پذیر ابری رایج شد: ابزارهایی مثل Fivetran یا Airbyte داده‌ی خام را کپی می‌کنند و تبدیل‌های SQL (dbt) داخل warehouse و نزدیک داده اجرا می‌شوند.

```
ETL:  source ─► [transform on separate server/Spark] ─► warehouse
ELT:  source ─► raw tables in warehouse ─► [SQL transforms (dbt)] ─► models

-- ELT transform, run inside the warehouse
CREATE TABLE marts.daily_revenue AS
SELECT order_date, SUM(amount) AS revenue
FROM raw.orders WHERE status = 'PAID' GROUP BY 1;
```

مزایای ELT: داده‌ی خام حفظ می‌شود (replay و بازتولید)، تبدیل‌ها SQL هستند و تحلیلگر می‌تواند مالک آن‌ها باشد، مقیاس از موتور warehouse می‌آید و توسعه سریع‌تر است. ETL هنوز مناسب است برای: داده‌ی حساس که باید قبل از ورود mask شود، پردازش سنگین غیر SQL (parse، ML، تصویر)، غنی‌سازی استریمی و اعتبارسنجی سخت schema در ابتدا.

## نگاه سنیور و نکات مصاحبه
در عمل بیشتر stackهای مدرن «ELT با کمی ETL» هستند: پاک‌سازی سبک یا مدیریت PII در ingestion و مدل‌سازی عمده در warehouse. مصالحه‌ها: ELT می‌تواند SQL پراکنده و هزینه‌ی غافلگیرکننده از تبدیل مکرر کل جدول بسازد (مدل‌های incremental)، فرود داده‌ی خام ممکن است قوانین حریم خصوصی را نقض کند و لایه‌های خام governance می‌خواهند. data contract و تست کیفیت در هر دو سبک بین لایه‌ها می‌آیند. ETL را منسوخ معرفی نکنید؛ برای هر تبدیل انتخاب کنید.

سؤال مصاحبه: «برای پلتفرم جدید ETL یا ELT؟» پاسخ: پیش‌فرض ELT: داده‌ی خام در object storage/warehouse، تبدیل با SQL لایه‌ای در dbt و خام تغییرناپذیر برای replay؛ پردازش به سبک ETL قبل از فرود فقط برای mask کردن PII، تبدیل سنگین غیر SQL یا نیاز استریم.
