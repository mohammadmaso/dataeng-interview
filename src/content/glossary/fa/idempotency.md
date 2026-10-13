---
term: "خودتوانی (Idempotency)"
term_en: "Idempotency"
aliases: ["خودتوان", "ایدمپوتنت"]
category: distributed
short: "عملیاتی خودتوان است که اجرای چندباره‌ی آن همان نتیجه‌ی یک‌بار اجرا را بدهد؛ پایه‌ی retry و backfill امن."
related: ["exactly-once", "backfill", "upsert", "dead-letter-queue"]
---
## به زبان ساده (مبتدی)
دکمه‌ی آسانسور را فرض کنید. اگر پنج بار فشار بدهید پنج آسانسور نمی‌آید؛ نتیجه همان یک بار فشردن است. یک job داده‌ی خودتوان هم همین‌طور است: اگر وسط کار خراب شود و دوباره اجرایش کنید، داده‌ی تکراری یا خراب تولید نمی‌شود. این مهم است چون در سیستم‌های داده خطا و retry همیشه اتفاق می‌افتد و job باید تکرار امن باشد.

## نحوه‌ی کار (متوسط)
یک مرحله‌ی pipeline زمانی خودتوان است که اثر اجرای دوباره‌ی آن روی مقصد با اجرای یک‌بار برابر باشد (`f(f(x)) = f(x)`). تکنیک‌های رایج:

- به‌جای append، یک برش مشخص (پارتیشن یا بازه‌ی تاریخ) را overwrite کنید.
- با کلید طبیعی یا قطعی بنویسید (`MERGE`/upsert یا `INSERT ... ON CONFLICT`).
- شناسه را از محتوا بسازید (hash کلید کسب‌وکار) نه UUID تصادفی یا `now()`.
- یک جدول batch-id یا offset پردازش‌شده نگه دارید تا هر batch فقط یک بار اعمال شود.

```
-- Not idempotent: a retry doubles the numbers
INSERT INTO daily_sales SELECT ... WHERE dt = '2024-05-01';

-- Idempotent: replace the whole partition (overwrite by key/date)
INSERT OVERWRITE TABLE daily_sales PARTITION (dt = '2024-05-01')
SELECT ... WHERE order_date = '2024-05-01';

-- Idempotent: keyed upsert
MERGE INTO sales t USING staging s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

خود job هم باید نسبت به ورودی قطعی (deterministic) باشد: خواندن «همه‌چیز تا همین الان» خودتوانی را می‌شکند، ولی خواندن «بازه‌ی تاریخ منطقی» آن را حفظ می‌کند.

## نگاه سنیور و نکات مصاحبه
خودتوانی چیزی است که at-least-once را در عمل مثل exactly-once می‌کند: retry و تکراری‌ها بی‌ضرر می‌شوند. تله‌های رایج: append در taskی از Airflow که retry می‌شود، استفاده از `current_timestamp` در خروجی، side effectهای غیرخودتوان (ایمیل، پرداخت، فراخوانی API) و ترتیب غیرقطعی که تعیین می‌کند کدام رکورد تکراری برنده شود. producer خودتوان در Kafka فقط retryهای یک session را dedupe می‌کند؛ برای end-to-end باید sink هم خودتوان باشد (upsert کلیددار، `MERGE` در Iceberg، ReplacingMergeTree در ClickHouse با محدودیت‌هایش).

سؤال مصاحبه: «چطور یک pipeline روزانه را امن برای اجرای مجدد می‌کنی؟» پاسخ: پارامتر تاریخ منطقی، بازه‌ی خواندن قطعی، و نوشتن با overwrite پارتیشن یا merge کلیددار تا اجرای مجدد جایگزین شود نه اضافه؛ و برای side effectهای غیرقابل overwrite یک run-id.
