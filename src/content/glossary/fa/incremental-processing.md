---
term: "پردازش افزایشی (Incremental processing)"
term_en: "Incremental processing"
aliases: ["incremental load", "بارگذاری افزایشی", "incremental model", "high-water mark"]
category: batch
short: "پردازش فقط داده‌ی جدید یا تغییرکرده از آخرین اجرا به‌جای محاسبه‌ی دوباره‌ی همه‌چیز."
related: ["backfill", "upsert", "idempotency", "watermark"]
---
## به زبان ساده (مبتدی)
به‌جای شمارش دوباره‌ی کل موجودی انبار هر شب، فقط ثبت می‌کنید امروز چه وارد و چه خارج شد و جمع را تنظیم می‌کنید. پردازش افزایشی فقط داده‌ی جدید یا تغییرکرده را پردازش می‌کند و با رشد داده در زمان و هزینه صرفه‌جویی می‌کند.

## نحوه‌ی کار (متوسط)
هزینه‌ی بارگذاری کامل با کل داده رشد می‌کند و هزینه‌ی اجرای افزایشی با حجم تغییرات. تکنیک‌ها: high-water mark روی ستون یکنواخت (`updated_at`، id افزایشی)، مبتنی بر پارتیشن (پردازش مجدد N پارتیشن جدید)، جریان CDC (اعمال insert/update/delete)، ردیابی فایل/offset برای ingestion (فایل‌های جدید S3، offsetهای Kafka) و خواندن افزایشی table format (diff بین snapshotهای Iceberg).

```
-- dbt incremental model
{{ config(materialized='incremental', unique_key='order_id') }}
SELECT * FROM {{ source('app','orders') }}
{% if is_incremental() %}
WHERE updated_at >= (SELECT max(updated_at) - interval '3 hours' FROM {{ this }})  -- overlap!
{% endif %}
-- merge on unique_key => idempotent despite the overlap
```

برای درستی: از پنجره‌ی هم‌پوشان یا lookback برای داده‌ی دیررس استفاده کنید، با merge/upsert بر اساس کلید سطرهای پردازش‌شده‌ی دوباره جایگزین شوند نه تکرار، حذف‌ها را مدیریت کنید (پرچم soft-delete یا CDC) و مسیر full-refresh برای بازسازی هنگام تغییر منطق داشته باشید.

## نگاه سنیور و نکات مصاحبه
حالت‌های خرابی: `updated_at` توسط همه‌ی writerها به‌روز نمی‌شود (تغییرات از دست می‌روند)، اختلاف ساعت و ترتیب commit تراکنش‌ها باعث می‌شود high-water mark سطرهای دیر commit‌شده را رد کند (هم‌پوشانی یا CDC مبتنی بر لاگ)، حذف‌هایی که برای استخراج مبتنی بر query نامرئی‌اند، appendهای غیرخودتوان، و drift حالت از خطاهای کوچک تا full refresh. دوره‌ای تعداد/checksum مبدأ و مقصد را تطبیق دهید. aggregateهای افزایشی عملیات قابل‌ادغام لازم دارند. واحد افزایشی‌بودن (سطر، پارتیشن، فایل) را بر اساس قابلیت اعتماد منبع انتخاب کنید.

سؤال مصاحبه: «چرا چند ساعت از high-water mark کم می‌کنیم؟» پاسخ: تراکنش‌ها نامرتب commit می‌شوند و داده دیر می‌رسد، پس بزرگ‌تر از بیشینه‌ی timestamp ممکن است سطرها را از دست بدهد؛ هم‌پوشانی و merge بر اساس کلید بارگذاری را خودتوان و کامل نگه می‌دارد.
