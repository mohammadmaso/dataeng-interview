---
term: "دریاچه‌ی داده (Data lake)"
term_en: "Data lake"
aliases: ["data lake", "دریاچه داده", "دیتا لیک"]
category: lakehouse
short: "مخزن مرکزی روی object storage ارزان که داده‌ی خام را با فرمت اصلی نگه می‌دارد و schema را هنگام خواندن اعمال می‌کند."
related: ["lakehouse", "data-warehouse", "object-storage"]
---
## به زبان ساده (مبتدی)
data lake یک انبار بزرگ است که هر چیزی را بدون مرتب‌کردن اولیه در آن می‌ریزید: جدول، لاگ، JSON، تصویر، همان‌طور که می‌رسد. ارزان و منعطف است و بعداً تصمیم می‌گیرید چطور سازمان‌دهی و خوانده شود. ولی بدون مراقبت می‌تواند به باتلاقی نامرتب تبدیل شود.

## نحوه‌ی کار (متوسط)
data lakeها روی object storage (S3، GCS، ADLS، و در گذشته HDFS) قرار دارند و فایل‌ها را با فرمت باز (Parquet، ORC، Avro، JSON، CSV) نگه می‌دارند. schema هنگام خواندن اعمال می‌شود، برخلاف schema-on-write در warehouse. ذخیره‌سازی و محاسبه جدا هستند؛ موتورهای مختلف (Spark، Trino، Hive، Presto) همان فایل‌ها را کوئری می‌کنند. zoneهای معمول: raw/landing، پاک‌سازی‌شده/curated و آماده‌ی مصرف.

```
s3://company-lake/
  raw/orders/dt=2024-05-01/part-0001.json.gz      # as received
  curated/orders/dt=2024-05-01/part-0001.parquet  # cleaned, columnar
  ml/features/...                                 # images, logs, text also live here
-- schema-on-read: structure is applied when you query
CREATE EXTERNAL TABLE raw_orders (...) LOCATION 's3://company-lake/raw/orders/';
```

نقاط قوت: هزینه‌ی کم به ازای هر ترابایت، هر نوع داده، انعطاف برای data science و ML، و بدون قفل‌شدن به vendor در فرمت فایل. ضعف‌های lake ساده: بدون ACID (نوشتن ناقص دیده می‌شود)، بدون update/delete سازگار، schema drift، governance و قابلیت کشف ضعیف، و کارایی وابسته به چیدمان فایل.

## نگاه سنیور و نکات مصاحبه
همین ضعف‌ها دلیل پیدایش table formatها (Iceberg، Delta، Hudi) و lakehouse است: روی همان فایل‌ها تراکنش، اعمال schema و pruning مبتنی بر متادیتا می‌افزایند. lake بدون کاتالوگ، مالکیت، قرارداد نام‌گذاری، قوانین چرخه‌ی عمر و بررسی کیفیت باتلاق می‌شود. دغدغه‌های governance: PII در zoneهای خام، کنترل دسترسی سطح جدول/ستون، retention و حذف برای GDPR (در فایل‌های تغییرناپذیر سخت است). تله‌های هزینه: egress، درخواست list روی فایل‌های زیاد، تکرار داده بین zoneها و داده‌ی فراموش‌شده بدون مالک.

سؤال مصاحبه: «Data lake در برابر data warehouse؟» پاسخ: lake داده‌ی خام و متنوع را ارزان با schema-on-read و فایل باز نگه می‌دارد؛ منعطف ولی نیازمند governance؛ warehouse داده‌ی مدل‌شده با schema-on-write و کارایی تنظیم‌شده دارد؛ فرمت‌های lakehouse امروز این دو را ترکیب و ACID و SQL روی ذخیره‌ی lake می‌دهند.
