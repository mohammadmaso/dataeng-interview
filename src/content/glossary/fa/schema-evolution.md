---
term: "تکامل schema (Schema evolution)"
term_en: "Schema evolution"
aliases: ["schema evolution", "تغییر schema", "schema drift"]
category: lakehouse
short: "تغییر schema یک دیتاست در طول زمان (افزودن، حذف، تغییر نام یا نوع ستون) بدون خراب‌کردن خواننده‌ها یا داده‌ی موجود."
related: ["data-contract", "parquet", "iceberg"]
---
## به زبان ساده (مبتدی)
فرمی که مردم پر می‌کنند ناگزیر بعداً سؤال تازه می‌خواهد. اگر فیلدی اضافه کنید، فرم‌های پرشده‌ی قدیمی باید هنوز خواندنی باشند و فرم جدید برای خواننده‌های قدیمی هم کار کند. تکامل schema انضباط تغییر شکل داده است به شکلی که کسانی و برنامه‌هایی که از آن استفاده می‌کنند خراب نشوند.

## نحوه‌ی کار (متوسط)
تعریف سازگاری (Avro/Schema Registry): backward یعنی خواننده‌ی جدید می‌تواند داده‌ی قدیمی را بخواند (مثلاً افزودن فیلد با مقدار پیش‌فرض، حذف فیلد اختیاری)؛ forward یعنی خواننده‌ی قدیمی داده‌ی جدید را بخواند؛ full هر دو. تغییرات امن معمولاً افزودن فیلد اختیاری یا دارای پیش‌فرض و گسترش نوع (widening) است؛ ناامن: حذف فیلد الزامی، تغییر نام، باریک‌کردن نوع یا تغییر معنا.

```
-- Safe, backward compatible: add optional column
ALTER TABLE orders ADD COLUMN coupon_code STRING;

-- Risky: rename / retype / drop
ALTER TABLE orders RENAME COLUMN amt TO amount;       -- fine in Iceberg (by column id)
ALTER TABLE orders ALTER COLUMN qty TYPE BIGINT;      -- widening int->bigint usually OK

-- Avro compatibility modes: BACKWARD, FORWARD, FULL (+ _TRANSITIVE)
```

فرمت‌ها متفاوت عمل می‌کنند: Parquet ستون را با نام (یا موقعیت، بسته به موتور) تطبیق می‌دهد پس rename ممکن است بی‌صدا خراب کند؛ Iceberg id ستون را دنبال می‌کند پس rename و reorder امن است؛ Delta از column mapping استفاده می‌کند؛ Avro schema خواننده و نویسنده را resolve می‌کند؛ Protobuf به شماره‌ی ثابت فیلد تکیه دارد. table formatها تاریخچه‌ی schema را نگه می‌دارند تا snapshotهای قدیمی درست خوانده شوند.

## نگاه سنیور و نکات مصاحبه
بیشتر حوادث production از تغییرات معنایی می‌آیند که سیستم نوع نمی‌گیرد (فیلدی که از ریال به تومان یا سنت به دلار می‌رود)، پس علاوه بر سازگاری نحوی به قرارداد و تست نیاز است. استریم مراقبت بیشتری می‌خواهد چون تولیدکننده‌های قدیم و جدید هم‌زمان‌اند: registry، حالت سازگاری به‌ازای subject، و برای تغییر backward اول consumerها بعد producerها. گزینه‌های ingestion: شکست در drift، تکامل خودکار، یا فرستادن فیلدهای غیرمنتظره به ستون variant/JSON یا DLQ. به‌جای تغییر شکننده در جا همیشه دوره‌ی deprecation داشته باشید؛ الگوی expand-migrate-contract خوب کار می‌کند.

سؤال مصاحبه: «منبع ستونی اضافه کرد و pipeline خراب شد؛ چطور جلوگیری می‌کنی؟» پاسخ: schema صریح با خواننده‌ی بردبار برای تغییر افزایشی، registry با بررسی سازگاری در CI تولیدکننده، هشدار drift، و مسیر quarantine برای رکوردهای ناسازگار به‌جای شکست کل job.
