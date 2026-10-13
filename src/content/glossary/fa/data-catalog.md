---
term: "کاتالوگ داده (Data catalog)"
term_en: "Data catalog"
aliases: ["data catalog", "کاتالوگ داده", "metastore", "hive metastore", "کشف داده"]
category: quality
short: "فهرست قابل‌جست‌وجوی دیتاست‌ها با schema، مالک، توضیح، lineage و سیاست دسترسی؛ کاتالوگ جدول نام را به متادیتای جدول نگاشت می‌کند."
related: ["lineage", "data-contract", "data-mesh"]
---
## به زبان ساده (مبتدی)
کاتالوگ کتابخانه می‌گوید چه کتاب‌هایی هست، کجاست، چه کسی نوشته و آیا می‌توانید امانت بگیرید. data catalog همین را برای دیتاست‌ها انجام می‌دهد: به آدم‌ها کمک می‌کند جدول درست را پیدا کنند، معنایش را بفهمند و بدانند از چه کسی بپرسند، به‌جای حدس‌زدن.

## نحوه‌ی کار (متوسط)
کاتالوگ‌های فنی بخشی از runtime هستند: نام جدول را به مکان ذخیره و schema نگاشت می‌کنند و برای table formatهایی مثل Iceberg کاتالوگ اشاره‌گر به فایل متادیتای فعلی را نگه می‌دارد و commit با compare-and-swap اتمیک می‌شود. یک کاتالوگ مشترک باعث می‌شود Spark، Trino و Flink جدول‌های یکسان ببینند.

```
Two meanings, often mixed up:

1. Technical catalog / metastore (runtime):  db.table -> location + schema + table metadata pointer
   Hive Metastore, AWS Glue, Unity Catalog, Iceberg REST/Nessie catalog
   Engines (Spark, Trino, Flink) call it to resolve and atomically commit tables.

2. Business / discovery catalog (humans):  search, glossary, owners, tags, PII classification, lineage
   DataHub, Amundsen, OpenMetadata, Collibra, Atlan
```

کاتالوگ‌های کشف (discovery) متادیتا (schema، لاگ کوئری، lineage، محبوبیت استفاده) را خودکار جمع می‌کنند و به آدم‌ها اجازه می‌دهند توضیح، tag، مالک، واژه‌نامه و طبقه‌بندی مثل PII اضافه کنند. زیربنای governance (سیاست دسترسی، retention)، تحلیل اثر و self-service در data mesh هستند.

## نگاه سنیور و نکات مصاحبه
کیفیت کاتالوگ یعنی تازگی متادیتا و مالکیت. توضیحات کهنه و جدول‌های بی‌مالک کاتالوگ را بی‌فایده می‌کنند؛ پس ingestion را خودکار کنید، مالکیت را به تیم‌ها گره بزنید، میزان استفاده را نشان دهید و مستندسازی را بخشی از قرارداد یا CI کنید (dbt docs). محیط‌های چندموتوره کاتالوگ بی‌طرف نسبت به موتور (REST catalog) می‌خواهند تا قفل‌شدن و نمای ناسازگار پیش نیاید. امنیت: سیاست‌های سطح ستون، دسترسی مبتنی بر tag و audit log باید توسط پلتفرم اعمال شوند نه فقط مستند. بدون کاتالوگ، lake باتلاق می‌شود.

سؤال مصاحبه: «کاتالوگ در معماری Iceberg چه می‌کند؟» پاسخ: شناسه‌ی جدول را به فایل متادیتای فعلی نگاشت می‌کند و هنگام commit اشاره‌گر را اتمیک عوض می‌کند تا همه‌ی موتورها روی وضعیت جدول توافق کنند؛ کاتالوگ کشف جداگانه جست‌وجو، مالکیت و lineage را برای انسان‌ها اضافه می‌کند.
