---
term: "ORC"
term_en: "ORC"
aliases: ["orc","فرمت orc"]
category: storage
short: "فرمت فایل ستونی از اکوسیستم Hive با ایندکس داخلی و پشتیبانی ACID."
related: ["parquet","columnar-storage","predicate-pushdown"]
---
## به زبان ساده (مبتدی)
پسرعمو‌ی Parquet: آن هم داده را ستون به ستون و فشرده نگه می‌دارد و برای هر بلوک یادداشت‌های خلاصه می‌گذارد تا query بتواند تکه‌های بی‌ربط را رد کند.

## نحوه‌ی کار (متوسط)
فایل ORC شامل stripeهایی با داده‌ی ستون، ایندکس سبک (min/max، bloom filter) و footer با آمار است. predicate pushdown را پشتیبانی می‌کند و در Hive جدول‌های تراکنشی ACID را با فایل‌های delta دارد.

## نگاه سنیور و نکات مصاحبه
Parquet در اکوسیستم Spark/lakehouse غالب است؛ ORC برای تیم‌های Hive/Presto/Trino طبیعی است. فشرده‌سازی و سرعت‌شان مشابه است، پس بر اساس پشتیبانی اکوسیستم و table format انتخاب کنید (Iceberg با هر دو کار می‌کند).

سؤال مصاحبه: «Parquet یا ORC؟» پاسخ: از نظر فنی تقریباً جایگزین‌پذیرند؛ همان را بردار که موتورهایت بهتر بهینه می‌کنند، معمولاً Parquet.
