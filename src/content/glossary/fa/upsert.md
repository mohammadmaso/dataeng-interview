---
term: "آپسرت (Upsert)"
term_en: "Upsert"
aliases: ["upsert", "merge", "آپسرت"]
category: ingestion
short: "عملیاتی که اگر کلید وجود داشته باشد سطر را به‌روز و در غیر این صورت درج می‌کند، در یک گام اتمیک."
related: ["idempotency", "cdc", "tombstone"]
---
## به زبان ساده (مبتدی)
به‌روزرسانی فهرست مخاطبین: اگر آن فرد از قبل در گوشی هست شماره‌اش را ویرایش می‌کنید و اگر نیست به‌عنوان مخاطب جدید اضافه می‌کنید. لازم نیست اول چک کنید و هرگز دو ورودی نمی‌شود. upsert یعنی «به‌روزرسانی یا درج» به‌صورت یک عملیات.

## نحوه‌ی کار (متوسط)
upsert سطرهای ورودی را بر اساس کلید یکتا در مقصد ادغام می‌کند. بارگذاری را خودتوان می‌کند (اجرای دوباره‌ی همان batch همان نتیجه را می‌دهد) و هسته‌ی اعمال CDC، مدیریت SCD و مدل‌های incremental است. اشکال: `INSERT ... ON CONFLICT` (Postgres)، `ON DUPLICATE KEY UPDATE` (MySQL)، `MERGE` استاندارد و «آخرین نوشتن برنده» بر اساس کلید در storeهایی مثل Cassandra یا Redis.

```
-- PostgreSQL
INSERT INTO users (id, email, updated_at) VALUES (42, 'a@x.com', now())
ON CONFLICT (id) DO UPDATE
  SET email = EXCLUDED.email, updated_at = EXCLUDED.updated_at
  WHERE users.updated_at < EXCLUDED.updated_at;     -- ignore stale events

-- Lakehouse / warehouse
MERGE INTO dim_user t USING staged s ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

به کلید یکتای قابل‌اعتماد و ورودی قطعی نیاز دارد: اگر batch مبدأ چند سطر برای یک کلید داشته باشد `MERGE` خطا می‌دهد یا دلخواه انتخاب می‌کند، پس اول dedupe کنید (آخرین بر اساس version/timestamp). بررسی version یا timestamp بگذارید تا رویداد نامرتب داده‌ی جدیدتر را با قدیمی‌تر بازنویسی نکند.

## نگاه سنیور و نکات مصاحبه
هزینه به store بستگی دارد. storeهای سطری آن را با index ارزان انجام می‌دهند. در جدول‌های ستونی lakehouse، `MERGE` فایل‌های متأثر را بازنویسی می‌کند (copy-on-write) یا delete file می‌نویسد که هنگام خواندن ادغام می‌شود (merge-on-read): گران است، پس upsert را batch کنید و بر اساس کلید cluster/partition کنید تا فایل‌های بازنویسی‌شده کم شود. ReplacingMergeTree در ClickHouse در زمان merge به‌صورت نهایی dedupe می‌کند (برای دقت `FINAL` یا `argMax`). `MERGE`های هم‌زمان روی سطرهای یکسان تعارض دارند. حذف‌ها پرچم یا ستون op می‌خواهند. مراقب کلیدهای نامنطبق به‌خاطر تفاوت نوع یا حروف باشید.

سؤال مصاحبه: «رویدادهای CDC را چطور روی جدول Iceberg اعمال می‌کنی؟» پاسخ: هر micro-batch را به آخرین رویداد هر primary key بر اساس موقعیت لاگ dedupe می‌کنم، سپس MERGE که بر اساس op حذف، به‌روزرسانی یا درج می‌کند، با partition pruning و compaction منظم و ترتیب تضمین‌شده با ستون version.
