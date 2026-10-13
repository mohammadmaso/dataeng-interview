---
term: "الگوی Outbox"
term_en: "Outbox pattern"
aliases: ["outbox", "transactional outbox", "الگوی outbox", "جدول outbox"]
category: distributed
short: "نوشتن داده‌ی کسب‌وکار و رویداد خروجی در یک تراکنش محلی و سپس انتشار قابل‌اعتماد رویداد از جدول outbox."
related: ["cdc", "saga", "two-phase-commit", "exactly-once"]
---
## به زبان ساده (مبتدی)
فرض کنید هر بار فروشی ثبت می‌کنید باید نامه‌ای پست کنید. اگر فروش را ثبت کنید و قبل از پست برق برود نامه گم می‌شود؛ اگر اول پست کنید و ثبت فروش شکست بخورد نامه‌ی دروغین فرستاده‌اید. راه‌حل: نامه را در همان ثبت دفترچه‌ی فروش در سینی خروجی بنویسید و دستیاری هرچه در سینی هست را مطمئن پست کند.

## نحوه‌ی کار (متوسط)
مشکل dual-write: به‌روزرسانی دیتابیس و انتشار در Kafka را نمی‌توان اتمیک انجام داد چون دو سیستم تراکنش مشترک ندارند. crash بین آن‌ها state و رویداد را ناسازگار می‌کند. الگوی outbox رویداد را به‌صورت یک سطر در جدول `outbox` و در همان تراکنش محلی تغییر کسب‌وکار می‌نویسد تا اتمیک commit شوند.

```
BEGIN;
  INSERT INTO orders (id, status, total) VALUES (42, 'PLACED', 99.0);
  INSERT INTO outbox (id, aggregate_id, type, payload, created_at)
  VALUES (gen_random_uuid(), 42, 'OrderPlaced', '{"order_id":42,"total":99.0}', now());
COMMIT;      -- both or neither

-- A relay (Debezium reading the WAL, or a poller) publishes outbox rows to Kafka,
-- then marks/deletes them. Consumers dedupe by event id.
```

یک relay جدا سطرهای outbox را منتشر می‌کند: CDC مبتنی بر لاگ (outbox event router در Debezium روی WAL، تأخیر کم و بدون بار polling) یا publisher از نوع polling. تحویل به broker at-least-once است پس consumerها باید خودتوان باشند (dedupe با شناسه‌ی رویداد). ترتیب هر aggregate با کلید کردن پیام‌ها به شناسه‌ی aggregate حفظ می‌شود.

## نگاه سنیور و نکات مصاحبه
هماهنگی سفت two-phase commit را با سازگاری نهایی و رویداد at-least-once معاوضه می‌کند. جزئیات عملیاتی: پاک‌سازی سطرهای outbox (حذف بعد از انتشار یا از طریق CDC با delete و TTL)، رشد جدول outbox و فشار vacuum، پایش lag مربوط به relay، schema رویداد به‌عنوان قرارداد منتشرشده (برخلاف CDC خام جدول، ساختار داخلی را نشت ندهید) و مسائل مقیاس و ترتیب publisher از نوع polling. معادل سمت مصرف‌کننده الگوی inbox است: شناسه‌ی پیام‌های پردازش‌شده را در همان تراکنش اثر ذخیره کنید تا dedupe شود.

سؤال مصاحبه: «چطور تضمین می‌کنی با ذخیره‌ی یک سطر، رویداد هم منتشر شود؟» پاسخ: رویداد را در جدول outbox و در همان تراکنش دیتابیس می‌نویسم، ناهمگام با CDC یا poller به Kafka منتشر می‌کنم و consumerها را خودتوان می‌کنم؛ بدین ترتیب ناسازگاری dual-write بدون تراکنش توزیع‌شده رفع می‌شود.
