---
term: "ثبت تغییرات داده (CDC)"
term_en: "Change data capture (CDC)"
aliases: ["cdc", "change data capture", "ثبت تغییرات داده"]
category: ingestion
short: "استخراج insert، update و delete در سطح سطر از دیتابیس، معمولاً با خواندن لاگ تراکنش، و ارسال آن‌ها به پایین‌دست."
related: ["wal", "upsert", "tombstone", "outbox-pattern"]
---
## به زبان ساده (مبتدی)
به‌جای کپی کامل دفترچه هر شب برای فهمیدن تغییرات، دوستی را تصور کنید که از روی شانه‌ی شما می‌خواند و هر ویرایش را همان لحظه می‌گوید: «خط ۵ عوض شد، خط ۹ پاک شد.» CDC این کار را برای دیتابیس می‌کند. فقط تغییرات را سریع می‌فرستد تا سیستم‌های دیگر بدون کپی کامل سنگین همگام بمانند.

## نحوه‌ی کار (متوسط)
روش‌ها: مبتنی بر query (poll روی `updated_at`؛ حذف‌های واقعی و حالت‌های میانی را از دست می‌دهد)، مبتنی بر trigger (بار اضافه روی منبع) و مبتنی بر لاگ (خواندن WAL/binlog؛ اثر کم، ثبت همه‌ی تغییرات به ترتیب commit، شامل delete). Debezium ابزار رایج log-based است که رویدادها را در Kafka منتشر می‌کند.

```
{
  "op": "u",
  "before": {"id": 7, "status": "NEW"},
  "after":  {"id": 7, "status": "PAID"},
  "source": {"db": "shop", "table": "orders", "lsn": 23984112},
  "ts_ms": 1715000000123
}
```

هر رویداد نوع عملیات، تصویر before/after و موقعیت لاگ دارد. sink آن‌ها را به‌صورت upsert/delete بر اساس primary key اعمال می‌کند (`MERGE` در Iceberg/Hudi/Delta، ReplacingMergeTree در ClickHouse) تا یک replica بسازد. معمولاً ابتدا snapshot اولیه گرفته می‌شود و سپس از موقعیت ثبت‌شده‌ی لاگ استریم ادامه می‌یابد.

## نگاه سنیور و نکات مصاحبه
بخش‌های سخت: snapshot اولیه‌ی سازگار و تحویل به استریم، تغییر schema در منبع، ترتیب به‌ازای هر کلید (پارتیشن بر اساس primary key)، ستون‌های TOAST/بدون تغییر در Postgres، متورم‌شدن replication slot وقتی consumer می‌ایستد (WAL جمع می‌شود و دیسک را پر می‌کند) و tombstone برای حذف در topicهای compacted. تحویل at-least-once است پس sink باید خودتوان باشد؛ به ترتیب لاگ اعمال کنید و با موقعیت لاگ به‌روزرسانی‌های کهنه را رد کنید. CDC schema داخلی جدول‌ها را نشت می‌دهد؛ برای رویدادهای دامنه از قرارداد یا الگوی outbox استفاده کنید.

سؤال مصاحبه: «چطور Postgres را به lakehouse replicate می‌کنی؟» پاسخ: Debezium روی logical replication slot به Kafka با کلید primary key، سپس merge خودتوان در Iceberg، با مانیتورینگ slot lag و برنامه‌ی compaction.
