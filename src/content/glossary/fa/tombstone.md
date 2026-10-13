---
term: "توم‌ستون (Tombstone)"
term_en: "Tombstone"
aliases: ["tombstone", "توم‌ستون", "نشانگر حذف", "delete marker"]
category: storage
short: "رکورد نشانگری که یک حذف را نشان می‌دهد تا حذف‌ها در فایل‌های تغییرناپذیر، لاگ‌ها و replicaها منتشر شود."
related: ["lsm-tree", "compaction", "cdc", "upsert"]
---
## به زبان ساده (مبتدی)
در دفترچه‌ی مشترکی که کسی حق پاک‌کردن ندارد، برای حذف یک مورد کنارش می‌نویسید «مورد ۴۲ لغو شد». خواننده‌ها یادداشت را می‌بینند و آن مورد را نادیده می‌گیرند و بعداً یک پاک‌سازی هر دو را فیزیکی حذف می‌کند. آن یادداشت یک tombstone است.

## نحوه‌ی کار (متوسط)
ساختارهای تغییرناپذیر و append-only (SSTableهای LSM، لاگ‌های Kafka، فایل‌های Parquet، storeهای replicate‌شده) نمی‌توانند رکورد را در جا پاک کنند. حذف به‌صورت رکورد جدیدی به نام tombstone نوشته می‌شود که نسخه‌های قدیمی کلید را می‌پوشاند. خواندنی که tombstone را ببیند کلید را ناموجود می‌داند؛ compaction بعداً وقتی امن بود tombstone و داده‌ی پوشانده‌شده را حذف می‌کند.

```
# Kafka (log-compacted topic): null value = tombstone for the key
producer.send("users", key=b"user-42", value=None)
# After delete.retention.ms, compaction removes the key entirely.

# Cassandra: DELETE writes a tombstone, removed after gc_grace_seconds
DELETE FROM users WHERE id = 42;
```

«امن» در سیستم‌های replicate‌شده مهم است: اگر tombstone قبل از دیدن همه‌ی replicaها پاک شود، replicaای که حذف را از دست داده می‌تواند مقدار قدیمی را زنده کند («زامبی»). Cassandra برای همین tombstone را به مدت `gc_grace_seconds` نگه می‌دارد و repair در آن بازه را می‌خواهد؛ Kafka آن را به مدت `delete.retention.ms` نگه می‌دارد تا consumerها حذف را ببینند. table formatها هم با delete file (position/equality delete در Iceberg) همین کار را می‌کنند.

## نگاه سنیور و نکات مصاحبه
مسائل عملیاتی: انباشت tombstone خواندن را کند می‌کند (اسکن سلول‌های مرده؛ Cassandra هشدار می‌دهد و کوئری را روی آستانه‌ها رد می‌کند)، workload شبیه صف روی Cassandra ضدالگو است، انقضای TTL هم tombstone می‌سازد و پارتیشن‌های پهن با حذف زیاد خطرناک‌اند. در pipelineهای CDC رویداد delete باید به delete یا tombstone پایین‌دست نگاشت شود؛ فراموشی آن سطرهای شبح باقی می‌گذارد. برای GDPR حذف منطقی کافی نیست: داده‌ی زیرین باید در نهایت با compaction یا بازنویسی فیزیکی حذف شود.

سؤال مصاحبه: «چرا Cassandra نمی‌تواند سطر را مستقیم حذف کند؟» پاسخ: SSTableها تغییرناپذیرند و داده replicate است؛ پس حذف به‌صورت tombstone نوشته می‌شود که نسخه‌های قدیمی را می‌پوشاند و تا gc_grace نگه داشته می‌شود تا همه‌ی replicaها بفهمند، وگرنه داده‌ی حذف‌شده بعد از repair برمی‌گردد.
