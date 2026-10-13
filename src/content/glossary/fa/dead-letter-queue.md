---
term: "صف نامه‌های مرده (Dead letter queue)"
term_en: "Dead letter queue (DLQ)"
aliases: ["dlq", "dead letter queue", "صف خطا", "poison message"]
category: ingestion
short: "صف یا جدول جداگانه برای پیام‌هایی که بارها در پردازش شکست می‌خورند تا جریان اصلی ادامه یابد."
related: ["schema-evolution", "idempotency", "exactly-once"]
---
## به زبان ساده (مبتدی)
در دفتر پست، نامه‌ای با آدرس ناخوانا کل نوار نقاله را متوقف نمی‌کند. به سطل ویژه‌ای می‌رود تا انسان بعداً بررسی کند. DLQ همان سطل است: پیام‌هایی که پردازش نمی‌شوند کنار گذاشته می‌شوند تا بقیه‌ی پیام‌ها پیش بروند.

## نحوه‌ی کار (متوسط)
یک پیام بد (payload خراب، نقض schema، باگ در حالت مرزی) اگر بی‌نهایت retry شود می‌تواند یک پارتیشن مرتب را برای همیشه بلاک کند. الگو: خطاهای موقت را چند بار با backoff retry کنید و خطاهای دائمی را همراه با context (payload اصلی، خطا، topic/پارتیشن/offset مبدأ، زمان، تعداد تلاش) به DLQ بفرستید، سپس offset را commit کنید و ادامه دهید.

```
try:
    record = parse(msg.value)
    process(record)
except (ValidationError, ParseError) as e:
    producer.send("orders.dlq", key=msg.key, value=msg.value,
                  headers={"error": str(e), "src_topic": msg.topic,
                           "partition": msg.partition, "offset": msg.offset})
consumer.commit()   # move on; do not block the partition
```

رویه‌ی خوب: خطای موقت را از دائمی جدا کنید؛ بایت‌های اصلی را دست‌نخورده نگه دارید تا قابل replay باشند؛ روی اندازه و نرخ DLQ هشدار بدهید؛ و ابزاری برای بررسی، اصلاح و re-drive پیام‌ها به topic اصلی بسازید. pipelineهای batch هم با جدول quarantine برای سطرهای ردشده همین کار را می‌کنند.

## نگاه سنیور و نکات مصاحبه
مصالحه‌ها: رد کردن پیام ترتیب سخت آن کلید را می‌شکند و ممکن است state پایین‌دست ناسازگار شود؛ DLQ بدون مانیتورینگ فقط از دست رفتن بی‌صدای داده است. جهش پیام‌های DLQ معمولاً نشانه‌ی تغییر schema یا باگ بالادست است نه داده‌ی بد تصادفی؛ پس با data contract و هشدار جفتش کنید. retry topicهای با تأخیر فزاینده (retry-5m، retry-1h) از بلاک‌شدن جلوگیری می‌کنند. پردازش باید خودتوان باشد چون پیام‌های re-drive شده ممکن است تکرار شوند. Kafka برای consumer ساده DLQ داخلی ندارد؛ Kafka Connect و فریم‌ورک‌هایی مثل Spring Kafka دارند.

سؤال مصاحبه: «یک event خراب پارتیشن Kafka را بلاک کرده؛ چه می‌کنی؟» پاسخ: خطای دائمی را می‌گیرم، پیام را با متادیتای خطا به DLQ می‌فرستم، offset را commit می‌کنم، روی حجم DLQ هشدار می‌گذارم و بعداً اصلاح و replay می‌کنم؛ خطای موقت retry محدود با backoff می‌گیرد.
