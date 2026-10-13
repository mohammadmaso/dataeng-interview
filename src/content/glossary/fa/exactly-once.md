---
term: "دقیقاً یک‌بار (Exactly-once)"
term_en: "Exactly-once semantics"
aliases: ["exactly-once", "دقیقاً یک بار", "دقیقاً یک‌بار"]
category: distributed
short: "اثر هر رکورد روی نتیجه‌ی نهایی دقیقاً یک بار است، هرچند تحویل ممکن است تکرار شود؛ معمولاً با idempotency یا تراکنش."
related: ["idempotency", "two-phase-commit", "checkpoint", "outbox-pattern"]
---
## به زبان ساده (مبتدی)
ارسال حواله‌ی بانکی با پست را تصور کنید. پستچی ممکن است نامه را گم کند (پس دوباره می‌فرستید) یا دو نسخه تحویل دهد (اگر اشتباهی دوباره بفرستید). exactly-once یعنی هر چند نامه فرستاده یا گم شود، پول دقیقاً یک بار منتقل می‌شود. شبکه نمی‌تواند تحویل فیزیکی یک‌باره را تضمین کند؛ پس سیستم‌ها کاری می‌کنند که اثر فقط یک بار رخ دهد.

## نحوه‌ی کار (متوسط)
تضمین‌های تحویل: at-most-once (ممکن است گم شود)، at-least-once (ممکن است تکراری شود)، exactly-once (در اثر هیچ‌کدام). تحویل واقعاً یک‌باره روی شبکه‌ی ناپایدار ناممکن است؛ آنچه سیستم‌ها می‌دهند exactly-once در اثر پردازش است و از دو جزء ساخته می‌شود:

۱. منبع قابل replay (offset) به‌همراه state اتمیک: موتور موقعیت منبع و state را با هم checkpoint می‌کند (Flink checkpoint، offset و WAL در Spark).
۲. sink خودتوان یا تراکنشی: نوشتن یا با کلید dedupe می‌شود یا اتمیک همراه offsetها commit می‌شود (تراکنش Kafka، sink با two-phase commit).

```
# Kafka producer + transactional consume-transform-produce
enable.idempotence=true
transactional.id=job-42

# consumer reading only committed data
isolation.level=read_committed
```

تضمین فقط درون مرز pipeline برقرار است؛ side effectهای بیرونی (HTTP، ایمیل) کلید idempotency خودشان را لازم دارند.

## نگاه سنیور و نکات مصاحبه
exactly-once در Kafka چرخه‌ی read-process-write بین topicهای Kafka را با producer خودتوان و تراکنش پوشش می‌دهد؛ وقتی به سیستم بیرونی می‌نویسید باید sink تراکنشی یا خودتوان داشته باشید (مثلاً sink با two-phase commit در Flink یا commit در Iceberg گره‌خورده به checkpoint). هزینه‌ها: latency بیشتر (commit در مرز checkpoint یا تراکنش)، انتظار `read_committed` برای تراکنش‌های باز، و نیاز به fencing برای zombie producerها. بسیاری از تیم‌ها at-least-once به‌علاوه‌ی sink خودتوان را انتخاب می‌کنند چون ساده‌تر و به‌همان‌اندازه درست است.

سؤال مصاحبه: «آیا exactly-once ممکن است؟» پاسخ: به‌عنوان تحویل شبکه نه، به‌عنوان اثر بله: replay از offset با commit اتمیک state و خروجی، یا نوشتن خودتوان؛ و دقیق می‌گویم مرز تضمین کجاست.
