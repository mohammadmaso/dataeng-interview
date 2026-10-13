---
term: "قرارداد داده (Data contract)"
term_en: "Data contract"
aliases: ["قرارداد داده", "data contract"]
category: quality
short: "توافق صریح و نسخه‌دار بین تولیدکننده و مصرف‌کننده‌ی داده درباره‌ی schema، معنا، کیفیت و SLA."
related: ["lineage", "schema-evolution", "data-mesh"]
---
## به زبان ساده (مبتدی)
وقتی آپارتمان اجاره می‌کنید قرارداد می‌بندید تا هر دو طرف بدانند چه انتظاری دارند. قرارداد داده همین است بین تیم تولیدکننده و تیم‌های مصرف‌کننده. می‌گوید چه فیلدهایی هست، چه معنایی دارند، داده چقدر تازه خواهد بود و تغییرات چطور اعلام می‌شوند؛ تا تولیدکننده نتواند بی‌صدا همه‌ی پایین‌دستی‌ها را خراب کند.

## نحوه‌ی کار (متوسط)
قرارداد داده متادیتای ماشین‌خوان (معمولاً YAML یا تعریف در schema registry) است که این‌ها را پوشش می‌دهد: schema و نوع‌ها، معنا (واحد، enum، برچسب PII)، قواعد کیفیت (not null، یکتایی، بازه)، SLA (تازگی، کامل‌بودن)، مالکیت و سیاست سازگاری برای تکامل.

```
dataset: orders
version: 2.1.0
owner: team-checkout
schema:
  - {name: order_id, type: string, required: true, unique: true}
  - {name: amount,   type: decimal(12,2), required: true, min: 0}
  - {name: status,   type: string, enum: [NEW, PAID, CANCELLED]}
sla: {freshness: 15m, availability: 99.9%}
compat: backward
```

اجرای آن در CI (بررسی breaking change در PR تولیدکننده)، هنگام ingestion (اعتبارسنجی با schema و ارسال تخلف‌ها به dead letter queue) و در مانیتورینگ (تست تازگی و کیفیت) انجام می‌شود. ابزارها: Confluent Schema Registry با حالت‌های سازگاری Avro/Protobuf، model contract در dbt، و Great Expectations یا Soda.

## نگاه سنیور و نکات مصاحبه
هدف انتقال کیفیت داده به چپ است: تیم تولیدکننده مالک interface است، مثل API. بدون آن خرابی رایج این است که تیم اپلیکیشن ستونی را تغییر نام می‌دهد و ساعت ۳ بامداد داشبوردها خراب می‌شوند. مصالحه‌ها: قرارداد فرایند اضافه می‌کند و مالکیت واقعی می‌خواهد؛ خیلی سخت‌گیرانه تیم محصول را کند می‌کند، خیلی شل بی‌فایده است. سازگاری (backward، forward، full) و دوره‌ی deprecation را تعریف کنید. قرارداد با data mesh و با pipelineهای CDC که schema جداول طراحی داخلی دیتابیس را نشت می‌دهند، خوب جور است.

سؤال مصاحبه: «چطور جلوی خراب‌شدن pipeline با تغییر schema بالادست را می‌گیری؟» پاسخ: قرارداد نسخه‌دار در registry، بررسی سازگاری در CI تولیدکننده، اعتبارسنجی هنگام ingestion با DLQ، و مالک مشخص با دوره‌ی deprecation توافق‌شده.
