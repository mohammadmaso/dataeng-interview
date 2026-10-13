---
term: "زمان رویداد (Event time)"
term_en: "Event time vs processing time"
aliases: ["event time", "processing time", "زمان پردازش", "زمان رویداد"]
category: streaming
short: "event time زمان واقعی وقوع رویداد است و processing time زمانی که سیستم آن را پردازش می‌کند؛ تحلیل درست معمولاً event time می‌خواهد."
related: ["watermark", "late-arriving-data", "sessionization"]
---
## به زبان ساده (مبتدی)
ساعت ۲۳:۵۹ قهوه می‌خرید ولی دستگاه کارت فروش را ساعت ۰۰:۰۳ آپلود می‌کند. این فروش مربوط به کدام روز است؟ روزی که خریدید (event time) نه روزی که بانک خبردار شد (processing time). سیستم‌های استریم باید این دو ساعت را از هم تشخیص دهند.

## نحوه‌ی کار (متوسط)
معمولاً سه timestamp هم‌زمان وجود دارد: event time (در مبدأ تولید می‌شود)، ingestion/arrival time (وقتی broker یا lake دریافت کرد) و processing time (وقتی job اجرا شد). رویدادها به‌خاطر شبکه و batching نامرتب و دیر می‌رسند؛ پس گروه‌بندی با processing time نتیجه‌ای می‌دهد که به بار سیستم و retry وابسته است نه واقعیت.

```
{"user": 7, "action": "purchase",
 "event_time":  "2024-05-01T23:59:58Z",   // when it happened on the phone
 "ingest_time": "2024-05-02T00:03:10Z"}   // when Kafka/our system saw it

-- Which day does this purchase belong to? Event time: May 1.
-- Processing-time bucketing would wrongly put it on May 2.
```

پردازش event-time رویداد را با timestamp خودش به پنجره می‌دهد و با واترمارک می‌فهمد پنجره کامل شده است. processing-time ساده‌تر و کم‌تأخیرترین است ولی غیرقطعی: اجرای دوباره‌ی همان داده در روز دیگر نتیجه‌ی متفاوت می‌دهد و replay و backfill را می‌شکند. نتیجه‌ی event-time تکرارپذیر است.

## نگاه سنیور و نکات مصاحبه
event time برای متریک کسب‌وکاری و processing time برای متریک عملیاتی (throughput، پایش SLA خود pipeline). تله‌ها: ساعت غیرقابل‌اعتماد کلاینت (timestamp را محدود کنید و در حالت پوچ به زمان سرور برگردید)، time zone و DST (UTC ذخیره کنید)، منابع timestamp ترکیبی بین سیستم‌ها و هزینه‌ی event time: state تا عبور واترمارک باید بماند، پس حافظه و latency بیشتر. در Kafka تنظیم topic بین `CreateTime` و `LogAppendTime` مهم است. در batch همین ایده یعنی پارتیشن بر اساس تاریخ رویداد و نگه‌داشتن ستون `ingested_at`.

سؤال مصاحبه: «چرا job استریم نمی‌تواند فقط از ساعت ماشین استفاده کند؟» پاسخ: چون ترتیب و تأخیر ورود متغیر است؛ نتیجه غیرقطعی می‌شود و در replay عوض می‌شود؛ event time با واترمارک پنجره‌های درست و تکرارپذیر می‌دهد به بهای بافر state و تحمل تأخیر.
