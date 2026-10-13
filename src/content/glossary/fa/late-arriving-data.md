---
term: "داده‌ی دیررس (Late-arriving data)"
term_en: "Late-arriving data"
aliases: ["داده دیررس", "رویداد دیررس", "late data", "out-of-order"]
category: streaming
short: "رکوردهایی که بعد از پردازش بازه یا پنجره‌ی زمانیِ متعلق به خودشان می‌رسند."
related: ["watermark", "event-time", "backfill", "scd"]
---
## به زبان ساده (مبتدی)
پستچی را تصور کنید که نامه‌های دیروز را امروز تحویل می‌دهد. اگر دفتر دیروز را بسته‌اید باید تصمیم بگیرید: دفتر را دوباره باز کنید و نامه‌ها را اضافه کنید یا آن‌ها را مربوط به امروز ثبت کنید. داده‌ی دیررس در pipeline همین مسئله است: رویدادها بعد از آنکه نتیجه‌ی بازه‌ی زمانی‌شان را حساب کرده‌اید می‌رسند.

## نحوه‌ی کار (متوسط)
علل: کلاینت موبایل آفلاین، retry شبکه، تأخیر batch بالادست، اختلاف ساعت و replay. دیررس یعنی event time قدیمی‌تر از موقعیت پردازش جاری است. دو حالت رایج: fact دیررس (رویداد برای روز گذشته) و dimension دیررس (fact به مشتری‌ای ارجاع می‌دهد که هنوز در dimension نیست).

```
-- Batch: re-process a trailing window each run instead of only "yesterday"
DELETE FROM daily_agg WHERE dt >= current_date - 3;
INSERT INTO daily_agg SELECT event_date, ... FROM events
WHERE event_date >= current_date - 3 GROUP BY 1;

-- Streaming (Flink): accept events up to 1 hour late, route the rest aside
.window(TumblingEventTimeWindows.of(Time.minutes(5)))
.allowedLateness(Time.hours(1))
.sideOutputLateData(lateTag)
```

استراتژی‌ها: (۱) پنجره‌ی lookback، محاسبه‌ی دوباره‌ی N روز اخیر در هر اجرا؛ (۲) پردازش event-time با واترمارک و allowed lateness و انتشار نتیجه‌ی به‌روزشده؛ (۳) side output/DLQ برای داده‌ی خیلی دیر و batch اصلاحی دوره‌ای؛ (۴) برای dimension، درج سطر جایگزین استنباطی (`Unknown`/stub) و به‌روزرسانی آن با رسیدن رکورد واقعی، با اصلاح SCD2 در صورت نیاز.

## نگاه سنیور و نکات مصاحبه
میزان تأخیر قابل‌قبول را با کسب‌وکار تعیین کنید: هم درستی و هم latency را مشخص می‌کند. به‌روزرسانی دیررس یعنی مصرف‌کننده‌ها اعداد در حال تغییر می‌بینند؛ جدول‌های پایین‌دست باید retraction/upsert را تحمل کنند و داشبورد باید تازگی داده و وضعیت «نهایی» را نشان دهد. بر اساس تاریخ رویداد پارتیشن کنید ولی زمان ورود را نگه دارید (`ingested_at`) تا سطرهای دیررس پیدا شوند و فقط پارتیشن‌های متأثر دوباره پردازش شوند. اگر دیررس‌بودن بی‌صدا سطر حذف کند exactly-once بی‌معناست: همیشه تعداد دورریخته‌ها را بشمارید.

سؤال مصاحبه: «رویدادهایی که دو روز دیر می‌رسند را چطور مدیریت می‌کنی؟» پاسخ: پردازش در event time، پارتیشن بر اساس تاریخ رویداد، پردازش خودتوان پارتیشن‌های متأثر از پنجره‌ی lookback، و در استریم allowed lateness با به‌روزرسانی به‌علاوه‌ی side output برای اصلاح بعدی، با اندازه‌گیری و هشدار روی حجم رویداد دیررس.
