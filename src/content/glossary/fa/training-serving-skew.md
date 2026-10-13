---
term: "ناهمخوانی آموزش و سرو (Training-serving skew)"
term_en: "Training-serving skew"
aliases: ["training-serving skew", "data drift", "concept drift", "data leakage", "نشت داده"]
category: ml
short: "ناهمخوانی بین داده یا منطق feature هنگام آموزش مدل و آنچه در production می‌بیند که بی‌صدا کیفیت پیش‌بینی را پایین می‌آورد."
related: ["feature-store", "data-quality", "event-time"]
---
## به زبان ساده (مبتدی)
دانش‌آموزی با ماشین‌حسابی تمرین می‌کند که گرد کردنش یک‌جور است و در امتحان ماشین‌حسابی با گرد کردن دیگر می‌گیرد. پاسخ‌هایش کمی غلط می‌شود بی‌آنکه بداند چرا. مدل هم وقتی اعدادی که از آن یاد گرفته با اعدادی که در استفاده‌ی واقعی می‌گیرد متفاوت آماده شده باشند همین مشکل را دارد.

## نحوه‌ی کار (متوسط)
منابع skew: منطق feature تکراری در دو سیستم، نشت time-travel هنگام ساخت دیتاست آموزش، تفاوت در برخورد با null/پیش‌فرض/واحد، featureهای کهنه در online store و تغییر پردازش داده (نسخه‌ی کتابخانه، تغییر schema). مرتبط ولی متفاوت: drift، که داده‌ی production با گذر زمان تغییر می‌کند حتی اگر pipelineها سازگار باشند، و concept drift که رابطه‌ی هدف تغییر می‌کند پس توزیع پایدار feature هم پیش‌بینی بدتر می‌دهد.

```
Typical causes
1. Different code paths:   training in SQL/Spark, serving in Python/Java -> subtle logic differences
2. Time leakage:           training feature uses data after the label time (not point-in-time)
3. Missing/late features:  online store lag, nulls handled differently, defaults differ
4. Data drift:             input distribution shifts after deployment (new product, season)
5. Concept drift:          relationship between input and label changes (fraud tactics change)

Detect: compare feature distributions (PSI, KS test, mean/null rate) training vs live; log served features
```

راه‌حل‌ها: یک تعریف feature مشترک بین آموزش و سرو (feature store، کتابخانه‌ی تبدیل مشترک)، joinهای point-in-time، لاگ دقیق featureهای استفاده‌شده در سرو و آموزش روی همان لاگ‌ها، و پایش توزیع‌ها با هشدار.

## نگاه سنیور و نکات مصاحبه
مانیتورینگ هم ورودی و هم خروجی می‌خواهد: drift توزیع feature (PSI، KS، نرخ null)، توزیع پیش‌بینی و متریک ground-truth با تأخیر وقتی برچسب‌ها می‌رسند. مهندسان داده اغلب مالک ریشه‌ی مشکل‌اند: تغییر schema بالادست، داده‌ی دیررس و backfillهایی که تاریخچه را بدون آموزش مجدد تغییر دادند. «log and replay» الگوی قدرتمندی است: featureهای سرو را ثبت، با برچسب‌های بعدی join و برای آموزش استفاده کنید تا منطق دقیقاً یکی باشد. مراقب label leakage از aggregateهایی باشید که پنجره‌شان رویداد برچسب را شامل می‌شود، و از آموزش روی داده‌ای که مدل قبلی انتخاب کرده (حلقه‌ی بازخورد).

سؤال مصاحبه: «مدل offline خوب و در production ضعیف است؛ چه چیزی را چک می‌کنی؟» پاسخ: اول leakage و training-serving skew: featureهای لاگ‌شده‌ی سرو را با featureهای offline برای همان موجودیت‌ها مقایسه می‌کنم، point-in-time بودن و برخورد با null/پیش‌فرض را راستی‌آزمایی می‌کنم، تازگی featureهای آنلاین را می‌بینم و بعد drift داده و تأخیر برچسب را بررسی می‌کنم.
