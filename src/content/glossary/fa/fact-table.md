---
term: "جدول واقعیت (Fact table)"
term_en: "Fact table"
aliases: ["fact table", "جدول fact", "جدول واقعیت"]
category: modeling
short: "جدول مدل ابعادی که اندازه‌گیری‌های عددی یک رویداد کسب‌وکار را در grain مشخص و با کلید به dimensionها نگه می‌دارد."
related: ["grain", "star-schema", "dimension-table", "surrogate-key"]
---
## به زبان ساده (مبتدی)
جدول fact دفتر خاطرات اتفاق‌هاست: هر خط یک رویداد مثل فروش را با اعدادی مثل تعداد و قیمت و اشاره‌گر به کارت‌های توصیفی (چه کسی، چه چیزی، کِی) ثبت می‌کند. معمولاً خیلی بزرگ می‌شود چون رویداد جدید مدام اضافه می‌شود.

## نحوه‌ی کار (متوسط)
جدول fact معیارها (مقادیر عددی قابل تجمیع) و کلیدهای خارجی به dimensionها را در grain دقیقاً اعلام‌شده نگه می‌دارد. بزرگ‌ترین جدول مدل است، معمولاً بیشتر append است و با join به dimensionها و سپس `GROUP BY` کوئری می‌شود.

```
Types (Kimball):
- Transaction fact:        one row per event (order line, click)
- Periodic snapshot:       one row per entity per period (account balance per day)
- Accumulating snapshot:   one row per process instance, updated as it progresses
- Factless fact:          events/coverage with no numeric measure (attendance)

Measure additivity: additive (revenue) | semi-additive (balance, not across time) | non-additive (ratios)
```

additivity مهم است: درآمد روی هر بُعد قابل جمع است؛ موجودی semi-additive است (روی حساب‌ها جمع می‌شود نه روی زمان)؛ نسبت‌ها را باید از صورت و مخرج additive دوباره حساب کرد نه میانگین گرفت. صورت و مخرج را ذخیره کنید نه درصد را. تاریخ کلید dimension تاریخ است؛ مقدار نامشخص dimension به عضو `Unknown` اشاره می‌کند نه NULL تا inner join درست بماند.

## نگاه سنیور و نکات مصاحبه
نکات عملی: ترتیب بارگذاری (dimension قبل از fact)، مدیریت fact و dimension دیررس، بارگذاری خودتوان با پارتیشن یا merge روی کلید طبیعی، پارتیشن بر اساس تاریخ و rollup از پیش تجمیع‌شده برای داشبوردها. باگ‌های رایج: ترکیب grain، fan-out که دوبار می‌شمارد، null در کلید خارجی که سطر را در inner join حذف می‌کند و میانگینِ میانگین‌ها. رویدادهای استریم ممکن است قبل از بارگذاری dedup شوند. factهای بزرگ معمولاً طرف بزرگ join‌اند و dimensionها broadcast می‌شوند.

سؤال مصاحبه: «fact تراکنشی در برابر snapshot؟» پاسخ: fact تراکنشی هر رویداد را یک بار ثبت می‌کند و ریزترین grain است؛ periodic snapshot وضعیت را در فواصل منظم ثبت می‌کند (مثل موجودی روزانه) و حتی بدون رویداد روند را ارزان کوئری‌پذیر می‌کند؛ accumulating snapshot فرایندی با milestoneها را ردیابی و درجا به‌روز می‌کند.
