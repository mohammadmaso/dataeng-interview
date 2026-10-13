---
term: "کاردینالیتی (Cardinality)"
term_en: "Cardinality"
aliases: ["cardinality", "کاردینالیتی", "high cardinality", "low cardinality", "selectivity"]
category: sql
short: "تعداد مقادیر متمایز یک ستون (یا رابطه‌ی تعداد سطر بین جدول‌ها)؛ بر ایندکس، پارتیشن، encoding و انتخاب plan اثر می‌گذارد."
related: ["database-index", "partition", "cost-based-optimizer"]
---
## به زبان ساده (مبتدی)
در کلاسی با ۳۰ دانش‌آموز «نام کوچک» مقادیر متفاوت زیادی دارد (کاردینالیتی بالا) در حالی که «پایه‌ی تحصیلی» شاید فقط یک یا دو مقدار داشته باشد (کاردینالیتی پایین). دانستن اینکه یک ستون چقدر متنوع است به شما کمک می‌کند تصمیم بگیرید چطور ذخیره، جست‌وجو و تقسیمش کنید.

## نحوه‌ی کار (متوسط)
دو معنا: cardinality ستون (مقادیر متمایز) و cardinality رابطه (۱:۱، ۱:N، N:M بین جدول‌ها). cardinality ستون طراحی را هدایت می‌کند: ستون‌های پر cardinality برای ایندکس و bloom filter در point lookup مناسب‌اند ولی کلید پارتیشن بدی هستند (میلیون‌ها پارتیشن) و state گروه‌بندی را منفجر می‌کنند؛ ستون‌های کم cardinality برای dictionary encoding، ایندکس bitmap، پارتیشن (به‌اندازه) و نوع `LowCardinality` در ClickHouse مناسب‌اند.

```
SELECT count(*) AS rows,
       count(DISTINCT country)  AS country_card,   -- ~200   : LOW  -> good partition/dictionary
       count(DISTINCT user_id)  AS user_card,      -- ~50 M  : HIGH -> bad partition key
       count(DISTINCT status)   AS status_card     -- 4      : very low
FROM orders;
-- selectivity = distinct / rows  (user_id ~ 1.0 selective; status ~ 0.0 not selective)
-- Relationship cardinality: customers 1 --- N orders N --- M products (via order_items)
```

optimizer از تخمین تعداد متمایز و selectivity برای انتخاب ترتیب و نوع join و مفید بودن ایندکس استفاده می‌کند. cardinality رابطه رفتار join را تعیین می‌کند: join یک رابطه‌ی ۱:N سطرها را ضرب می‌کند که ریشه‌ی باگ‌های fan-out و معیارهای دوبار شمرده‌شده است.

## نگاه سنیور و نکات مصاحبه
حالت‌های خرابی را بشناسید: سیستم‌های متریک و لاگ با label پر cardinality (شناسه‌ی کاربر به‌عنوان tag) ذخیره و حافظه را منفجر می‌کنند (Prometheus، پایگاه‌های سری زمانی)؛ `COUNT(DISTINCT)` روی ستون پر cardinality گران است و اغلب با HyperLogLog جایگزین می‌شود؛ group-by روی کلید پر cardinality در استریم state بزرگ می‌خواهد؛ skew در ستون کم cardinality (۸۰٪ یک مقدار) هنوز data skew می‌سازد. تخمین cardinality heuristic است و با ستون‌های همبسته بدتر می‌شود؛ تخمین غلط به plan بد می‌انجامد. در مدل‌سازی cardinality رابطه را صریح تعریف و تست کنید (کلید یکتا، تست relationships) تا fan-out بی‌صدا رخ ندهد.

سؤال مصاحبه: «جدول events با ۱۰ میلیارد سطر را روی کدام ستون پارتیشن می‌کنی؟» پاسخ: ستونی با cardinality کم تا متوسط که با فیلترهای رایج جور باشد، معمولاً تاریخ رویداد (و در صورت لزوم ساعت)، نه user_id؛ ستون‌های پر cardinality به sort key، bloom filter یا bucketing می‌روند.
