---
term: "شافل (Shuffle)"
term_en: "Shuffle"
aliases: ["شافل", "جابه‌جایی داده بین نودها"]
category: batch
short: "بازتوزیع داده از طریق شبکه بین نودها تا سطرهای هم‌کلید کنار هم قرار بگیرند، مثلاً برای join و group by."
related: ["data-skew", "broadcast-join", "partition", "predicate-pushdown"]
---
## به زبان ساده (مبتدی)
کلاسی را تصور کنید که هر دانش‌آموز کارت‌هایی به رنگ‌های مختلف دارد و معلم می‌گوید هر کس برود پیش کسانی که همان رنگ را دارند. همه در کلاس راه می‌افتند و کارت‌ها را جابه‌جا می‌کنند. این رفت‌وآمد همان shuffle است: داده بین ماشین‌ها جابه‌جا می‌شود تا سطرهای مرتبط یک‌جا جمع شوند. معمولاً گران‌ترین بخش یک job است.

## نحوه‌ی کار (متوسط)
عملیاتی مثل `groupBy`، `join`، `distinct`، `orderBy` و `repartition` نیاز دارند همه‌ی سطرهای یک کلید روی یک نود باشند. هر task خروجی‌اش را روی دیسک محلی و بر اساس پارتیشن مقصد می‌نویسد (سمت map)؛ taskهای بعدی تکه‌های خود را از شبکه می‌گیرند (سمت reduce). این شامل serialization، I/O دیسک، انتقال شبکه و spill احتمالی است و برای همین زمان job را می‌بلعد.

```
df.groupBy("country").sum("amount")        # shuffle: hash by country
df1.join(df2, "user_id")                   # shuffle both sides (sort-merge)
df.repartition(200, "user_id")             # explicit shuffle
df.coalesce(10)                            # narrow: no full shuffle
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

راه‌های کاهش: فیلتر و انتخاب ستون زودتر، pre-aggregation (map-side combine)، broadcast جدول‌های کوچک، bucketing یا co-partition برای joinهای پرتکرار، و تنظیم تعداد پارتیشن (خیلی کم یعنی پارتیشن بزرگ و spill؛ خیلی زیاد یعنی سربار زمان‌بندی و فایل‌های ریز).

## نگاه سنیور و نکات مصاحبه
جست‌وجوی نودهای `Exchange` در plan (Spark) یا مراحل `Exchange`/`Redistribute` (Trino، StarRocks) سریع‌ترین راه پیدا کردن shuffle است. هزینه‌ی shuffle با حجم داده تناسب دارد نه تعداد سطر؛ پس سطرهای پهن و ستون‌های هرس‌نشده آسیب می‌زنند. حالت‌های خرابی: fetch failure با از دست رفتن executor، skew، پارتیشن‌های ریز که میلیون‌ها block کوچک می‌سازند، و فشار روی external shuffle service یا دیسک. AQE می‌تواند پارتیشن‌های ریز پس از shuffle را خودکار ادغام کند.

سؤال مصاحبه: «چرا یک join کند است؟» پاسخ: معمولاً هر دو طرف shuffle می‌شوند؛ plan را می‌بینم، زودتر فیلتر می‌کنم، اگر جا شود طرف کوچک را broadcast می‌کنم و تعداد پارتیشن و skew را بررسی می‌کنم.
