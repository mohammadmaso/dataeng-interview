---
term: "عدم توازن داده (Data skew)"
term_en: "Data skew"
aliases: ["skew", "اسکیو", "چولگی داده", "عدم توازن داده"]
category: batch
short: "توزیع نامتوازن داده بین پارتیشن‌ها یا کلیدها که باعث می‌شود چند task بیشترین کار را بکنند و کل job کند شود."
related: ["shuffle", "hot-key", "salting", "broadcast-join", "partition"]
---
## به زبان ساده (مبتدی)
یک سوپرمارکت با ده صندوق را تصور کنید که تقریباً همه‌ی مشتری‌ها در یک صندوق صف می‌کشند. فروشگاه در کل شلوغ نیست ولی همه منتظر همان یک صندوق‌اند. skew هم همین است: کار بر اساس کلید تقسیم می‌شود اما چند کلید داده‌ی عظیمی دارند؛ پس یک worker زیر بار می‌رود و بقیه بیکارند.

## نحوه‌ی کار (متوسط)
در موتورهای توزیع‌شده سطرها بر اساس hash کلید به پارتیشن‌ها می‌روند (برای `GROUP BY`، `JOIN`، `repartition`). اگر یک کلید (مثلاً `NULL`، یک user id پیش‌فرض یا حساب پرمخاطب) سهم بزرگی از سطرها را داشته باشد، پارتیشن آن کُند می‌شود. علائم در Spark UI: یک task خیلی طولانی‌تر از میانه، shuffle read عظیم برای یک task، spill یا OOM در یک executor.

```
-- Find the heavy keys
SELECT user_id, COUNT(*) c FROM events
GROUP BY user_id ORDER BY c DESC LIMIT 20;

# Spark 3: let AQE split skewed shuffle partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

راه‌حل‌ها: AQE skew join، broadcast طرف کوچک، salting کلیدهای داغ، فیلتر یا جداسازی `NULL` و کلیدهای پیش‌فرض، pre-aggregation قبل از shuffle، انتخاب کلید پارتیشن بهتر، یا مسیر جدا برای کلیدهای داغ.

## نگاه سنیور و نکات مصاحبه
skew ویژگی داده است نه کلاستر، پس اضافه‌کردن executor کمکی نمی‌کند؛ طولانی‌ترین task زمان کل را تعیین می‌کند. بررسی کنید که skew کلیدی است (چند کلید سنگین) یا skew اندازه‌ی پارتیشن به‌خاطر چیدمان فایل‌ها. salting طرف کوچک را چندبرابر می‌کند و پیچیدگی می‌آورد؛ AQE ارزان‌تر است ولی فقط joinهای shuffle قابل‌تقسیم را پوشش می‌دهد. skew در Kafka (پارتیشن داغ از کلید بد)، state کلیددار Flink و دیتابیس‌های shard‌شده هم دیده می‌شود.

سؤال مصاحبه: «در یک join اسپارک ۱۹۹ task تمام شده و یکی یک ساعت است که اجراست؛ چه می‌کنی؟» پاسخ: با متریک task و شمارش کلیدها skew را تأیید می‌کنم، سپس AQE skew join یا broadcast طرف کوچک؛ اگر چند کلید داغ ماند salting یا پردازش جدا و union نتیجه.
