---
term: "کلید داغ (Hot key)"
term_en: "Hot key"
aliases: ["hot key", "کلید داغ", "hot partition", "hotspot", "پارتیشن داغ"]
category: distributed
short: "کلیدی که سهم نامتناسبی از ترافیک یا داده را می‌گیرد و پارتیشن یا نودی را که صاحب آن است اشباع می‌کند."
related: ["data-skew", "salting", "sharding", "partition"]
---
## به زبان ساده (مبتدی)
اگر صد هزار طرفدار همه بخواهند به یک آدم معروف پیام بدهند، صف صندوق پیام او قفل می‌شود و صندوق بقیه خالی است. hot key هم همین است: یک کلید (کاربر، محصول یا تاریخ) آن‌قدر ترافیک می‌گیرد که تنها ماشینِ مسئولش نمی‌رسد.

## نحوه‌ی کار (متوسط)
سیستم‌ها کلیدها را با hash یا range به پارتیشن می‌دهند و بار یکنواخت فرض می‌کنند. اگر دسترسی skewed باشد (شبیه Zipf)، صاحب کلید محبوب CPU، حافظه یا I/O را اشباع می‌کند و بقیه بیکارند. در جاهای زیادی دیده می‌شود: پارتیشن‌های Kafka و consumer lag، عملگرهای keyed در Spark/Flink، پارتیشن‌های Cassandra/DynamoDB (throttling)، رقابت روی یک کلید Redis، و کلیدهای بازه‌ی زمانی که جدیدترین بازه همه‌ی نوشتن‌ها را می‌گیرد.

```
-- Kafka: all events of a celebrity share one partition
partition = hash("user:celebrity") % 12    -- one partition, one consumer thread

-- Mitigation: split the key
key = "user:celebrity#" + (random() % 16)  -- 16 sub-keys, merge downstream

-- Detect
SELECT key, COUNT(*) c FROM events GROUP BY key ORDER BY c DESC LIMIT 10;
```

راه‌حل‌ها: شکستن کلید به زیرکلید (salting) و ادغام نتایج؛ تکثیر خواندن‌های داغ (cache، read replica)؛ batch/تجمیع نوشتن‌ها قبل از رسیدن به کلید (pre-aggregation محلی، شمارنده‌های shard‌شده روی زیرکلیدها)؛ کلید پارتیشن مرکب (کاربر + bucket زمانی)؛ و مدیریت جدا برای N کلید سنگین.

## نگاه سنیور و نکات مصاحبه
اول تشخیص دهید: نمونه‌گیری فرکانس هر کلید، متریک هر پارتیشن و tail latency به‌تفکیک پارتیشن. read-hot را از write-hot جدا کنید چون راه‌حل‌ها فرق دارند. شکستن کلید ترتیب تک‌کلیدی را از بین می‌برد و مرحله‌ی merge می‌خواهد؛ پس فقط برای کلیدهای سنگین شناخته‌شده (salting پویا) اعمال کنید نه همه‌جا. در Kafka اگر ترتیب بر اساس کلید لازم نیست producer بدون کلید یا round-robin از پارتیشن داغ جلوگیری می‌کند. adaptive capacity در DynamoDB کمک می‌کند ولی تا ابد نه. کلید را پیش از بزرگ شدن داده بر اساس الگوی دسترسی طراحی کنید.

سؤال مصاحبه: «یک پارتیشن Cassandra اشباع شده؛ مدل داده را چطور اصلاح می‌کنی؟» پاسخ: یک bucket (زمان یا پسوند hash) به partition key اضافه می‌کنم تا اندازه‌ی پارتیشن محدود و بار پخش شود، و scatter-gather روی bucketها را برای آن موجودیت می‌پذیرم.
