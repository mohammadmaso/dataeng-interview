---
term: "شاردینگ (Sharding)"
term_en: "Sharding"
aliases: ["شاردینگ", "شارد", "shard"]
category: distributed
short: "تقسیم افقی دیتاست بین چند ماشین بر اساس shard key تا هر نود فقط بخشی از داده را نگه دارد و سرو کند."
related: ["partition", "replication", "hot-key"]
---
## به زبان ساده (مبتدی)
یک صندوقدار نمی‌تواند جمعیت یک استادیوم را سرویس دهد. چندین باجه باز می‌کنید و مردم را مثلاً بر اساس حرف اول نام خانوادگی به باجه‌ها می‌سپارید. شاردینگ همین کار را برای داده می‌کند: هیچ ماشینی همه‌چیز را ندارد و درخواست‌ها به ماشینی می‌روند که مسئول آن کلید است.

## نحوه‌ی کار (متوسط)
shard بخشی از داده است که روی نود خودش (یا replica set خودش) قرار می‌گیرد. router یا کتابخانه‌ی کلاینت کلید را به shard نگاشت می‌کند. استراتژی‌ها: hash (توزیع یکنواخت، بدون range scan)، range (مرتب، پشتیبانی از کوئری بازه‌ای، ریسک نقطه‌ی داغ)، جدول directory/lookup (انعطاف‌پذیر، یک hop اضافه) و consistent hashing (کمترین جابه‌جایی هنگام افزودن نود).

```
-- hash sharding
shard = hash(user_id) % N          -- simple, but N change reshuffles everything
shard = consistent_hash(user_id)   -- ring: adding node moves ~1/N of keys

-- range sharding
[a-g] -> shard1, [h-p] -> shard2, [q-z] -> shard3   -- range scans, hotspot risk

-- ClickHouse: Distributed table over shards with a sharding key
ENGINE = Distributed(cluster, db, events_local, cityHash64(user_id))
```

شاردینگ ذخیره‌سازی و throughput نوشتن را افقی مقیاس می‌دهد، در حالی که replication ظرفیت خواندن و دسترس‌پذیری را؛ سیستم‌های production هر دو را ترکیب می‌کنند (هر shard چند replica دارد). در Cassandra، MongoDB، Elasticsearch، کلاسترهای ClickHouse، Vitess و Kafka (پارتیشن‌ها) استفاده می‌شود.

## نگاه سنیور و نکات مصاحبه
بخش‌های سخت: انتخاب shard key (cardinality بالا، دسترسی یکنواخت، سازگار با الگوی کوئری)؛ shard داغ و کلیدهای پرمخاطب؛ کوئری و join بین shardها (scatter-gather و tail latency بالاتر)؛ تراکنش بین shardها (2PC یا saga، یا اجتناب)؛ و resharding بدون downtime (virtual node یا shardهای منطقی ریز کمک می‌کند). تغییر کلید بد بعداً گران است. داده‌هایی را که با هم کوئری می‌شوند هم‌مکان کنید (shard key یکسان برای جدول‌های مرتبط) تا joinها محلی بمانند.

سؤال مصاحبه: «جدول user-events را چطور shard می‌کنی؟» پاسخ: با hash کاربر تا داده‌ی هر کاربر هم‌مکان و بار یکنواخت باشد؛ کلید بازه‌ای زمانی به shard جدید فشار می‌آورد. برای resharding از consistent hashing یا shardهای مجازی زیاد، هر shard را replicate و برای کوئری سراسری scatter-gather را می‌پذیرم.
