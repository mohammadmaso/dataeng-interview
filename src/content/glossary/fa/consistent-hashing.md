---
term: "هشینگ سازگار (Consistent hashing)"
term_en: "Consistent hashing"
aliases: ["consistent hashing", "حلقه‌ی hash", "virtual node", "vnode"]
category: distributed
short: "روش hash که در آن افزودن یا حذف نود فقط حدود 1/N کلیدها را جابه‌جا می‌کند، نه تقریباً همه را."
related: ["sharding", "partition", "hot-key"]
---
## به زبان ساده (مبتدی)
فرض کنید کتاب‌ها با فرمولی که به تعداد قفسه‌ها بستگی دارد به قفسه‌ها داده می‌شوند. یک قفسه اضافه کنید فرمول برای تقریباً همه‌ی کتاب‌ها عوض می‌شود و باید کل کتابخانه را دوباره بچینید. هشینگ سازگار چیدمان هوشمندتری دارد که افزودن قفسه فقط بخش کوچکی از کتاب‌ها را جابه‌جا می‌کند.

## نحوه‌ی کار (متوسط)
کلیدها و نودها روی یک فضای دایره‌ای hash می‌شوند. هر کلید به نود بعدی در جهت عقربه‌های ساعت تعلق دارد. وقتی نودی اضافه شود فقط بخشی از بازه‌ی یک همسایه را می‌گیرد؛ وقتی برود کلیدهایش به نود بعدی می‌روند. virtual nodeها (هر نود فیزیکی در موقعیت‌های زیادی از حلقه) عدم توازن را هموار می‌کنند و ماشین‌های ناهمگن سهم متناسب می‌گیرند.

```
Naive: node = hash(key) % N      -> N from 3 to 4 moves ~75% of keys

Ring: place nodes (and many virtual nodes each) at hash(node_id) on a 0..2^32 ring.
      key goes to the first node clockwise from hash(key).
      add a node -> it takes over only the arc just before it (~1/N of keys)

  vnodes: each physical node owns e.g. 128-256 small arcs -> even load, smooth rebalancing
```

در storeهای سبک Dynamo (Cassandra، Riak)، cache (کلاینت‌های memcached، مسیریابی CDN)، load balancer و shard کردن consumerها استفاده می‌شود. replication کپی‌ها را روی R نود بعدی می‌گذارد. rendezvous (HRW) و jump hash جایگزین‌هایی با ویژگی جابه‌جایی کم هستند.

## نگاه سنیور و نکات مصاحبه
مصالحه‌ها: به نمای عضویت/حلقه‌ای نیاز دارد که همه‌ی کلاینت‌ها روی آن توافق کنند و داده باید به نود جدید استریم شود (بار rebalancing، hinted handoff). hash خالص بار را نمی‌بیند پس hot keyها هنوز نقطه‌ی داغ می‌سازند. range partitioning (HBase، Spanner، Cockroach) یکنواختی hash را با range scan و split پویا معاوضه می‌کند. Kafka از consistent hashing استفاده نمی‌کند: کلید به پارتیشن با `hash % partitions` می‌رود و برای همین افزایش پارتیشن‌ها locality کلید را می‌شکند.

سؤال مصاحبه: «چرا برای کلاستر cache هشینگ سازگار و نه hash mod N؟» پاسخ: با mod N تغییر اندازه اکثر کلیدها را دوباره نگاشت می‌کند و طوفان cache miss می‌سازد؛ هشینگ سازگار فقط حدود 1/N کلیدها را جابه‌جا می‌کند و virtual nodeها بار را یکنواخت نگه می‌دارند.
