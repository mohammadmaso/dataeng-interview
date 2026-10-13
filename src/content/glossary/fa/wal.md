---
term: "لاگ نوشتن پیشین (WAL)"
term_en: "Write-ahead log (WAL)"
aliases: ["wal", "write-ahead log", "لاگ تراکنش", "لاگ پیش‌نویس"]
category: storage
short: "لاگ فقط‌افزودنی که تغییرات پیش از اعمال در آن ثبت می‌شوند؛ برای دوام، بازیابی بعد از crash و replication."
related: ["cdc", "mvcc", "checkpoint", "lsm-tree"]
---
## به زبان ساده (مبتدی)
صاحب مغازه هر فروش را قبل از گذاشتن پول در کشو خط‌به‌خط در دفتر ثبت می‌کند. اگر وسط روز برق برود، دفتر دقیقاً می‌گوید چه شده و کشو درست می‌شود. WAL همان دفتر برای دیتابیس است: تغییر اول به‌صورت ایمن و به ترتیب ثبت می‌شود، بعد اعمال می‌گردد.

## نحوه‌ی کار (متوسط)
نوشتن در جاهای تصادفی فایل‌های داده کند و غیراتمیک است. دیتابیس‌ها به‌جای آن رکورد فشرده‌ی هر تغییر را به لاگ ترتیبی append و fsync می‌کنند؛ فقط بعد از آن تراکنش commit می‌شود. صفحه‌های داده در حافظه به‌روز می‌شوند و بعداً flush می‌گردند (checkpoint). پس از crash موتور WAL را از آخرین checkpoint دوباره اجرا می‌کند (redo) و کارهای commit‌نشده را برمی‌گرداند (undo).

```
client: UPDATE accounts SET bal = bal - 10 WHERE id = 1;

1. append record to WAL     (sequential write)
2. fsync WAL                (now durable)
3. ack to client
4. later: flush dirty page to data file (checkpoint)

crash? -> replay WAL from last checkpoint
```

این لاگ فراتر از بازیابی هم کاربرد دارد: replicaها آن را replay می‌کنند (replication فیزیکی)، ابزارهای CDC آن را decode می‌کنند (logical decoding در Postgres، binlog در MySQL) و point-in-time recovery از backup پایه به‌علاوه‌ی WAL آرشیوشده برمی‌گردد. commit log در Kafka، درخت‌های LSM (memtable محافظت‌شده با لاگ) و Raft همین ایده را به کار می‌برند.

## نگاه سنیور و نکات مصاحبه
append ترتیبی دوام را با throughput بالا می‌دهد ولی latency مربوط به `fsync` سقف latency commit است؛ group commit چند تراکنش را در یک fsync جمع می‌کند. ریسک‌های عملیاتی: رشد WAL وقتی replica یا replication slot عقب بماند (پر شدن دیسک)، اوج‌های checkpoint، و تنظیمات دوام که برای سرعت قربانی می‌شوند (`synchronous_commit=off`، خاموش کردن fsync) و ممکن است commitهای اخیر را از بین ببرند. torn write نیاز به checksum یا full-page write دارد.

سؤال مصاحبه: «دیتابیس چطور از crash جان سالم به در می‌برد؟» پاسخ: تغییرات قبل از ack به commit در WAL نوشته و fsync می‌شوند؛ در restart لاگ از آخرین checkpoint replay می‌شود تا کارهای commit‌شده redo و بقیه undo شوند؛ بدین ترتیب دوام و اتمیک بودن بدون نوشتن همزمان صفحه‌های داده برقرار است.
