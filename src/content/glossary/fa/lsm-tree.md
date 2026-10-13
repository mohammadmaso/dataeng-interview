---
term: "درخت LSM (Log-Structured Merge-tree)"
term_en: "LSM-tree (log-structured merge-tree)"
aliases: ["lsm", "درخت lsm", "sstable"]
category: storage
short: "ساختار بهینه برای نوشتن که نوشتن‌ها را در حافظه بافر می‌کند، فایل‌های مرتب و تغییرناپذیر flush می‌کند و در پس‌زمینه ادغام می‌کند."
related: ["wal", "compaction", "bloom-filter"]
---
## به زبان ساده (مبتدی)
تصور کنید یادداشت‌های جدید را سریع روی یک کاغذ چسب‌دار روی میز می‌نویسید و هر چند وقت یک بار کل کاغذها را مرتب در پوشه می‌گذارید و پوشه‌ها را ادغام می‌کنید. نوشتن سریع است چون برای درج هرگز بایگانی را جست‌وجو نمی‌کنید. درخت LSM همین کار را می‌کند: نوشتن‌ها را در حافظه جذب می‌کند و بعداً بازسازمان‌دهی می‌کند.

## نحوه‌ی کار (متوسط)
نوشتن‌ها به WAL و یک ساختار مرتب در حافظه (memtable) می‌روند. وقتی پر شد، memtable به‌صورت فایل مرتب و تغییرناپذیر (SSTable) flush می‌شود. حذف‌ها tombstone هستند و update‌ها نسخه‌ی جدید. یک compaction پس‌زمینه فایل‌ها را ادغام می‌کند، مقادیر بازنویسی‌شده و tombstoneها را حذف می‌کند و تعداد فایل‌ها را محدود نگه می‌دارد.

```
write -> WAL (durability)
      -> memtable (sorted, in RAM)
memtable full -> flush -> SSTable L0 (immutable, sorted)
background compaction merges L0 -> L1 -> L2 ...
read -> memtable -> bloom filter -> SSTables newest to oldest
```

خواندن باید memtable و احتمالاً چند SSTable را بررسی کند؛ پس از Bloom filter، sparse index و block cache برای رد کردن فایل‌ها استفاده می‌شود. سبک‌های compaction: size-tiered (write amplification کمتر، فضا و read amplification بیشتر) در برابر leveled (خواندن و فضای بهتر، write amplification بیشتر). در Cassandra، RocksDB، HBase، LevelDB و MergeTree در ClickHouse (ایده‌ی مشابه با part) استفاده می‌شود.

## نگاه سنیور و نکات مصاحبه
LSM در برابر B-tree: LSM نوشتن تصادفی را ترتیبی می‌کند و در workloadهای write-heavy برنده است؛ B-tree خواندن قابل‌پیش‌بینی‌تر و update در جا می‌دهد. مصالحه‌ی RUM برقرار است: هم‌زمان نمی‌توان سربار read، update و حافظه را کمینه کرد. مشکلات عملیاتی: توقف compaction و throttle نوشتن، space amplification هنگام compaction، انباشت tombstone که خواندن را کند می‌کند (هشدار tombstone در Cassandra) و تنظیم compaction با workload.

سؤال مصاحبه: «چرا Cassandra در نوشتن سریع است؟» پاسخ: نوشتن فقط append به commit log و درج در حافظه است بدون read-before-write؛ فایل‌های مرتب تغییرناپذیر با flush ساخته و با compaction ادغام می‌شوند، به بهای read/write amplification که با Bloom filter و استراتژی compaction مدیریت می‌شود.
