---
term: "Vacuum"
term_en: "Vacuum"
aliases: ["vacuum","پاکسازی فایل","expire snapshots"]
category: lakehouse
short: "حذف فایل‌های داده‌ای که دیگر به جدول ارجاع ندارند و از بازه‌ی نگهداری گذشته‌اند."
related: ["time-travel","compaction","delta-lake","iceberg"]
---
## به زبان ساده (مبتدی)
بیرون‌بردن زباله: نسخه‌های قدیمی جدول که ریخته مانده‌اند برای آزادسازی فضا پاک می‌شوند، اما فقط وقتی مطمئنی کسی لازمشان ندارد.

## نحوه‌ی کار (متوسط)
table formatها فایل‌های قدیمی را برای time travel و خواننده‌های هم‌زمان نگه می‌دارند. VACUUM (در Delta) یا expire_snapshots و remove_orphan_files (در Iceberg) فایل‌هایی را که هیچ snapshot نگهداری‌شده‌ای به آن‌ها ارجاع نمی‌دهد حذف می‌کنند. VACUUM در Postgres tupleهای مرده‌ی MVCC را بازیابی می‌کند.

## نگاه سنیور و نکات مصاحبه
retention باید از طولانی‌ترین query/stream و وعده‌ی time travel شما بیشتر باشد؛ vacuum تهاجمی خواننده‌ها و rollback را می‌شکند. حذف orphan باید از فایل‌های نوشتن در جریان دوری کند. با compaction زمان‌بندی شود.

سؤال مصاحبه: «بعد از delete حجم ذخیره بالا می‌رود، چرا؟» پاسخ: delete فقط snapshot جدید می‌افزاید؛ فایل‌های قدیمی تا انقضای snapshotها و vacuum می‌مانند.
