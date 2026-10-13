---
term: "سفر در زمان (Time travel)"
term_en: "Time travel"
aliases: ["time travel", "snapshot query", "rollback", "کوئری گذشته"]
category: lakehouse
short: "کوئری گرفتن از جدول به همان شکل که در نسخه یا زمان گذشته بود؛ با فایل‌های تغییرناپذیر و متادیتای snapshot ممکن می‌شود."
related: ["iceberg", "delta-lake", "mvcc"]
---
## به زبان ساده (مبتدی)
مثل تاریخچه‌ی نسخه‌ی یک سند است: می‌توانید ببینید سند سه‌شنبه‌ی گذشته چطور بود، با الان مقایسه کنید یا بعد از ویرایش بد نسخه‌ی قدیمی را برگردانید. time travel این تور ایمنی را به جدول‌ها می‌دهد.

## نحوه‌ی کار (متوسط)
چون فایل‌های داده تغییرناپذیرند و هر commit یک snapshot جدید می‌سازد که فایل‌های سازنده‌ی جدول را فهرست می‌کند، snapshotهای قدیمی فقط به مجموعه‌فایل‌های قدیمی‌تر اشاره می‌کنند. خواندن «as of» نسخه یا زمان یعنی resolve کردن snapshot و خواندن فایل‌هایش. کاربردها: ممیزی و تکرارپذیری (اجرای دوباره‌ی گزارش یا مدل روی همان داده)، دیباگ (بین دو اجرا چه تغییر کرد)، rollback بعد از نوشتن یا job بد و خواندن افزایشی (diff بین snapshotها).

```
-- Iceberg / Spark
SELECT * FROM db.orders FOR TIMESTAMP AS OF '2024-05-01 00:00:00';
SELECT * FROM db.orders FOR VERSION AS OF 4817263548223;
CALL system.rollback_to_snapshot('db.orders', 4817263548223);

-- Delta
SELECT * FROM orders VERSION AS OF 42;
RESTORE TABLE orders TO VERSION AS OF 42;

-- Snowflake / BigQuery:  AT (TIMESTAMP => ...)  /  FOR SYSTEM_TIME AS OF ...
```

فقط تا زمانی کار می‌کند که فایل‌های قدیمی وجود دارند: snapshot expiry (`expire_snapshots`) و `VACUUM` فایل‌های بدون ارجاع را فیزیکی حذف می‌کنند و بازه‌ی time travel (retention) را محدود می‌کنند. warehouseهایی مثل Snowflake و BigQuery پنجره‌های مشابه با دوره‌های fail-safe دارند.

## نگاه سنیور و نکات مصاحبه
time travel backup نیست: ذخیره‌سازی و کاتالوگ را با جدول زنده شریک است، retention محدود است و متادیتای خراب یا drop اتفاقی جدول ممکن است آن را بی‌اثر کند. مصالحه: retention طولانی‌تر هزینه‌ی ذخیره دارد و عملیات متادیتا را کند می‌کند. با الزام حذف (GDPR) تضاد دارد: داده‌ی حذف‌شده تا انقضای snapshotها خواندنی می‌ماند، پس retention را با انطباق هماهنگ کنید. برای ML تکرارپذیر شناسه‌ی snapshot را برای هر اجرای آموزش ثبت کنید. branch و tag (Iceberg) این را به workflowهای write-audit-publish و نسخه‌های نام‌دار طولانی‌عمر گسترش می‌دهد.

سؤال مصاحبه: «job بدی یک ساعت پیش جدول را بازنویسی کرده؛ چطور بازیابی می‌کنی؟» پاسخ: آخرین snapshot یا نسخه‌ی سالم را از تاریخچه‌ی جدول پیدا می‌کنم، به آن rollback یا restore می‌کنم (یا branch می‌سازم و راستی‌آزمایی می‌کنم) و بعد job را اصلاح می‌کنم؛ فقط اگر retention فایل‌های قدیمی را منقضی نکرده باشد کار می‌کند، پس retention معقول و هشدار audit هم می‌گذارم.
