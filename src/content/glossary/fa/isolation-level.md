---
term: "سطح انزوا (Isolation level)"
term_en: "Isolation level"
aliases: ["isolation level", "سطح isolation", "read committed", "serializable", "snapshot isolation"]
category: storage
short: "تنظیمی که مشخص می‌کند تراکنش چه ناهنجاری‌های همروندی (dirty read، non-repeatable read، phantom، write skew) را ممکن است ببیند."
related: ["acid", "mvcc", "oltp"]
---
## به زبان ساده (مبتدی)
دو نفر هم‌زمان یک spreadsheet را ویرایش می‌کنند. سطح isolation مشخص می‌کند هرکس چقدر از تغییرات نیمه‌کاره‌ی دیگری را می‌بیند. قواعد سخت همه‌چیز را مرتب نگه می‌دارد ولی مردم منتظر هم می‌مانند؛ قواعد شل سریع‌تر است ولی نتایج عجیب می‌دهد، مثل اینکه هر دو آخرین مرخصی را تأیید کنند.

## نحوه‌ی کار (متوسط)
ناهنجاری‌ها: dirty read (دیدن داده‌ی commit‌نشده)، non-repeatable read (خواندن دوباره‌ی یک سطر مقدار متفاوت می‌دهد)، phantom (کوئری تکراری سطرهای جدید برمی‌گرداند)، lost update و write skew (دو تراکنش داده‌ی هم‌پوشان می‌خوانند و سطرهای جدا می‌نویسند و یک invariant نقض می‌شود). استاندارد SQL چهار سطح بر اساس ناهنجاری‌های ممنوع تعریف می‌کند؛ پیاده‌سازی‌ها فرق دارند.

```
Level               Dirty  NonRepeatable  Phantom  WriteSkew
READ UNCOMMITTED     yes       yes          yes      yes
READ COMMITTED       no        yes          yes      yes     (Postgres default)
REPEATABLE READ      no        no           yes*     yes     (*Postgres RR = snapshot isolation: no phantoms, but write skew)
SERIALIZABLE         no        no           no       no

-- Write skew: two on-call doctors both check "someone else is on call", both go off.
```

پیاده‌سازی: مبتنی بر قفل (two-phase locking) یا snapshot با MVCC. snapshot isolation بیشتر ناهنجاری‌های خواندن را می‌بندد ولی write skew را اجازه می‌دهد؛ serializable واقعی SSI (در Postgres `SERIALIZABLE`)، predicate locking یا اجرای واقعاً سریال می‌خواهد. سطح بالاتر یعنی abort/retry یا blocking بیشتر؛ برنامه باید خطاهای serialization را retry کند.

## نگاه سنیور و نکات مصاحبه
پیش‌فرض واقعی دیتابیس‌تان را بدانید؛ بسیاری گمان می‌کنند تضمینی قوی‌تر از read committed هست. در مهندسی داده: استخراج snapshot سازگار از OLTP برای full load به snapshot از نوع repeatable-read (یا snapshot مربوط به logical replication slot) نیاز دارد وگرنه ترکیبی از زمان‌های مختلف می‌خوانید. تراکنش snapshot طولانی vacuum/purge را عقب می‌اندازد. در lakehouse نویسنده‌ها optimistic concurrency دارند و isolation (serializable یا snapshot) به‌ازای جدول قابل‌تنظیم است.

سؤال مصاحبه: «write skew چیست و کدام سطح جلوی آن را می‌گیرد؟» پاسخ: دو تراکنش یک شرط را می‌خوانند و هرکدام سطر متفاوتی را به‌روز می‌کنند به‌طوری که با هم invariant را می‌شکنند (هر دو پزشک از نوبت‌کاری می‌روند)؛ snapshot isolation اجازه می‌دهد، serializable (SSI یا قفل) جلویش را می‌گیرد، یا تعارض را با قفل/قید مادی می‌کنیم.
