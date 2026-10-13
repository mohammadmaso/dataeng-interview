---
term: "ACID"
term_en: "ACID"
aliases: ["ویژگی‌های acid", "تراکنش acid", "اتمیک بودن"]
category: storage
short: "چهار تضمین تراکنش: Atomicity (اتمیک)، Consistency (سازگاری)، Isolation (انزوا) و Durability (دوام)."
related: ["oltp", "mvcc", "wal", "isolation-level"]
---
## به زبان ساده (مبتدی)
حواله‌ی بانکی باید یا کامل انجام شود یا اصلاً نشود و وسط راه پول گم نشود. باید قواعد را رعایت کند، حواله‌های هم‌زمان دیگر مزاحمش نشوند و بعد از قطع برق هم بماند. این چهار قول همان ACID است.

## نحوه‌ی کار (متوسط)
Atomicity: همه‌ی عملیات تراکنش اعمال می‌شوند یا هیچ‌کدام (با undo/rollback). Consistency: تراکنش دیتابیس را از یک حالت معتبر به حالت معتبر دیگر می‌برد و قیود اعمال می‌شوند (بخشی با برنامه). Isolation: تراکنش‌های هم‌زمان کار نیمه‌کاره‌ی هم را نمی‌بینند، تا حدی که سطح isolation تعیین می‌کند. Durability: داده‌ی commit‌شده از crash جان به در می‌برد، با write-ahead log و fsync یا replication.

```
BEGIN;
  UPDATE accounts SET bal = bal - 100 WHERE id = 1;   -- A: both or neither
  UPDATE accounts SET bal = bal + 100 WHERE id = 2;
  -- C: CHECK (bal >= 0) and FKs must hold at commit
COMMIT;  -- D: after this returns, survives crash (WAL fsync)
-- I: concurrent transactions behave per the isolation level
```

در مهندسی داده ACID برای table formatها (Iceberg/Delta روی object storage commit اتمیک می‌دهند)، sinkها (انتشار اتمیک یک batch) و مقایسه با NoSQLهای سبک BASE که بعضی ویژگی‌ها را شل می‌کنند مهم است.

## نگاه سنیور و نکات مصاحبه
حرف C در ACID با C در CAP (linearizability) فرق دارد؛ قاطی‌کردنشان اشتباه کلاسیک است. Isolation شل‌ترین و بدفهمیده‌ترین حرف است: سطح پیش‌فرض بسیاری از دیتابیس‌ها (read committed، snapshot) ناهنجاری‌هایی مثل write skew را اجازه می‌دهد. ACID توزیع‌شده به 2PC یا تراکنش replicate‌شده با consensus و هزینه‌ی latency نیاز دارد. بسیاری از NoSQLها اتمیک بودن را فقط در سطح سطر/سند می‌دهند. ACID در lakehouse معمولاً خوش‌بینانه است: serializable یا snapshot isolation در سطح جدول با conflict در نوشتن‌های هم‌زمان.

سؤال مصاحبه: «حرف D در ACID چه می‌خواهد؟» پاسخ: داده‌ی commit‌شده باید از crash جان به در ببرد؛ پس commit فقط بعد از رسیدن تغییر به لاگ پایدار (WAL با fsync یا replicate به quorum) تأیید می‌شود؛ شل‌کردنش (async commit) احتمال از دست رفتن commitهای اخیر را با latency کمتر معاوضه می‌کند.
