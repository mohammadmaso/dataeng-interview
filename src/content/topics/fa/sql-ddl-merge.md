---
order: 28
tid: "sql-ddl-merge"
track: "2"
section: null
num: "2.13"
title: "DDL, Upserts & MERGE"
type: "concept"
level: "intermediate"
priority: "P0"
est_hours: 2
summary: "طراحی جدول، constraintها، جدول partition‌شده، و الگوی upsert با MERGE یا INSERT ... ON CONFLICT. پایه‌ی بارگذاری افزایشی."
covers: ["idempotent loads","staging-table swap pattern","truncate-insert vs delete-insert per partition"]
interview_qs: []
extras: []
prerequisites: ["sql-fundamentals", "sql-transactions"]
related: ["dedup-idempotent-loads", "pattern-dedup", "pattern-backfill", "partitioning-sharding", "scd", "schema-evolution", "sql-window-functions"]
---

## 🎯 خلاصه

> **DDL** (زبان تعریف داده) ساختار را تعریف می‌کند: جدول، کلید، constraint و partition. **upsert** یعنی «اگر ردیف جدید است وارد کن، اگر وجود دارد به‌روزش کن». **MERGE** هر دو کار را در یک دستور برای کل یک batch انجام می‌دهد.

- اول **کلید** را طراحی کن. یک کلید طبیعی (مثل `customer_id`) یا یک کلید جایگزین (surrogate) ردیف را مشخص می‌کند؛ upsert به یک constraint از نوع unique روی آن نیاز دارد.
- upsert با `INSERT ... ON CONFLICT` (در PostgreSQL) یا `MERGE` (استاندارد SQL، پشتیبانی‌شده در PostgreSQL 15+، BigQuery، Snowflake، Databricks و SQL Server) نوشته می‌شود.
- بارگذاری **idempotent** یعنی اجرای دوباره‌اش همان جدول نهایی را می‌سازد. هدف هر pipeline افزایشی همین است.
- دو الگو بارگذاری را امن می‌کنند: **جابه‌جایی جدول staging** (داده‌ی جدید را کنار بساز، بعد جابه‌جا کن) و **بازنویسی partition** (کل یک partition را از نو بساز، به‌جای وصله‌کردن ردیف‌ها).

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** لیست مهمان‌های یک مهمانی. وقتی مهمانی می‌آید، لیست را نگاه می‌کنی. اگر اسمش تازه است، اضافه‌اش می‌کنی. اگر قبلاً هست، اطلاعاتش را به‌روز می‌کنی (مثلاً شماره‌ی جدید). upsert دقیقاً همین است: «اگر جدید است اضافه کن، اگر شناخته‌شده است به‌روز کن». اسم مهمان همان کلید است.

از یک جدول کوچک شروع کن که یک job شبانه پرش می‌کند:

```sql
CREATE TABLE dim_customer (
  customer_id text PRIMARY KEY,         -- کلید طبیعی از سیستم مبدأ
  city        text,
  updated_at  timestamp NOT NULL
);
```

فردا سیستم مبدأ دو نوع ردیف می‌فرستد: مشتری‌های جدید و مشتری‌های تغییرکرده. یک `INSERT` ساده روی ردیف‌های تغییرکرده خطا می‌دهد، چون کلید از قبل هست. upsert هر دو را درست انجام می‌دهد:

```sql
INSERT INTO dim_customer (customer_id, city, updated_at)
VALUES ('C1', 'Tehran', '2026-03-01 10:00')
ON CONFLICT (customer_id)
DO UPDATE SET city = EXCLUDED.city, updated_at = EXCLUDED.updated_at;
```

`EXCLUDED` یعنی «ردیفی که می‌خواستی وارد کنی». اگر همین دستور را دوباره با همان مقدارها اجرا کنی هیچ چیزی تغییر نمی‌کند: اولین طعم idempotency.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### constraint ها و کلیدها

| constraint | چه چیزی را تضمین می‌کند | نکته |
|---|---|---|
| `NOT NULL` | هر ردیف مقدار دارد | ارزان، همیشه مفید |
| `PRIMARY KEY` | یکتا و غیرِ null؛ فقط یکی در هر جدول | هدف upsert و foreign key |
| `UNIQUE` | هیچ دو ردیفی مقدار یکسان ندارند | برای `ON CONFLICT` به‌عنوان هدف تداخل لازم است |
| `CHECK` | قانون سطح ردیف، مثل `amount >= 0` | در موتورهای OLTP اجرا می‌شود |
| `FOREIGN KEY` | ردیف ارجاع‌شده وجود دارد | بسیاری از انبارها تعریفش می‌کنند ولی **اجرا** نمی‌کنند |

در انبارها (BigQuery، Snowflake، Redshift) کلیدهای اصلی و خارجی اغلب **اطلاعاتی (informational)** هستند: optimizer ممکن است از آن‌ها استفاده کند، ولی موتور ردیف بد را رد نمی‌کند. بررسی‌های کیفیت داده در pipeline باید این کار را انجام دهند.

### جدول‌های partition‌شده

```sql
CREATE TABLE events (
  event_time timestamptz NOT NULL,
  user_id    bigint NOT NULL,
  payload    jsonb
) PARTITION BY RANGE (event_time);

CREATE TABLE events_2026_01 PARTITION OF events
  FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

هر partition یک جدول فیزیکی است. کوئری‌هایی که روی `event_time` فیلتر می‌کنند بقیه را رد می‌کنند (pruning، ببین [[sql-query-optimization]])، و داده‌ی قدیمی را می‌شود با `DROP TABLE` روی یک partition حذف کرد، به‌جای یک `DELETE` بزرگ. در PostgreSQL کلید اصلی یک جدول partition‌شده باید کلید partition را هم شامل شود.

### INSERT ... ON CONFLICT

هدف تداخل باید یک ایندکس یا constraint از نوع unique باشد. داخل یک دستور، یک ردیف نمی‌تواند دو بار روی یک ردیف هدف اثر بگذارد، پس یک batch با دو ردیف `C1` خطای «cannot affect row a second time» می‌دهد. اول batch را یکتا کن.

### MERGE

```sql
MERGE INTO dim_customer AS t
USING staging_customer AS s
   ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET city = s.city, updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, city, updated_at)
  VALUES (s.customer_id, s.city, s.updated_at);
```

`MERGE` درج، به‌روزرسانی و (در بسیاری از موتورها) حذف را در یک دستور انجام می‌دهد. شرط `AND s.updated_at > t.updated_at` باعث می‌شود ردیف‌های منبع قدیمی نادیده گرفته شوند. مثل `ON CONFLICT`، منبعی با کلید تکراری در PostgreSQL و SQL Server خطاست، و در بعضی موتورهای دیگر انتخاب غیرقطعی انجام می‌شود. MySQL دستور `MERGE` ندارد و از `INSERT ... ON DUPLICATE KEY UPDATE` استفاده می‌کند.

### قبل از upsert یکتا کن

مرحله‌ی معمول staging، آخرین نسخه‌ی هر کلید را نگه می‌دارد:

```sql
SELECT *
FROM (
  SELECT s.*,
         ROW_NUMBER() OVER (PARTITION BY customer_id
                            ORDER BY updated_at DESC, source_seq DESC) AS rn
  FROM staging_customer s
) x
WHERE rn = 1;
```

ستون کمکی (`source_seq`) انتخاب را قطعی می‌کند. ببین [[pattern-dedup]].

### دو الگوی بارگذاری

**جابه‌جایی جدول staging.** batch را در `dim_customer_staging` بار کن، اعتبارسنجی کن (تعداد ردیف، یکتایی، بررسی‌های کیفیت)، و بعد نام‌ها را در یک تراکنش عوض کن:

```sql
BEGIN;
ALTER TABLE dim_customer RENAME TO dim_customer_old;
ALTER TABLE dim_customer_staging RENAME TO dim_customer;
COMMIT;
```

خواننده‌ها یا جدول قدیمی را می‌بینند یا جدید را، نه وضعیت نیمه‌بارگذاری‌شده را. DDL در PostgreSQL تراکنشی است، پس این کار جواب می‌دهد. انبارها عملیات مشابه دارند (در Snowflake، `ALTER TABLE ... SWAP WITH`).

**بازنویسی partition.** برای جدول fact روزانه، partition آن روز را از نو بساز به‌جای وصله‌کردن:

- *truncate-insert برای هر partition:* partition آن روز را حذف یا truncate کن و ردیف‌های بازسازی‌شده را درج کن. سریع است، ردیف مرده ندارد، و اجرای دوباره همان نتیجه را می‌دهد.
- *delete-insert برای هر partition:* ردیف‌های آن روز را `DELETE` کن و بعد درج کن. روی جدول مشترک کار می‌کند، ولی در PostgreSQL ردیف‌های حذف‌شده تا `VACUUM` به‌عنوان dead tuple می‌مانند و روی partition های بزرگ کندتر است.

دستور `INSERT OVERWRITE` در Spark با حالت dynamic partition overwrite همان الگوی truncate-insert را برای partition هایی که می‌نویسد انجام می‌دهد.

## 🧠 عمق و trade-offها (سنیور)

- **idempotency قرارداد است.** هر بارگذاری باید بعد از خطا امن اجرای دوباره باشد. یعنی: یکتاسازی قطعی، upsert روی کلید یکتای واقعی، یا بازنویسی کل partition. اگر job فقط append کند، هر تلاش مجدد داده را تکرار می‌کند. ببین [[idempotency]].
- **هزینه‌ی MERGE.** `MERGE` جدول هدف را به منبع join می‌کند. روی جدول هدف بزرگ، شرط `ON` را کلید partition هم بگیر (`ON t.event_date = s.event_date AND t.id = s.id`) تا موتور هدف را prune کند. وگرنه برای یک batch کوچک کل جدول را اسکن می‌کند.
- **قفل‌ها.** `ALTER TABLE` در PostgreSQL قفل قوی می‌گیرد. جابه‌جایی با rename سریع است، ولی منتظر تراکنش‌های در حال اجرا روی جدول می‌ماند و این انتظار می‌تواند صف کوئری‌ها را بلاک کند. در job یک `lock_timeout` بگذار تا زود شکست بخورد.
- **جابه‌جایی و وابستگی‌ها.** بعد از rename، view ها، grant ها و foreign key ها در PostgreSQL هنوز به شیء قدیمی اشاره می‌کنند. قبل از جابه‌جایی وابستگی‌ها را چک کن، یا view ها را در همان مرحله‌ی swap بساز.
- **داده‌ی دیررسیده و به‌روزشده.** upsert تغییرات را مدیریت می‌کند ولی تاریخچه را نه. اگر مقدار قبلی لازم است، جدول هدف باید SCD باشد ([[scd]])، و MERGE می‌شود «ردیف قدیمی را ببند، ردیف جدید را درج کن».
- **تغییر schema.** در PostgreSQL نسخه‌ی ۱۱ به بعد، `ADD COLUMN ... DEFAULT <constant>` فقط تغییر metadata است و جدول بازنویسی نمی‌شود. اضافه کردن ستون `NOT NULL` بدون default به جدول پرِ داده شکست می‌خورد؛ اول ستون nullable بساز، بعد دسته‌ای پرش کن، و در آخر constraint را اضافه کن. ببین [[schema-evolution]].
- **تفاوت موتورها.** انبارها اغلب بر اساس بایت‌های اسکن‌شده هزینه می‌گیرند، پس MERGE ای که کل یک partition را بازنویسی می‌کند هزینه‌ی واقعی دارد. برای نسخه‌ی خودت مستندات MERGE و partition را بخوان؛ معنای تطابق چندگانه بین موتورها فرق دارد.

## 🧪 امتحان کن

از PostgreSQL نسخه‌ی ۱۵ به بعد استفاده کن تا هم `ON CONFLICT` و هم `MERGE` را اجرا کنی.

1. جدول `dim_customer` (بالا) و جدول `staging_customer` با ستون‌های `customer_id`، `city`، `updated_at`، `source_seq` بساز. دو ردیف برای `C2` با `updated_at` متفاوت وارد کن. کوئری یکتاسازی را بنویس که آخرین را نگه دارد.
2. برای ردیف‌های یکتاشده یک upsert با `INSERT ... ON CONFLICT` اجرا کن. دوباره اجرا کن و تعداد ردیف و `updated_at` را چک کن.
3. همان بارگذاری را به‌صورت `MERGE` بنویس که ردیف‌های منبعِ قدیمی‌تر از هدف را نادیده بگیرد. بعد توضیح بده اگر جدول staging هنوز دو ردیف `C2` داشته باشد چه می‌شود.

<details><summary>راه‌حل‌ها را نشان بده</summary>

**۱.** کوئری یکتاسازی بخش متوسط را با `ORDER BY updated_at DESC, source_seq DESC` استفاده کن. ردیفی که `updated_at` جدیدتر دارد برنده است؛ `source_seq` فقط در حالت مساوی تصمیم می‌گیرد.

**۲.** `INSERT INTO dim_customer SELECT customer_id, city, updated_at FROM (کوئری یکتاشده) d ON CONFLICT (customer_id) DO UPDATE SET city = EXCLUDED.city, updated_at = EXCLUDED.updated_at;` در اجرای دوم هیچ مقداری تغییر نمی‌کند، پس نتیجه یکسان است: idempotent.

**۳.** `MERGE` بخش متوسط را استفاده کن. با دو ردیف `C2` در staging، PostgreSQL خطا می‌دهد چون یک ردیف هدف دو بار تحت تأثیر قرار می‌گیرد. راه‌حل مرحله‌ی یکتاسازی است، نه شرط `ON` بزرگ‌تر.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «چطور یک بارگذاری شبانه را idempotent می‌کنی؟»** جواب متوسط: «از upsert روی کلید اصلی استفاده می‌کنم، پس اجرای دوباره ردیف تکراری درج نمی‌کند.» جواب سنیور: «upsert فقط وقتی idempotent است که batch قبلاً به‌شکل قطعی یکتا شده باشد و کلید، کلید یکتای واقعی باشد. برای جدول‌های fact، به‌جایش partition آن روز را بازنویسی می‌کنم تا تلاش مجدد داده را جایگزین کند، نه اضافه. قبل از جابه‌جایی داده‌ی staging را اعتبارسنجی می‌کنم و شناسه‌ی اجرای job را نگه می‌دارم تا بدانم هر اجرا چه نوشته است.»

**سؤال: «MERGE یا INSERT ... ON CONFLICT؟»** برای upsert ساده‌ی ردیف به ردیف در PostgreSQL از `ON CONFLICT` استفاده کن. وقتی هم درج، هم به‌روزرسانی و هم حذف را با هم لازم داری، یا انبار فقط `MERGE` را برای بارگذاری دسته‌ای ارائه می‌دهد، از `MERGE` استفاده کن.

> **🎤 این را در مصاحبه بگو:** «بارگذاری را طوری طراحی می‌کنم که دو بار اجرایش امن باشد. batch را قطعی یکتا می‌کنم، روی کلید یکتای واقعی upsert می‌زنم، و برای fact ها کل partition را بازنویسی می‌کنم. داده را در staging اعتبارسنجی می‌کنم و با یک swap اتمی جابه‌جا می‌کنم، پس خواننده‌ها هرگز داده‌ی نیمه‌کاره نمی‌بینند.»

## ⚠️ اشتباهات رایج

> **⚠️ مراقب باش:** upsert کردن batch با کلید تکراری. دستور یا خطا می‌دهد (PostgreSQL، SQL Server) یا یک ردیف دلخواه انتخاب می‌کند (بعضی موتورها). همیشه اول با ترتیب صریح یکتا کن.

> **⚠️ مراقب باش:** استفاده از `ON CONFLICT (col)` روی ستونی که ایندکس unique ندارد. PostgreSQL رد می‌کند. هدف تداخل باید دقیقاً با یک constraint یا ایندکس unique تطابق داشته باشد.

- استفاده از `INSERT` ساده در job ای که retry می‌شود، که هر بار ردیف‌ها را تکراری می‌کند.
- فرض اینکه `FOREIGN KEY` در انبار ردیف بد را رد می‌کند. اغلب فقط مستندات است.
- اجرای `DELETE` روی یک partition بزرگ و تعجب از اینکه جدول بزرگ می‌ماند. در PostgreSQL فضا فقط بعد از `VACUUM` برمی‌گردد.
- فراموش کردن اینکه `ALTER TABLE` قفل قوی می‌گیرد و پشت تراکنش‌های طولانی منتظر می‌ماند.
- نوشتن `MERGE` بدون شرط روی `updated_at`، که اجازه می‌دهد ردیف منبع قدیمی، ردیف جدیدتر هدف را بازنویسی کند.
- اضافه کردن ستون `NOT NULL` بدون default به جدول پرِ داده.

## ✅ خودت را بسنج

۱. یک job شبانه ردیف‌ها را به یک جدول fact append می‌کند. job وسط کار خطا می‌دهد و دوباره اجرا می‌شود. محتمل‌ترین نتیجه چیست و بهترین راه‌حل کدام است؟
   - A) هیچ؛ دیتابیس همه‌چیز را rollback می‌کند، پس append ساده کافی است
   - B) ردیف‌های تکراری برای بخشی که قبلاً بارگذاری شده؛ به‌جای append، partition را بازنویسی کن یا روی کلید یکتا upsert کن
   - C) ردیف‌های گمشده؛ در هر تلاش مجدد `TRUNCATE` بزن
   - D) خطای کلید خارجی؛ foreign key ها را حذف کن

<details><summary>جواب</summary>

**B** — تلاش مجدد یک append ساده، ردیف‌هایی را که قبلاً بارگذاری شده‌اند دوباره می‌نویسد. بازنویسی partition یا upsert روی کلید یکتا باعث می‌شود تلاش مجدد همان جدول نهایی را بسازد. A فقط وقتی درست است که کل job در یک تراکنش باشد، که برای بارگذاری‌های بزرگ اغلب ممکن نیست. C داده‌ی بقیه‌ی partition ها را از بین می‌برد، و D مشکل تکرار را حل نمی‌کند.

</details>

۲. کدام جمله دربارهٔ `ON CONFLICT` دقیق‌ترین است؟
   - A) بدون هیچ ایندکس unique روی جدول کار می‌کند
   - B) هدف تداخل باید با یک constraint یا ایندکس unique تطابق داشته باشد
   - C) می‌تواند در یک دستور، یک ردیف هدف را دو بار به‌روز کند
   - D) روش استاندارد SQL برای upsert در همه‌ی موتورهاست

<details><summary>جواب</summary>

**B** — PostgreSQL به یک constraint یا ایندکس unique که با هدف تداخل تطابق دارد نیاز دارد. A نادرست است، C صراحتاً خطاست، و D نادرست است چون `ON CONFLICT` مخصوص PostgreSQL است و شکل استاندارد `MERGE` است.

</details>

۳. یک `MERGE` روی جدول fact با ۲ میلیارد ردیف برای یک batch روزانه‌ی ۵۰٬۰۰۰ ردیفی اجرا می‌شود و کند است. اولین تغییری که باید بررسی کنی چیست؟
   - A) حذف شاخه‌ی `WHEN NOT MATCHED`
   - B) اضافه کردن کلید partition به شرط `ON` تا موتور بتواند هدف را prune کند
   - C) جایگزینی `MERGE` با `INSERT` ساده
   - D) اضافه کردن شاخه‌های `WHEN MATCHED` بیشتر

<details><summary>جواب</summary>

**B** — join ای که کلید partition را هم دارد، فقط partition هایی از هدف را می‌خواند که batch به آن‌ها دست می‌زند. A منطق upsert را می‌شکند، C در تلاش‌های مجدد ردیف تکراری می‌سازد، و D منطق اضافه می‌کند بدون اینکه اسکن را کم کند.

</details>

۴. (سنیور) job جابه‌جایی staging، `dim_customer` را به `_old` تغییر نام می‌دهد و staging را به `dim_customer`. بعد از آن یک view گزارش خطا می‌دهد. چرا، و چطور جلوی آن را بگیری؟
   - A) در PostgreSQL view نمی‌تواند به جدول ارجاع دهد؛ باید view حذف شود
   - B) view به شیء جدول وابسته است و جابه‌جایی معنای نام را عوض کرده؛ قبل از swap وابستگی‌ها را چک کن و view ها را در همان تراکنش بساز یا به‌روز کن
   - C) تغییر نام جدول داده‌اش را پاک می‌کند
   - D) view به `MERGE` نیاز دارد

<details><summary>جواب</summary>

**B** — وابستگی‌هایی مثل view، grant و foreign key به خود شیء گره خورده‌اند، و نام حالا به شیء دیگری اشاره می‌کند. وابستگی‌ها را قبل از swap چک کن و در همان تراکنش بازسازی یا به‌روز کن. A نادرست است، C نادرست است (rename داده را نگه می‌دارد)، و D ربطی ندارد.

</details>

۵. (سنیور، trade-off) برای یک partition روزانه با ۴۰۰ میلیون ردیف، کدام روش در PostgreSQL اجرای مجدد امن را با کمترین bloat بلندمدت می‌دهد؟
   - A) حذف ردیف‌های آن روز و درج، و هرگز `VACUUM` نزدن
   - B) ساخت ردیف‌های آن روز در یک جدول staging، اعتبارسنجی، و سپس الصاق آن به‌عنوان partition (یا حذف و ساخت دوباره‌ی partition) در یک تراکنش
   - C) درج دوباره‌ی ردیف‌ها بدون حذف هیچ چیز
   - D) ساخت جدول جدید با `SELECT *` هر ساعت

<details><summary>جواب</summary>

**B** — ساخت partition در کنار و جابه‌جایی آن، ردیف مرده نمی‌گذارد و اجرای مجدد داده را تمیز جایگزین می‌کند. A bloat می‌سازد، C ردیف تکراری می‌سازد، و D هویت partition و جابه‌جایی را حل نمی‌کند.

</details>

## 🗂️ فلش‌کارت

| رو | پشت |
|---|---|
| upsert چیست؟ | اگر کلید جدید است درج کن، اگر وجود دارد به‌روز کن |
| `EXCLUDED` در `ON CONFLICT` چیست؟ | ردیفی که قرار بود درج شود |
| `MERGE` چه می‌کند؟ | در یک دستور درج، به‌روزرسانی (و در صورت نیاز حذف) ردیف‌ها را بر اساس شرط تطابق منبع و هدف انجام می‌دهد |
| unique constraint چه چیزی به upsert می‌دهد؟ | هدف تداخل: موتور می‌فهمد ردیف از قبل وجود دارد |
| بارگذاری idempotent چیست؟ | اجرای دوباره‌ی همان بارگذاری، جدول نهایی یکسان می‌سازد |
| جابه‌جایی جدول staging چیست؟ | جدول جدید را کنار بساز و اعتبارسنجی کن، بعد نام‌ها را اتمی عوض کن |
| truncate-insert در برابر delete-insert برای هر partition؟ | بازسازی کل partition (بدون ردیف مرده) در برابر حذف بخشی و درج (در PostgreSQL ردیف مرده می‌گذارد) |
| چرا foreign key های انبار اغلب اجرا نمی‌شوند؟ | اجرای آن‌ها هزینه دارد و موتور برای بارگذاری حجیم ساخته شده؛ pipeline باید یکپارچگی را چک کند |

## 🔗 ادامه

- پیش‌نیازها: [[sql-fundamentals]] (مبانی DML) و [[sql-transactions]] (چرا swap باید اتمی باشد).
- مرتبط: [[dedup-idempotent-loads]] و [[pattern-dedup]] (استراتژی کامل یکتاسازی)، [[pattern-backfill]] (اجرای امن دوباره‌ی تاریخچه)، [[scd]] (MERGE حفظ‌کننده‌ی تاریخچه)، [[partitioning-sharding]] (انتخاب partition ها).
- مستندات MERGE و partition را برای موتور و نسخه‌ی خودت بخوان (PostgreSQL «MERGE» و «Table Partitioning»، و مرجع DML انبار داده‌ات).
