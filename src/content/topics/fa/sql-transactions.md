---
order: 27
tid: "sql-transactions"
track: "2"
section: null
num: "2.12"
title: "Transactions, Isolation & Locking"
type: "concept"
level: "advanced"
priority: "P1"
est_hours: 3
summary: "ACID، سطوح ایزولیشن (Read Uncommitted تا Serializable)، ناهنجاری‌ها (dirty read، non-repeatable read، phantom، write skew)، MVCC و قفل‌گذاری."
covers: ["SELECT FOR UPDATE","deadlocks","optimistic vs pessimistic concurrency"]
interview_qs: []
extras: []
prerequisites: ["sql-fundamentals", "sql-indexing"]
related: ["wal-mvcc", "distributed-transactions", "consistency-models", "kafka-exactly-once", "sql-ddl-merge"]
---
## 🎯 خلاصه

> **تراکنش (transaction)** چند دستور را در یک واحد **همه‌یا‌هیچ** گروه می‌کند. **ایزولیشن (isolation)** تعیین می‌کند یک تراکنش از کارهای هم‌زمان دیگر چه چیزی ببیند. ایزولیشن ضعیف‌تر سریع‌تر است اما ناهنجاری‌ها را رد می‌کند؛ ایزولیشن قوی‌تر امن‌تر است ولی به قفل، retry یا هر دو نیاز دارد.

- **ACID**: Atomicity (همه‌یا‌هیچ)، Consistency (قیدها برقرار می‌مانند)، Isolation (کار هم‌زمان بیشتر از سطح انتخابی تداخل نمی‌کند)، Durability (commit شده یعنی باقی می‌ماند).
- سطوح ایزولیشن استاندارد SQL عبارت‌اند از Read Uncommitted، Read Committed، Repeatable Read و Serializable. موتورها در این‌که هرکدام دقیقاً چه چیزی را جلو می‌گیرند فرق دارند.
- ناهنجاری‌ها: **dirty read**، **non-repeatable read**، **phantom**، **lost update** و **write skew**. بیشتر آدم‌ها از write skew شگفت‌زده می‌شوند.
- محافظت از طریق **قفل** (`SELECT ... FOR UPDATE`)، **snapshot های MVCC**، یا **بررسی serializable** که تراکنش را abort می‌کند و تکرار می‌شود، انجام می‌شود.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** انتقال بانکی. پول از حساب A خارج و به حساب B وارد می‌شود. اگر برق بین این دو مرحله قطع شود، پول نباید گم شود. تراکنش یعنی هر دو مرحله انجام شود، یا هیچ‌کدام.

مثال‌های زیر PostgreSQL هستند.

```sql
CREATE TABLE accounts (id int PRIMARY KEY, owner text, balance numeric NOT NULL CHECK (balance >= 0));
INSERT INTO accounts VALUES (1, 'Ali', 500), (2, 'Sara', 100);

BEGIN;
UPDATE accounts SET balance = balance - 200 WHERE id = 1;
UPDATE accounts SET balance = balance + 200 WHERE id = 2;
COMMIT;
```

قبل از `COMMIT` هیچ session دیگری تغییر را نمی‌بیند. اگر قبل از `COMMIT` چیزی خطا بدهد (یا `ROLLBACK` بزنی)، هر دو UPDATE برگردانده می‌شوند. این **atomicity** است. وقتی `COMMIT` برگردد، نتیجه با کرش هم از بین نمی‌رود. این **durability** است.

قید `CHECK` بخشی از **consistency** است: به‌روزرسانی‌ای که موجودی را منفی کند شکست می‌خورد و تراکنش همراه آن.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

**ایزولیشن درباره‌ی هم‌روندی است.** دو session هم‌زمان اجرا می‌شوند. سطح ایزولیشن تعیین می‌کند کدام ترتیب‌های درهم‌تنیده مجازند. استاندارد SQL سطوح را با ناهنجاری‌هایی تعریف می‌کند که باید جلوگیری کنند:

| سطح | Dirty read | Non-repeatable read | Phantom | Write skew |
|---|---|---|---|---|
| Read Uncommitted | ممکن | ممکن | ممکن | ممکن |
| Read Committed | جلوگیری می‌شود | ممکن | ممکن | ممکن |
| Repeatable Read | جلوگیری می‌شود | جلوگیری می‌شود | در استاندارد ممکن (در PostgreSQL جلوگیری می‌شود) | ممکن |
| Serializable | جلوگیری می‌شود | جلوگیری می‌شود | جلوگیری می‌شود | جلوگیری می‌شود |

واقعیت موتور مهم‌تر از جدول است. در PostgreSQL، `READ UNCOMMITTED` مثل `READ COMMITTED` رفتار می‌کند، پس dirty read هرگز رخ نمی‌دهد. `REPEATABLE READ` در آن snapshot isolation است و `SERIALIZABLE` بررسی واقعی serializable را انجام می‌دهد. قبل از استدلال درباره‌ی یک باگ، مستندات موتور خودت را برای هر سطح بخوان.

**ناهنجاری‌ها به شکل مشخص.** هرکدام به دو session نیاز دارد.

- **Non-repeatable read** (در Read Committed): session A موجودی علی (500) را می‌خواند. session B مقدار `balance = 300` را commit می‌کند. A دوباره می‌خواند و 300 می‌بیند. دو خواندن داخل یک تراکنش با هم نمی‌خوانند.
- **Phantom**: A اجرا می‌کند `SELECT count(*) FROM accounts WHERE balance > 200`. B یک ردیف مطابق اضافه و commit می‌کند. A همان شمارش را می‌زند و عدد دیگری می‌گیرد.
- **Lost update** (چرخه‌ی خواندن-تغییر-نوشتن در اپلیکیشن):

```text
A: SELECT balance -> 500          B: SELECT balance -> 500
A: write 500 - 200 = 300          B: write 500 - 50 = 450
A: COMMIT                         B: COMMIT
-> final balance 450; the 200 withdrawal is lost
```

راه‌حل این است که محاسبه را پایگاه‌داده انجام دهد، `UPDATE accounts SET balance = balance - 50 WHERE id = 1`، یا ابتدا ردیف را قفل کنی.

**Write skew** ناهنجاری‌ای است که به ردیف یکسانی مربوط نیست. قاعده می‌گوید «حداقل یک پزشک باید آنکال بماند». جدول `oncall(doctor, on_call)` علی و بابک را آنکال دارد.

```sql
CREATE TABLE oncall (doctor text PRIMARY KEY, on_call boolean);
INSERT INTO oncall VALUES ('Alice', true), ('Bob', true);
```

هر session همان منطق را اجرا می‌کند:

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM oncall WHERE on_call;   -- 2 می‌بیند، پس رفتن امن است
UPDATE oncall SET on_call = false WHERE doctor = 'Alice';   -- Bob همین کار را با 'Bob' می‌کند
COMMIT;
```

در snapshot isolation هر دو تراکنش عدد 2 را می‌بینند، **ردیف‌های متفاوتی** را به‌روز می‌کنند، پس تداخل قفل رخ نمی‌دهد و هر دو commit می‌شوند. حالا هیچ‌کس آنکال نیست. در `SERIALIZABLE`، PostgreSQL می‌فهمد نتیجه با هیچ ترتیب سریالی معادل نیست و یکی از تراکنش‌ها را با SQLSTATE برابر `40001` (serialization failure) abort می‌کند. اپلیکیشن باید آن را دوباره اجرا کند.

**قفل‌ها.** کنترل بدبینانه (pessimistic) قبل از عمل، ردیف را قفل می‌کند:

```sql
BEGIN;
SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;   -- قفل ردیف تا COMMIT
UPDATE accounts SET balance = balance - 50 WHERE id = 1;
COMMIT;
```

session های دیگری که بخواهند همان ردیف را `UPDATE` یا `FOR UPDATE` کنند منتظر می‌مانند. `FOR SHARE` شکل ضعیف‌تری برای خواننده‌هاست. `SKIP LOCKED` باعث می‌شود یک worker صف، به‌جای انتظار، ردیف آزاد بعدی را بردارد که برای جدول job ها مفید است.

**Deadlock.** دو تراکنش هرکدام قفلی را دارند که دیگری لازم دارد:

```text
T1: UPDATE accounts WHERE id = 1   (قفل ردیف 1)
T2: UPDATE accounts WHERE id = 2   (قفل ردیف 2)
T1: UPDATE accounts WHERE id = 2   (منتظر T2)
T2: UPDATE accounts WHERE id = 1   (منتظر T1)  -> deadlock
```

PostgreSQL این چرخه را تشخیص می‌دهد و یکی از تراکنش‌ها را abort می‌کند (SQLSTATE برابر `40P01`). روش پیشگیری استاندارد این است که **ردیف‌ها همیشه با یک ترتیب ثابت قفل شوند**، مثلاً `id` صعودی.

**MVCC.** در PostgreSQL خواننده‌ها نویسنده‌ها را بلاک نمی‌کنند و نویسنده‌ها خواننده‌ها را، چون هر نسخه‌ی ردیف با دید تراکنشش نگه داشته می‌شود. خواننده یک snapshot می‌بیند. هزینه‌اش نسخه‌های قدیمی ردیف است که `VACUUM` باید پاک کند. ببین [[mvcc]].

**کنترل خوش‌بینانه (optimistic)** از نگه‌داشتن قفل پرهیز می‌کند. هر ردیف یک شماره‌ی نسخه دارد و به‌روزرسانی فقط وقتی موفق است که نسخه تغییر نکرده باشد:

```sql
ALTER TABLE accounts ADD COLUMN version int NOT NULL DEFAULT 0;

UPDATE accounts
SET balance = balance - 50, version = version + 1
WHERE id = 1 AND version = 7;
-- اپلیکیشن تعداد ردیف‌ها را چک می‌کند: 0 یعنی کس دیگری تغییر داده؛ دوباره بخوان و retry کن
```

## 🧠 عمق و trade-offها (سنیور)

- **سطح ایزولیشن را بر اساس invariant انتخاب کن، نه عادت.** بیشتر اپلیکیشن‌ها روی Read Committed درست کار می‌کنند، تا وقتی قاعده‌ای چند ردیف را دربرگیرد (write skew) یا شمارشی را بخواند و بر اساس آن عمل کند. برای آن‌ها از `SERIALIZABLE` با حلقه‌ی retry استفاده کن، یا قاعده را با یک قید واقعی اعمال کن (کلید یکتا، exclusion constraint یا trigger).
- **Serializable هزینه‌ی retry دارد.** زیر رقابت، SSI تراکنش‌ها را abort می‌کند. اپلیکیشن به منطق retry idempotent و سقف تعداد تلاش نیاز دارد. بدون آن، یک «رفع» تحت بار به قطعی سرویس تبدیل می‌شود.
- **تراکنش‌های طولانی گران‌اند.** تراکنشی که snapshot نگه می‌دارد مانع پاک شدن نسخه‌های قدیمی توسط `VACUUM` می‌شود و قفل‌های نگه‌داشته‌شده بقیه‌ی نویسنده‌ها را می‌بندند. تراکنش‌ها را کوتاه نگه دار، داخلشان سرویس خارجی صدا نزن، و برای pool های اپلیکیشن `idle_in_transaction_session_timeout` تنظیم کن.
- **دانه‌بندی قفل و escalation.** قفل ردیفی تک‌تک ارزان است، اما یک `UPDATE` بزرگ با میلیون‌ها ردیف همه را تا commit نگه می‌دارد. تغییرات بزرگ را دسته‌ای کن (برای مثال 10 هزار ردیف در هر تراکنش) و بین دسته‌ها commit کن.
- **انبارها و lakeها متفاوت کار می‌کنند.** Snowflake و BigQuery تراکنش چنددستوری با قواعد خودشان دارند. Delta Lake و Apache Iceberg تغییر جدول را با کنترل خوش‌بینانه‌ی اتمیک commit می‌کنند: دو نویسنده که فایل‌های یکسان را لمس کنند در زمان commit تداخل دارند و یکی دوباره تلاش می‌کند. موتورهای batch به‌ندرت قفل ردیف می‌دهند. مستندات هر موتور را بررسی کن؛ بسیاری `SELECT FOR UPDATE` ندارند.
- **ClickHouse موتور تراکنش OLTP نیست.** مدل آن append-محور است با merge های پس‌زمینه. invariantهای اپلیکیشنی که به تراکنش چندردیفی نیاز دارند را آنجا طراحی نکن. منبع حقیقت را در PostgreSQL یا یک پایگاه OLTP مشابه نگه دار و تغییرات را با [[cdc]] بیرون بفرست.
- **اتمیک بودن بین سیستم‌ها** (نوشتن در پایگاه‌داده به‌علاوه‌ی پیام Kafka) یک تراکنش واحد نیست. از transactional outbox یا تراکنش‌های Kafka استفاده کن؛ ببین [[distributed-transactions]] و [[kafka-exactly-once]].
- **کجا serializable همه‌جا نباشد.** برای جابه‌جایی پول و موجودی انبار پیش‌فرض درستی است، و برای تحلیل یا لاگ پیش‌فرض بدی، چون طوفان retry ارزشی اضافه نمی‌کند.

## 🧪 امتحان کن

از جدول `accounts` ابتدای درس استفاده کن (PostgreSQL نسخه‌ی 13 یا بالاتر). برای تمرین‌های هم‌زمانی دو session در `psql` باز کن.

1. یک **lost update** را در دو session نشان بده، سپس با `SELECT ... FOR UPDATE` رفعش کن. ترتیب رخ‌دادها را شرح بده.
2. **write skew** را با `oncall` زیر `REPEATABLE READ` بازتولید کن، و بعد همان مراحل را زیر `SERIALIZABLE` اجرا کن. بگو کدام session شکست می‌خورد و با کدام SQLSTATE.
3. یک کوئری worker بنویس که بدون بلاک کردن worker های دیگر، یک job در انتظار را از جدول `jobs(id, status)` بردارد.

<details><summary>جواب‌ها را نشان بده</summary>

**1.** session A و B هر دو اجرا می‌کنند `SELECT balance FROM accounts WHERE id = 1;` و بعد `UPDATE accounts SET balance = <مقدار خوانده‌شده> - 50 WHERE id = 1;`. هر دو می‌توانند با یک موجودی اشتباه تمام شوند: یکی از دو برداشت ناپدید می‌شود. رفع:
```sql
BEGIN;
SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;
UPDATE accounts SET balance = balance - 50 WHERE id = 1;
COMMIT;
```
session دوم روی `SELECT ... FOR UPDATE` منتظر می‌ماند تا اولی commit کند، بعد موجودی جدید را می‌خواند. یا کل خواندن را حذف کن و با `UPDATE accounts SET balance = balance - 50 WHERE id = 1` قفل ردیف را خودِ UPDATE بگیرد.

**2.** زیر `REPEATABLE READ` هر دو `COMMIT` موفق می‌شوند و `SELECT count(*) FROM oncall WHERE on_call` بعداً 0 برمی‌گرداند. زیر `SERIALIZABLE` ، `COMMIT` دوم (یا یکی از دستورها) با SQLSTATE برابر `40001` شکست می‌خورد و اپلیکیشن دوباره اجرا می‌کند؛ retry یک پزشک آنکال می‌بیند و ادامه نمی‌دهد. لحظه‌ی دقیق خطا ممکن است فرق کند، پس نتیجه را بررسی کن، نه خط را.

**3.**
```sql
UPDATE jobs
SET status = 'running'
WHERE id = (
  SELECT id FROM jobs
  WHERE status = 'pending'
  ORDER BY id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING id;
```
`SKIP LOCKED` ردیف‌هایی را که worker دیگری قفل کرده رد می‌کند، پس worker ها پشت سر هم صف نمی‌کشند. کل دستور در حالت autocommit یک تراکنش است.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «تفاوت سطوح ایزولیشن چیست؟»** پاسخ متوسط: «آن‌ها تعیین می‌کنند کدام ناهنجاری‌ها مجازند؛ از dirty read در پایین‌ترین سطح تا هیچ‌کدام در Serializable.» پاسخ سنیور: «جدول کتاب درسی حداقل است، نه کل واقعیت. در PostgreSQL، Repeatable Read یک snapshot isolation است و هنوز write skew را مجاز می‌داند، پس برای invariantهای چندردیفی یا از Serializable با retry استفاده می‌کنم، یا قاعده را با یک قید اجرا می‌کنم.»

**سؤال: «چطور lost update را جلو می‌گیری؟»** پاسخ متوسط: «`SELECT ... FOR UPDATE` استفاده می‌کنم، یا محاسبه را در خود `UPDATE` انجام می‌دهم.» پاسخ سنیور: «بسته به میزان رقابت، یا قفل بدبینانه یا نسخه‌گذاری خوش‌بینانه با retry. خوش‌بینانه وقتی تداخل نادر است بهتر است و بدبینانه وقتی تداخل زیاد و retry گران است.»

**سؤال: «چرا تراکنش من deadlock می‌شود؟»** پاسخ مورد انتظار: دو تراکنش ردیف‌های یکسان را با ترتیب متفاوت قفل می‌کنند. رفع: ترتیب قفل ثابت و تراکنش‌های کوتاه.

> **🎤 این را در مصاحبه بگو:** «ایزولیشن را فهرست ناهنجاری‌هایی می‌دانم که باید حذف شوند. برای بیشتر کوئری‌ها Read Committed کافی است. وقتی invariant چند ردیف را دربرمی‌گیرد، مثل write skew، از Serializable با retry استفاده می‌کنم یا قاعده را با قید اجرا می‌کنم، چون سطح پیش‌فرض از آن محافظت نمی‌کند.»

## ⚠️ اشتباهات رایج

> **⚠️ توجه:** خواندن یک مقدار در اپلیکیشن، محاسبه‌ی چیزی و نوشتن دوباره‌ی آن یک race condition است. به‌روزرسانی را در SQL انجام بده، یا ابتدا ردیف را قفل کن.

> **⚠️ توجه:** فرض کردن اینکه `REPEATABLE READ` همه‌ی ناهنجاری‌ها را جلو می‌گیرد. write skew زیر snapshot isolation هم رخ می‌دهد.

- یکسان فرض کردن نام سطوح ایزولیشن بین موتورها. MySQL InnoDB، Oracle و PostgreSQL آن‌ها را متفاوت پیاده‌سازی می‌کنند.
- صدا زدن سرویس خارجی داخل تراکنش که قفل‌ها را چند ثانیه نگه می‌دارد.
- قفل کردن ردیف‌ها با ترتیب‌های متفاوت در مسیرهای مختلف کد، که deadlock می‌سازد.
- نادیده گرفتن SQLSTATE های `40001` و `40P01` در اپلیکیشن. این خطاها انتظارشده‌اند و باید retry شوند.
- استفاده از `SERIALIZABLE` بدون retry، که abort های گاه‌به‌گاه را به درخواست‌های شکست‌خورده تبدیل می‌کند.
- اجرای `UPDATE` چند میلیون ردیفی در یک تراکنش، که قفل‌ها و نسخه‌های قدیمی را ساعت‌ها نگه می‌دارد.

## ✅ خودت را بسنج

1. کدام ویژگی تضمین می‌کند که کرش بعد از `COMMIT` انتقال را از بین نمی‌برد؟
   - A) Atomicity
   - B) Consistency
   - C) Isolation
   - D) Durability

<details><summary>جواب</summary>

**D** — durability یعنی تراکنش commit شده از خرابی‌ها جان سالم به در می‌برد. Atomicity درباره‌ی همه‌یا‌هیچ بودن داخل خود تراکنش است.

</details>

2. دو تراکنش هر دو بررسی می‌کنند که حداقل یک پزشک آنکال است، و سپس هرکدام پزشک متفاوتی را حذف می‌کند. زیر snapshot isolation هر دو commit می‌شوند. این چه ناهنجاری‌ای است؟
   - A) Dirty read
   - B) Non-repeatable read
   - C) Phantom
   - D) Write skew

<details><summary>جواب</summary>

**D** — write skew: تراکنش‌ها داده‌ی هم‌پوشان می‌خوانند، ردیف‌های متفاوت می‌نویسند، و با هم یک قاعده را می‌شکنند. قفل ردیف تداخل را نمی‌بیند، پس فقط بررسی serializable یا یک قید آن را می‌گیرد.

</details>

3. در PostgreSQL، کدام گزاره درباره‌ی `READ UNCOMMITTED` درست است؟
   - A) اجازه‌ی dirty read می‌دهد
   - B) مثل `READ COMMITTED` رفتار می‌کند، پس dirty read رخ نمی‌دهد
   - C) همان `SERIALIZABLE` است
   - D) سرور آن را نمی‌پذیرد

<details><summary>جواب</summary>

**B** — PostgreSQL این سطح را می‌پذیرد اما هرگز داده‌ی commit نشده برنمی‌گرداند. موتورهای دیگر ممکن است فرق کنند، پس مستندات را بررسی کن.

</details>

4. **(سنیور)** یک سرویس `SELECT count(*) FROM shifts WHERE day = ?` را می‌خواند و اگر شمارش کمتر از سقف باشد، یک شیفت جدید درج می‌کند. زیر بار زیاد سقف رد می‌شود. کدام راه‌حل مقاوم‌تر است؟

   - A) اضافه کردن `ORDER BY` به شمارش
   - B) یک قید پایگاه‌داده یا قفلی که قاعده را اعمال کند (مثلاً `FOR UPDATE` روی یک ردیف روزانه، یا کلید یکتا روی اسلات)، و retry سریالیزبل به‌عنوان پشتیبان
   - C) انتقال شمارش به کش اپلیکیشن
   - D) استفاده از `READ UNCOMMITTED` برای سریع‌تر شدن

<details><summary>جواب</summary>

**B** — قاعده چند ردیف را دربرمی‌گیرد، که الگوی write skew است. قفل روی یک ردیف مشترک (مثل خود روز) یا یک اسلات یکتای اعمال‌شده توسط پایگاه‌داده باعث می‌شود بررسی و درج با هم تداخل کنند. کش اپلیکیشن یا سطح ایزولیشن پایین‌تر رقابت را محتمل‌تر می‌کند.

</details>

5. **(سنیور)** روی یک endpoint پرترافیک با `SERIALIZABLE`، بعد از تغییر نرخ خطا بالا می‌رود. طراحی باید چه چیزی داشته باشد و از چه چیزی پرهیز کنی؟

   - A) بررسی serializable را برای آن endpoint خاموش کن
   - B) تراکنش‌ها را کوتاه نگه دار، روی SQLSTATE برابر `40001` با retry محدود و idempotent دوباره اجرا کن، و فراخوانی خارجی داخل تراکنش نگذار
   - C) همه‌ی دستورهای `UPDATE` را حذف کن
   - D) مقدار `deadlock_timeout` را صفر کن

<details><summary>جواب</summary>

**B** — زیر رقابت، abort های serializable طبیعی‌اند، پس اپلیکیشن باید با سقف، retry امن داشته باشد. تراکنش‌های کوتاه‌تر تداخل و زمان قفل را کم می‌کنند، پس نرخ retry پایین می‌آید.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| ACID | Atomicity، Consistency، Isolation، Durability |
| Dirty read | خواندن تغییر commit نشده‌ی تراکنش دیگر |
| Non-repeatable read | یک ردیف داخل یک تراکنش مقادیر متفاوت برمی‌گرداند |
| Phantom | یک کوئری داخل یک تراکنش ردیف‌های متفاوت برمی‌گرداند |
| Write skew | تراکنش‌ها داده‌ی هم‌پوشان می‌خوانند، ردیف‌های متفاوت می‌نویسند و با هم قاعده را می‌شکنند |
| `SELECT ... FOR UPDATE` | ردیف‌های انتخاب‌شده را تا commit قفل می‌کند و نویسنده‌های دیگر منتظر می‌مانند |
| `SKIP LOCKED` | ردیف‌هایی را که session دیگری قفل کرده رد می‌کند؛ برای صف job مفید است |
| SQLSTATE 40001 / 40P01 | خطای serialization / تشخیص deadlock؛ هر دو باید retry شوند |

## 🔗 ادامه

- [[acid]] و [[isolation-level]] — تعریف‌های واژه‌نامه‌ای پشت این درس.
- [[mvcc]] و [[wal-mvcc]] — پیاده‌سازی snapshot و durability.
- [[distributed-transactions]] — وقتی تراکنش بیش از یک سیستم را دربرمی‌گیرد.
- [[kafka-exactly-once]] — تراکنش بین یک پایگاه‌داده و یک stream.
- [[consistency-models]] — نگاه سیستم‌های توزیع‌شده به همین ایده‌ها.

