---
order: 22
tid: "sql-nulls"
track: "2"
section: null
num: "2.7"
title: "NULLs & Three-Valued Logic"
type: "concept"
level: "beginner"
priority: "P0"
est_hours: 1
summary: "NULL برابر هیچ چیز نیست، حتی خودش. اثرش روی شرط‌ها، تجمیع، join و NOT IN. منبع رایج باگ و سؤال انحرافی در مصاحبه."
covers: ["IS DISTINCT FROM","NULL ordering","NULLIF","COUNT behavior"]
interview_qs: []
extras: []
prerequisites: ["sql-fundamentals", "sql-joins"]
related: ["sql-subqueries-ctes", "sql-window-functions", "sql-datetime", "data-quality"]
---

## 🎯 خلاصه

> در SQL، `NULL` یعنی **«نامشخص»**؛ نه صفر است، نه رشته‌ی خالی. هر مقایسه با `NULL` نتیجه‌اش **UNKNOWN** است و `WHERE` فقط ردیف‌هایی را نگه می‌دارد که شرطشان **TRUE** است.

- `x = NULL` هرگز true نمی‌شود، حتی وقتی `x` هم `NULL` باشد. از `IS NULL` / `IS NOT NULL` استفاده کن، یا برای مقایسه‌ی امن در برابر NULL از `IS DISTINCT FROM`.
- تجمیع‌ها (`COUNT(col)`، `SUM`، `AVG`) مقدار NULL را رد می‌کنند، اما `COUNT(*)` ردیف‌ها را می‌شمارد.
- `NOT IN (subquery)` به محض این‌که زیرکوئری یک `NULL` برگرداند، **هیچ** ردیفی برنمی‌گرداند. به‌جایش `NOT EXISTS` بنویس.
- ابزارهای کمکی: `COALESCE` مقدار NULL را جایگزین می‌کند، و `NULLIF(a, b)` مقدار را به NULL تبدیل می‌کند (مثلاً برای جلوگیری از تقسیم بر صفر).

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** یک پاکت دربسته. کسی می‌پرسد «عدد داخل این پاکت برابر ۵ است؟» جواب درست «نه» نیست، بلکه «نمی‌دانم، بازش نکرده‌ام» است. `NULL` همان پاکت دربسته است. دو پاکت دربسته هم معلوم نیست برابرند، با این‌که هر دو دربسته‌اند.

این یک دیتاست کوچک برای کل درس است:

```sql
CREATE TABLE users (id int, name text, phone text);
INSERT INTO users VALUES
 (1,'Ali','0912'), (2,'Sara',NULL), (3,'Reza','0935'), (4,'Nima',NULL);

CREATE TABLE orders (id int, user_id int, amount int);
INSERT INTO orders VALUES
 (10,1,50), (11,1,NULL), (12,3,70), (13,NULL,20);
```

این دو کوئری را امتحان کن. اولی هیچ ردیفی برنمی‌گرداند و بیشتر تازه‌کارها از این تعجب می‌کنند:

```sql
SELECT name FROM users WHERE phone = NULL;     -- 0 rows
SELECT name FROM users WHERE phone IS NULL;    -- Sara, Nima
```

`phone = NULL` برای همه‌ی ردیف‌ها UNKNOWN است، پس `WHERE` همه را حذف می‌کند. فقط `IS NULL` سؤال درست را می‌پرسد: «آیا این مقدار خالی است؟»

## 🔧 واقعاً چطور کار می‌کند (متوسط)

SQL از **منطق سه‌مقداری** استفاده می‌کند: هر شرط یکی از TRUE، FALSE یا UNKNOWN است. قاعده‌های مهم:

| عبارت | نتیجه |
|---|---|
| `NULL = NULL`، `NULL <> 1`، `NULL > 0` | UNKNOWN |
| `NULL AND FALSE` | FALSE |
| `NULL AND TRUE` | UNKNOWN |
| `NULL OR TRUE` | TRUE |
| `NOT NULL` | UNKNOWN |

`WHERE`، `ON` و `HAVING` فقط ردیفی را نگه می‌دارند که شرطشان TRUE باشد. UNKNOWN در این‌جا مثل FALSE رفتار می‌شود. عبارت `CASE` هم شاخه‌های UNKNOWN را رد می‌کند و به `WHEN` بعدی یا `ELSE` می‌رود.

**تجمیع‌ها ورودی NULL را رد می‌کنند.** این یکی از دلایل رایج محاسبه‌ی میانگین اشتباه است:

```sql
SELECT COUNT(*)      AS all_rows,   -- 4
       COUNT(amount) AS with_amount, -- 3
       AVG(amount)   AS avg_amount,  -- (50 + 70 + 20) / 3 = 46.67, ردیف NULL نادیده گرفته می‌شود
       SUM(amount) * 1.0 / COUNT(*) AS sum_over_rows -- 140 / 4 = 35.0
FROM orders;
```

`orders` چهار ردیف دارد، ولی فقط سه مقدار `amount` غیرNULL. `AVG` بر ۳ تقسیم می‌کند. تقسیم مجموع بر `COUNT(*)` بر ۴ تقسیم می‌کند و عدد متفاوتی می‌دهد. اگر مقدار خالی باید صفر حساب شود، این را صریح بگو و `COALESCE(amount, 0)` بنویس. اجازه نده پیش‌فرض موتور، معنای متریک تو را تعیین کند.

**مقایسه‌ی امن در برابر NULL.** وقتی می‌خواهی «برابر، با این‌که دو NULL را برابر بدانی» (مثلاً برای تشخیص تغییر یا مقایسه‌ی دو نسخه از یک ردیف)، از محمول استاندارد `IS DISTINCT FROM` استفاده کن (برای برابری: `IS NOT DISTINCT FROM`). نحو در موتورها فرق دارد؛ MySQL از `<=>` استفاده می‌کند.

```sql
SELECT id FROM users
WHERE phone IS DISTINCT FROM 'x';   -- برای phone های NULL هم true است
```

**توابع کمکی.**

- `COALESCE(a, b, ...)` اولین آرگومان غیرNULL را برمی‌گرداند: `COALESCE(phone, 'none')`.
- `NULLIF(a, b)` وقتی `a = b` باشد `NULL` برمی‌گرداند، وگرنه `a`. برای جلوگیری از تقسیم بر صفر به کار می‌رود: `amount / NULLIF(divisor, 0)`.

**Join.** شرط برابری هیچ‌وقت NULL را با NULL تطبیق نمی‌دهد، پس سفارش‌هایی که `user_id` آن‌ها NULL است با inner join حذف می‌شوند:

```sql
SELECT u.name, o.id
FROM users u JOIN orders o ON u.id = o.user_id;
-- Ali 10, Ali 11, Reza 12   (سفارش 13 کاربر ندارد و حذف می‌شود)
```

`LEFT JOIN` همه‌ی کاربرها را نگه می‌دارد و برای ستون‌های سفارش مقدار `NULL` می‌گذارد: Sara و Nima با `NULL` در `o.id` ظاهر می‌شوند.

**مرتب‌سازی.** NULL جای طبیعی ندارد، پس موتورها یکی را انتخاب می‌کنند. PostgreSQL در صعودی به‌صورت پیش‌فرض NULLها را آخر می‌گذارد؛ MySQL و SQLite اول. در صورت پشتیبانی، `NULLS FIRST` یا `NULLS LAST` بنویس و در گزارش به پیش‌فرض تکیه نکن.

## 🧠 عمق و trade-offها (سنیور)

- **`NOT IN` تله‌ی کلاسیک است.** اگر زیرکوئری بتواند `NULL` برگرداند، `x NOT IN (...)` برای هر `x`ی که در لیست نیست UNKNOWN می‌شود، پس کوئری صفر ردیف برمی‌گرداند. همین داده با `NOT EXISTS` درست کار می‌کند:

  ```sql
  SELECT u.name FROM users u
  WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);
  -- Sara, Nima
  ```

  الگوی anti-join که با `LEFT JOIN ... IS NULL` نوشته می‌شود همین رفتار را دارد و معمولاً برنامه‌ریزی خوبی دارد.

- **خطای بی‌صدای متریک.** NULL شایع‌ترین دلیل این است که عدد داشبورد بدون هیچ خطایی غلط باشد. `JOIN` روی کلید nullable ردیف‌ها را حذف می‌کند، `COUNT(col)` و `COUNT(*)` با هم فرق دارند، و نسبت از مخرجی حساب می‌شود که انتظارش را نداری. در لایه‌ی کیفیت داده تست‌های صریح `not_null` و `unique` بگذار (به [[data-contract]] و بحث [[idempotency]] برای pipelineهایی که داده را دوباره پردازش می‌کنند نگاه کن).
- **طراحی schema.** وقتی قانون کسب‌وکار می‌گوید مقدار همیشه وجود دارد، ستون را `NOT NULL` کن. ستون‌های nullable کمی فضا می‌گیرند و هر کوئری را مجبور می‌کنند به سه مقدار فکر کند. NULL را برای «نامشخص یا نامربوط» نگه دار و آن را جای «صفر» یا «رشته‌ی خالی» به کار نبر.
- **موتورهای توزیع‌شده همین قواعد را دارند.** در Spark، `col("x") == None` تست NULL نیست؛ از `isNull()` استفاده کن و برای برابری امن در برابر NULL از `eqNullSafe`. فرمت‌های ستونی مثل Parquet مقدارهای null را کنار داده در metadata ثبت می‌کنند، پس ستونی که بیشترش NULL است ارزان ذخیره می‌شود. اما فرمت ذخیره منطق را عوض نمی‌کند: NULL برای موتور query همچنان نامشخص است.
- **trade-off.** مقدار پیش‌فرض با `COALESCE` داده‌ی گم‌شده را پنهان می‌کند. برای نمایش یا وقتی پیش‌فرض کسب‌وکار مستند است، درست است. ولی وقتی NULL یعنی «هنوز اندازه‌گیری نشده»، میانگین معتبر به نظر می‌رسد در حالی که نیست.

## 🧪 امتحان کن

جدول‌های `users` و `orders` ابتدای این درس را در هر موتوری که SQL استاندارد دارد اجرا کن.

1. کاربرانی را که **هیچ** سفارشی ندارند با `NOT EXISTS` پیدا کن. بعد توضیح بده چرا `NOT IN (SELECT user_id FROM orders)` چیزی برنمی‌گرداند.
2. میانگین مبلغ سفارش را یک بار با مبلغ‌های NULL به‌عنوان **صفر** و یک بار با **نادیده گرفتن** آن‌ها حساب کن. هر دو نتیجه را نشان بده.
3. با یک کوئری تعداد کاربرها را بر اساس وضعیت شماره تلفن بشمار: یک شمارش برای `phone IS NULL` و یکی برای `phone IS NOT NULL`، با تجمیع شرطی.

<details><summary>راه‌حل‌ها را نشان بده</summary>

**1.**
```sql
SELECT u.name
FROM users u
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);
-- Sara, Nima
```
`NOT IN` شکست می‌خورد چون زیرکوئری `NULL` برمی‌گرداند (سفارش ۱۳ هیچ `user_id` ندارد). برای هر کاربر، `id NOT IN (1, 1, 3, NULL)` هرگز TRUE نمی‌شود، پس هیچ ردیفی برنمی‌گردد.

**2.**
```sql
SELECT AVG(amount)                   AS ignored_nulls,  -- 46.67
       AVG(COALESCE(amount, 0))      AS nulls_as_zero   -- 35.0
FROM orders;
```

**3.**
```sql
SELECT COUNT(CASE WHEN phone IS NULL THEN 1 END)      AS without_phone,  -- 2
       COUNT(CASE WHEN phone IS NOT NULL THEN 1 END)  AS with_phone      -- 2
FROM users;
```

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «تفاوت `COUNT(*)` و `COUNT(col)` چیست؟»** جواب متوسط: «`COUNT(*)` ردیف‌ها را می‌شمارد، `COUNT(col)` مقدارهای غیرNULL را.» جواب سنیور: «به همین دلیل روی ستون nullable نتیجه‌شان با هم فرق دارد. باید مشخص کنم متریک کدام را می‌خواهد، و وقتی بعد از join عدد مشکوک است، هر دو را تست می‌کنم.»

**سؤال: «چرا `NOT IN` ممکن است هیچ ردیفی برنگرداند؟»** جواب متوسط: «به‌خاطر NULL.» جواب سنیور: «اگر زیرکوئری یک NULL برگرداند، هر مقایسه‌ی خارج از لیست UNKNOWN می‌شود و فیلتر همه‌چیز را حذف می‌کند. من `NOT EXISTS` می‌نویسم که هم امن در برابر NULL است، و nullable بودن ستون زیرکوئری را چک می‌کنم.»

**سؤال: «دو ستونی را که ممکن است NULL باشند چطور مقایسه می‌کنی؟»** جواب متوسط: «`IS NOT DISTINCT FROM` یا `COALESCE`.» جواب سنیور: «برای تشخیص تغییر از مقایسه‌ی امن در برابر NULL استفاده می‌کنم و مطمئن می‌شوم نحو با موتوری که تیم استفاده می‌کند بخواند.»

> **🎤 این را در مصاحبه بگو:** «NULL نامشخص است، پس هر مقایسه‌ی آن نامشخص است و `WHERE` آن را حذف می‌کند. من از `IS NULL`، `IS DISTINCT FROM`، `NOT EXISTS` و `COALESCE` صریح استفاده می‌کنم تا نتیجه به پیش‌فرض‌های پنهان وابسته نباشد.»

## ⚠️ اشتباهات رایج

> **⚠️ توجه:** `WHERE col <> 'x'` ردیف‌هایی را که `col` در آن‌ها NULL است بی‌صدا حذف می‌کند. اگر آن ردیف‌ها باید بمانند، `WHERE col IS DISTINCT FROM 'x'` بنویس.

- نوشتن `= NULL` یا `<> NULL` به‌جای `IS NULL` یا `IS NOT NULL`.
- انتظار این‌که `AVG` مقدار NULL را صفر حساب کند.
- استفاده از `NOT IN` با زیرکوئری‌ای که می‌تواند NULL برگرداند.
- فرض این‌که inner join ردیف‌هایی را که کلیدشان NULL است نگه می‌دارد.
- تکیه به ترتیب پیش‌فرض NULL وقتی کوئری را به موتور دیگری منتقل می‌کنی.
- استفاده از `COALESCE(x, 0)` برای پنهان کردن داده‌ی گم‌شده‌ای که باید گم‌شده بماند.

## ✅ خودت را بسنج

1. کوئری `SELECT * FROM users WHERE phone = NULL;` چه چیزی برمی‌گرداند؟
   - A) همه‌ی ردیف‌ها
   - B) فقط ردیف‌هایی که `phone` در آن‌ها NULL است
   - C) هیچ ردیفی
   - D) خطا در هر موتوری

<details><summary>جواب</summary>

**C** — `phone = NULL` برای همه‌ی ردیف‌ها UNKNOWN است و `WHERE` فقط TRUE را نگه می‌دارد. از `phone IS NULL` استفاده کن.

</details>

2. در جدول `orders`، ستون `amount` مقادیر 50، NULL، 70 و 20 دارد. `AVG(amount)` چه برمی‌گرداند؟
   - A) 35
   - B) 46.67
   - C) 140
   - D) NULL

<details><summary>جواب</summary>

**B** — `AVG` مقدار NULL را نادیده می‌گیرد و بر ۳ تقسیم می‌کند: (50 + 70 + 20) / 3 = 46.67. تقسیم بر `COUNT(*)` (یعنی ۴) عدد ۳۵ می‌دهد که متریک دیگری است.

</details>

3. کدام کوئری درست کاربرانی را برمی‌گرداند که سفارش ندارند، حتی وقتی `orders.user_id` شامل NULL است؟
   - A) `SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders)`
   - B) `SELECT name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)`
   - C) `SELECT name FROM users WHERE id = NULL`
   - D) `SELECT name FROM users WHERE id <> ANY (SELECT user_id FROM orders)`

<details><summary>جواب</summary>

**B** — `NOT EXISTS` تحت تأثیر NULL در زیرکوئری قرار نمی‌گیرد. گزینه‌ی A وقتی حتی یک `user_id` معلوم NULL باشد هیچ ردیفی برنمی‌گرداند، و گزینه‌ی D مفهوم «سفارش نداشتن» را بیان نمی‌کند.

</details>

4. کدام عبارت وقتی `divisor` برابر ۰ است، به‌جای خطا مقدار `NULL` می‌دهد؟
   - A) `amount / COALESCE(divisor, 0)`
   - B) `amount / NULLIF(divisor, 0)`
   - C) `amount / IFNULL(divisor, 0)`
   - D) `amount / (divisor + 0)`

<details><summary>جواب</summary>

**B** — `NULLIF(divisor, 0)` وقتی مقسوم‌علیه صفر است NULL برمی‌گرداند و تقسیم بر NULL نتیجه‌ی NULL می‌دهد. گزینه‌های A و C فقط NULL را جایگزین می‌کنند و صفر را نگه می‌دارند، پس باز هم بر صفر تقسیم می‌کنند. D هم بر صفر تقسیم می‌کند.

</details>

5. **(سنیور)** یک job شبانه `events` را با `users` روی `users.account_id` join می‌کند که nullable است. بعد از تغییر schema که این ستون را برای بعضی کاربران nullable کرد، تعداد ردیف‌ها کم شد. علت احتمالی چیست و چه چیزی را تغییر می‌دهی؟

   - A) join به cross join تبدیل شده
   - B) ردیف‌هایی با `account_id` برابر NULL در شرط برابری تطبیق نمی‌شوند؛ اگر باید نگه داشته شوند از `LEFT JOIN` استفاده کن و یک تست `not_null` یا relationship بگذار تا تغییر را بگیرد
   - C) موتور NULL را به ۰ تبدیل کرده و تکرار ایجاد شده
   - D) راه‌حل این است که `DISTINCT` اضافه کنی

<details><summary>جواب</summary>

**B** — شرط برابری هرگز NULL را تطبیق نمی‌دهد، پس آن رویدادها در inner join حذف می‌شوند. راه‌حل درست به قانون کسب‌وکار بستگی دارد: با `LEFT JOIN` نگهشان دار، یا ستون را از منبع `NOT NULL` کن و تست بگذار.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| `NULL` در SQL یعنی چه؟ | نامشخص یا خالی؛ نه صفر و نه رشته‌ی خالی |
| نتیجه‌ی `NULL = NULL` | UNKNOWN (نه TRUE)، پس `WHERE` ردیف را حذف می‌کند |
| مقایسه‌ی امن در برابر NULL | `IS NOT DISTINCT FROM` (برابر) یا `IS DISTINCT FROM` (متفاوت)؛ MySQL از `<=>` استفاده می‌کند |
| `COUNT(*)` در برابر `COUNT(col)` | ردیف‌ها در برابر مقدارهای غیرNULL ستون `col` |
| چرا `NOT IN` هیچ‌چیز برنمی‌گرداند؟ | یک NULL در زیرکوئری هر مقایسه‌ی غیرتطبیقی را UNKNOWN می‌کند |
| `NULLIF(a, b)` | وقتی `a = b` باشد NULL، وگرنه `a`؛ برای جلوگیری از تقسیم بر صفر |
| `COALESCE(a, b, ...)` | اولین آرگومان غیرNULL |

## 🔗 ادامه

- درس بعدی: **تاریخ، زمان و منطقه‌ی زمانی در SQL** (`sql-datetime`) همین تفکر «مقدار گم‌شده» را برای timestamp بررسی می‌کند.
- قواعد join را در `sql-joins` و شکل‌های زیرکوئری را در `sql-subqueries-ctes` مرور کن.
- تمرین الگوهایی که مدیریت NULL در آن‌ها مهم است: `sql-interview-patterns`.
