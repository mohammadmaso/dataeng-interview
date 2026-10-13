---
order: 24
tid: "sql-interview-patterns"
track: "2"
section: null
num: "2.9"
title: "Classic SQL Interview Patterns"
type: "practice"
level: "advanced"
priority: "P0"
est_hours: 15
summary: "الگوهایی که بارها در مصاحبه تکرار می‌شوند. هر الگو یک راه‌حل استاندارد دارد که باید از حفظ باشی."
covers: []
interview_qs: []
extras: [{"label":"patterns","items":["Top-N per group — ROW_NUMBER + filter","Gaps & Islands — consecutive days / streaks (ROW_NUMBER difference trick)","Sessionization — LAG + new-session flag + cumulative SUM","Running totals & moving averages","Retention & cohort analysis — first-activity cohort, day-N retention","Funnel analysis — step conversion with ordered events","Deduplication — keep latest record per key","Median & percentiles — PERCENTILE_CONT, manual median","Period-over-period — MoM / YoY growth with LAG","Point-in-time lookup — join to SCD2 with valid_from/valid_to","Anti-join — \"never did X\"","Pairs & combinations — self-join with `a.id < b.id`","Histogram / bucketing — WIDTH_BUCKET / FLOOR","Overlapping intervals — concurrent sessions, max concurrency","Market basket — products bought together","Fill forward — last non-null value","Pivot reports — conditional aggregation"]},{"label":"practice","items":["60+ problems (DataLemur, LeetCode SQL 50 + Hard, StrataScratch) tagged by pattern"]}]
prerequisites: ["sql-window-functions", "sql-nulls", "sql-datetime", "sql-subqueries-ctes"]
related: ["sql-joins", "sql-advanced-grouping", "sql-query-optimization", "dimensional-modeling"]
---

## 🎯 خلاصه

> بیشتر سؤال‌های SQL در مصاحبه یکی از حدود ۱۷ الگو هستند که با لباس دیگری آمده‌اند. یاد بگیر الگو را از **صورت سؤال تشخیص بدهی**، بعد اسکلت استاندارد آن را به کار ببری. به ایده‌ی جدید کمتر نیاز داری؛ بیشتر به اسکلت درست نیاز داری.

- سرنخ‌های صورت سؤال: «N تای برتر در هر»، «پشت‌سرهم»، «streak»، «session»، «تجمعی»، «اولین بار»، «هرگز»، «آخرین»، «بین دو رویداد».
- بیشتر اسکلت‌ها از یکی از سه ابزار استفاده می‌کنند: **window function** (`ROW_NUMBER`، `LAG`، `SUM` تجمعی)، **date spine** یا تقویم، یا **تجمیع شرطی** (`COUNT(CASE WHEN ...)`).
- قبل از نوشتن کد، **grain** خروجی و قاعده‌ی تساوی (tie) را بگو.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** آشپزی که سس‌های پایه را بلد است. دستور غذای جدید معمولاً «یک roux درست کن، بعد استاک اضافه کن» است. SQL مصاحبه هم همین‌طور است: «N تای برتر در هر گروه» سسی است که در یک مرحله درست می‌شود و با فیلترها ترکیب می‌شود.

برای تمرین بیشتر الگوهای این درس دو جدول کافی است:

```sql
CREATE TABLE employees (id int, name text, dept text, salary int);
INSERT INTO employees VALUES
 (1,'Ali','data',9000), (2,'Sara','data',12000), (3,'Reza','data',12000),
 (4,'Nima','data',10000), (5,'Maryam','web',8000), (6,'Kian','web',8000),
 (7,'Parisa','web',7000);

CREATE TABLE logins (user_id int, login_date date);
INSERT INTO logins VALUES
 (1,'2026-01-01'), (1,'2026-01-02'), (1,'2026-01-03'), (1,'2026-01-05'),
 (1,'2026-01-06'), (1,'2026-01-06'), (2,'2026-01-02'), (2,'2026-01-04');
```

اولین الگو **top N در هر گروه** است: «دو حقوق بالا در هر بخش».

```sql
SELECT dept, name, salary
FROM (
  SELECT dept, name, salary,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, id) AS rn
  FROM employees
) t
WHERE rn <= 2
ORDER BY dept, rn;
-- data: Sara, Reza; web: Maryam, Kian
```

window ردیف‌ها را داخل هر بخش شماره می‌زند و فیلتر بیرونی دو تای اول را نگه می‌دارد. `id` در `ORDER BY` باعث می‌شود ترتیب تساوی‌ها قطعی باشد.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

الگوهای زیر پرتکرارترین‌ها هستند. هر کدام یک اسکلت است که می‌توانی تطبیقش بدهی.

| الگو | سرنخ در سؤال | اسکلت |
|---|---|---|
| Top-N در هر گروه | «۳ تای برتر در هر دسته» | `ROW_NUMBER() OVER (PARTITION BY g ORDER BY x DESC)` و بعد فیلتر |
| Gaps & islands | «روزهای پشت‌سرهم»، «streak» | `value - ROW_NUMBER()` داخل هر بازه ثابت است |
| Sessionization | «sessionهای با وقفه‌ی ۳۰ دقیقه» | `LAG` ← پرچم session جدید ← `SUM` تجمعی |
| Deduplication | «آخرین رکورد هر کلید» | `ROW_NUMBER` با `ORDER BY updated_at DESC, id DESC` |
| Funnel | «دیده، سپس سبد، سپس خرید» | یک ردیف برای هر کاربر با `MIN(CASE ...)` برای هر مرحله، بعد مقایسه |
| Anti-join | «هرگز X نکرده» | `NOT EXISTS` (نه `NOT IN`، ببین [[sql-nulls]]) |
| Fill forward | «آخرین مقدار شناخته‌شده» | `MAX(...) OVER (... ROWS UNBOUNDED PRECEDING)` روی شمارنده‌ی گروه |
| مقایسه‌ی دوره‌ای | «رشد ماه به ماه» | `LAG` روی تجمیع ماهانه |
| Pivot | «یک ستون برای هر وضعیت» | `SUM(CASE WHEN status = 'x' THEN 1 ELSE 0 END)` |

**Gaps & islands.** برای یک کاربر، `login_date - ROW_NUMBER()` تا وقتی تاریخ‌ها پشت‌سرهم‌اند ثابت می‌ماند، چون هر دو در هر ردیف یکی زیاد می‌شوند. مقدار یکسان یعنی یک streak.

```sql
WITH d AS (SELECT DISTINCT user_id, login_date FROM logins),
g AS (
  SELECT user_id, login_date,
         login_date - (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date))::int AS grp
  FROM d
)
SELECT user_id, MIN(login_date) AS start_date, MAX(login_date) AS end_date, COUNT(*) AS days
FROM g
GROUP BY user_id, grp
ORDER BY user_id, start_date;
-- user 1: 01-01 تا 01-03 (۳ روز)، 01-05 تا 01-06 (۲ روز)؛ user 2: دو streak یک‌روزه
```

`DISTINCT` لازم است چون ردیف تکراری `(1, '2026-01-06')` تفاضل را خراب می‌کرد.

**Sessionization.** وقتی فاصله تا رویداد قبلی از آستانه بیشتر باشد، session جدید شروع می‌شود. آن ردیف‌ها را پرچم بزن، بعد جمع تجمعی پرچم‌ها را بگیر تا شماره‌ی session به دست بیاید:

```sql
WITH o AS (
  SELECT user_id, ts, LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) AS prev_ts
  FROM page_views
),
f AS (
  SELECT user_id, ts,
         CASE WHEN prev_ts IS NULL OR ts - prev_ts > INTERVAL '30 minutes'
              THEN 1 ELSE 0 END AS new_session
  FROM o
)
SELECT user_id,
       SUM(new_session) OVER (PARTITION BY user_id ORDER BY ts) AS session_id,
       ts
FROM f;
```

اولین ردیف هر کاربر `prev_ts` ای ندارد پس پرچمش ۱ است و session شماره ۱ را شروع می‌کند. بررسی `IS NULL` صریح نوشته شده چون `NULL > interval` برابر UNKNOWN است (ببین [[sql-nulls]]).

**Funnel.** یک ردیف برای هر کاربر بساز، با اولین زمان هر مرحله، و بعد کاربرهایی را بشمار که مراحلشان به ترتیب رخ داده. `COUNT` شرطی مقایسه‌های NULL را نادیده می‌گیرد، پس کاربری که هرگز ندیده، «بعد از دیدن سبد زده» حساب نمی‌شود:

```sql
WITH s AS (
  SELECT user_id,
         MIN(CASE WHEN ev = 'view' THEN ts END) AS v,
         MIN(CASE WHEN ev = 'cart' THEN ts END) AS c,
         MIN(CASE WHEN ev = 'buy'  THEN ts END) AS b
  FROM events GROUP BY user_id
)
SELECT COUNT(v) AS viewed,
       COUNT(CASE WHEN c > v THEN 1 END) AS carted_after_view,
       COUNT(CASE WHEN b > c THEN 1 END) AS bought_after_cart
FROM s;
```

**Deduplication.** جدیدترین ردیف هر کلید را نگه دار. یک ستون یکتا به انتهای ترتیب اضافه کن تا زمان‌های برابر هم یک برنده داشته باشند:

```sql
SELECT id, customer_id, updated_at, city
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id
                               ORDER BY updated_at DESC, id DESC) AS rn
  FROM customer_updates
) t
WHERE rn = 1;
```

**الگوهای دیگری که باید درست نام ببری.** میانه: `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)` در موتورهایی که پشتیبانی می‌کنند؛ میانه‌ی دستی میانگین یک یا دو ردیف وسطی است. هیستوگرام: `FLOOR(x / width) * width` شروع هر bucket را می‌دهد. Point-in-time lookup: فکت‌ها را به جدول SCD2 با `event_ts >= valid_from AND event_ts < valid_to` وصل کن (بازه‌ی نیمه‌باز، تا مرز دوبار شمرده نشود؛ ببین [[scd]]). بازه‌های هم‌پوشان: شروع‌ها را `+1` و پایان‌ها را `-1` در نظر بگیر و جمع تجمعی بگیر تا همزمانی به دست بیاید.

## 🧠 عمق و trade-offها (سنیور)

- **اول grain را انتخاب کن.** بیشتر اشتباه‌ها از شمارش در grain اشتباه می‌آیند (به‌ازای رویداد به‌جای کاربر، یا به‌ازای ردیف به‌جای روز). قبل از نوشتن `SELECT` یک جمله بنویس که خروجی در چه grainی است.
- **هزینه‌ی window.** هر الگو معمولاً به یک sort روی `(کلید بخش، کلید ترتیب)` نیاز دارد. روی جدول بزرگ همین sort هزینه است. قبل از window فیلتر تاریخ بزن و پنج window با پنج `ORDER BY` متفاوت نساز وقتی یک پاس کافی است.
- **skew.** top-N یا sessionization روی یک کلید داغ (مثلاً یک کاربر ربات با میلیون‌ها رویداد) در Spark یک task کند می‌شود و در انبار داده یک پارتیشن داغ. اول توزیع کلید را بررسی کن. ببین [[data-skew]].
- **streamها و بارگذاری افزایشی.** در تولید، این الگوها به‌صورت افزایشی اجرا می‌شوند. sessionization از مرز دو batch به session بازِ اجرای قبلی نیاز دارد، و dedup به یک پنجره‌ی بازبینی (lookback). پاسخ درست روی جدول ثابت در یک pipeline روزانه ممکن است غلط باشد؛ این را صریح بگو.
- **پشتیبانی موتور.** `QUALIFY` در برخی موتورها جای فیلتر بیرونی را می‌گیرد، `PERCENTILE_CONT` و `WIDTH_BUCKET` در همه‌ی موتورها نیستند، و نحو interval فرق دارد. منطق را بنویس، بعد نحو را با موتور گفته‌شده تطبیق بده.

## 🧪 امتحان کن

جدول‌های `employees` و `logins` ابتدای این درس را در PostgreSQL نسخه‌ی ۱۳ یا بالاتر استفاده کن.

1. **طول streak.** بلندترین streak پشت‌سرهم ورود را برای هر کاربر پیدا کن.
2. **هرگز وارد نشده.** کارمندانی را برگردان که `id` آن‌ها هرگز در `logins` نیامده (فرض کن `logins.user_id` به id کارمندان اشاره می‌کند). از شکلی استفاده کن که با NULL امن باشد.
3. **دومین حقوق متمایز بالا در هر بخش**، و برای بخشی که فقط یک حقوق متمایز دارد هیچ ردیفی نده.

<details><summary>راه‌حل‌ها را نشان بده</summary>

**1.**
```sql
WITH d AS (SELECT DISTINCT user_id, login_date FROM logins),
g AS (
  SELECT user_id, login_date,
         login_date - (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date))::int AS grp
  FROM d
),
streaks AS (
  SELECT user_id, COUNT(*) AS len FROM g GROUP BY user_id, grp
)
SELECT user_id, MAX(len) AS longest_streak
FROM streaks
GROUP BY user_id;
-- user 1: 3، user 2: 1
```

**2.**
```sql
SELECT e.id, e.name
FROM employees e
WHERE NOT EXISTS (SELECT 1 FROM logins l WHERE l.user_id = e.id);
```
از `NOT EXISTS` استفاده شده چون یک `NULL` در `logins.user_id` باعث می‌شد `NOT IN` هیچ ردیفی برنگرداند.

**3.**
```sql
SELECT dept, salary
FROM (
  SELECT dept, salary,
         DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
  FROM employees
) t
WHERE rnk = 2;
```
`DENSE_RANK` در بالاترین مقدار تساوی‌ها را می‌پرد، پس بخشی که فقط یک حقوق متمایز دارد ردیف `rnk = 2` ندارد و حذف می‌شود.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «کاربرانی با streak ورود سه‌روزه را پیدا کن.»** جواب متوسط: «از ترفند جزیره‌ها استفاده می‌کنم: `date - ROW_NUMBER()`، بعد داخل هر گروه می‌شمارم و گروه‌های حداقل سه‌تایی را نگه می‌دارم.» جواب سنیور: «اول grain را تأیید می‌کنم (یک ردیف برای هر کاربر در هر روز) و dedup می‌کنم، چون ورود دوتایی تفاضل را خراب می‌کند. همچنین می‌گویم همین منطق به‌صورت افزایشی چطور اجرا می‌شود، چون streak ممکن است از مرز batch عبور کند.»

**سؤال: «رویدادهای کلیک‌استریم را با timeout سی دقیقه sessionize کن.»** جواب متوسط: «`LAG` برای پیدا کردن فاصله، پرچم برای session جدید، و بعد `SUM` تجمعی.» جواب سنیور: «همان اسکلت، ولی اول skew کلید را بررسی می‌کنم، و می‌گویم وضعیت بین batchها چطور منتقل می‌شود، چون یک session ممکن است بین دو اجرا باشد.»

**سؤال: «آخرین رکورد هر مشتری را بده.»** جواب متوسط: «`ROW_NUMBER` بر اساس زمان، فیلتر روی ۱.» جواب سنیور: «برای قطعی بودن نتیجه یک tiebreaker یکتا اضافه می‌کنم و می‌گویم dedup در انبار داده انجام می‌شود یا در لایه‌ی ingestion.»

> **🎤 این را در مصاحبه بگو:** «الگو را از صورت سؤال تشخیص می‌دهم، grain و قاعده‌ی تساوی را می‌گویم، و اسکلت استاندارد را به کار می‌برم. بعد NULLها، تکرارها و این‌که آیا همان منطق به‌صورت افزایشی کار می‌کند را بررسی می‌کنم.»

## ⚠️ اشتباهات رایج

> **⚠️ توجه:** ترفند جزیره‌ها با تاریخ تکراری خراب می‌شود. همیشه اول روی `(user, day)` به‌صورت `DISTINCT` بگیر.

- انتخاب `ROW_NUMBER` برای «top N» وقتی باید همه‌ی تساوی‌ها آمده باشند (از `RANK` یا `DENSE_RANK` استفاده کن).
- فراموش کردن tiebreaker یکتا در dedup و گرفتن برنده‌ی تصادفی.
- استفاده از `NOT IN` برای anti-join با زیرکوئری nullable.
- فیلتر گرفتن روی نتیجه‌ی window در `WHERE` به‌جای query بیرونی.
- شمردن به‌ازای رویداد وقتی سؤال به‌ازای کاربر است، که نرخ‌های funnel را بالاتر نشان می‌دهد.
- دادن پاسخ جدول ثابت برای مسئله‌ی streaming یا افزایشی، بدون گفتن این‌که چه state‌ای لازم است.

## ✅ خودت را بسنج

1. برای «دو حقوق برتر در هر بخش، شامل تساوی‌ها»، کدام تابع بهتر است؟
   - A) `ROW_NUMBER`
   - B) `RANK` یا `DENSE_RANK`
   - C) `NTILE(2)`
   - D) `LAG`

<details><summary>جواب</summary>

**B** — `ROW_NUMBER` به هر ردیف مساوی شماره‌ی متفاوت می‌دهد، پس `rn <= 2` ممکن است یکی از تساوی‌ها را به‌طور تصادفی حذف کند. `RANK` یا `DENSE_RANK` به ردیف‌های مساوی یک شماره می‌دهند و آن‌ها را کنار هم نگه می‌دارند. با `RANK <= 2` دو موقعیت بالا با تساوی‌ها را می‌گیری؛ با `DENSE_RANK <= 2` دو حقوق متمایز بالا را. بگو سؤال کدام را می‌خواهد.

</details>

2. در الگوی جزیره‌ها، `login_date - ROW_NUMBER()` چه چیزی را نشان می‌دهد؟
   - A) تعداد ورودها
   - B) مقداری که داخل یک بازه‌ی پشت‌سرهم ثابت است و آن بازه را مشخص می‌کند
   - C) فاصله‌ی بین اولین و آخرین ورود
   - D) رتبه‌ی کاربر بر اساس فعالیت

<details><summary>جواب</summary>

**B** — داخل یک بازه‌ی پشت‌سرهم، هم تاریخ و هم شماره‌ی ردیف در هر ردیف یکی زیاد می‌شوند، پس تفاضلشان ثابت است. بازه‌های مختلف ثابت‌های متفاوت دارند.

</details>

3. چرا در کوئری sessionization پرچم با `prev_ts IS NULL OR ...` نوشته شده؟
   - A) برای حذف رویدادهای تکراری
   - B) اولین رویداد هر کاربر `prev_ts` ندارد، و مقایسه با NULL برابر UNKNOWN است؛ پس ردیف اول باید صریح پرچم بخورد
   - C) `LAG` با timestamp استفاده نمی‌شود
   - D) برای مرتب‌سازی خروجی

<details><summary>جواب</summary>

**B** — اولین رویداد `prev_ts` برابر NULL دارد. `ts - NULL > interval` برابر UNKNOWN است و `CASE` آن را رد می‌کند، پس بررسی صریح `IS NULL` باعث می‌شود اولین رویداد session شماره ۱ را شروع کند.

</details>

4. **(سنیور)** یک pipeline شبانه با timeout سی دقیقه sessionize می‌کند و هر اجرا یک روز را پردازش می‌کند. session یک کاربر ساعت ۲۳:۵۰ روز اول شروع و ساعت ۰۰:۲۰ روز دوم تمام می‌شود. چرا تعداد sessionهای روزانه با یک اجرای کامل روی کل تاریخچه فرق دارد، و چه چیزی را تغییر می‌دهی؟

   - A) تابع window غیرقطعی است
   - B) هر اجرا session بازِ مرز را گم می‌کند؛ وضعیت session باز و آخرین زمانش را به اجرای بعد منتقل کن (یا یک پنجره‌ی بازبینی بخوان)
   - C) موتور نمی‌تواند بیش از یک روز را پردازش کند
   - D) از `DISTINCT` روی شماره‌ی session استفاده کن

<details><summary>جواب</summary>

**B** — اجرای روزانه دو session جدا، یکی برای هر روز، می‌بیند چون از مرز عبور نمی‌تواند را ببیند. state دارای session باز را به جلو منتقل کن (یا پنجره‌ی بازبینی روز قبل را بخوان) تا نتیجه‌ی افزایشی با نتیجه‌ی تاریخچه‌ی کامل یکی شود.

</details>

5. **(سنیور)** یک job dedup از `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC)` استفاده می‌کند. در تولید، بعضی مشتری‌ها در هر بار اجرای مجدد شهر متفاوتی دارند. علت و راه‌حل چیست؟

   - A) کلید پارتیشن خیلی کلی است
   - B) تساوی روی `updated_at` ترتیب دلخواه دارد؛ یک tiebreaker یکتا مثل `id DESC` اضافه کن
   - C) `ROW_NUMBER` را نمی‌شود برای dedup استفاده کرد
   - D) job باید به‌جایش `RANK` استفاده کند

<details><summary>جواب</summary>

**B** — وقتی دو ردیف `updated_at` یکسان داشته باشند، `ROW_NUMBER` ممکن است هر کدام را انتخاب کند و این انتخاب بین اجراها عوض می‌شود. یک tiebreaker یکتا برنده را قطعی می‌کند.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| Top-N در هر گروه | `ROW_NUMBER() OVER (PARTITION BY g ORDER BY x DESC)` و بعد فیلتر در query بیرونی |
| کلید gaps & islands | `date - ROW_NUMBER()` داخل هر بازه‌ی پشت‌سرهم ثابت است (اول dedup کن) |
| Sessionization | `LAG` ← پرچم وقتی فاصله > آستانه ← `SUM` تجمعی = شماره‌ی session |
| Dedup | `ROW_NUMBER` با tiebreaker یکتا، نگه داشتن `rn = 1` |
| Funnel | یک ردیف برای هر کاربر با `MIN(CASE ...)` برای هر مرحله، بعد `COUNT` شرطی |
| «هرگز X نکرده» | `NOT EXISTS`، نه `NOT IN` روی ستون nullable |
| Point-in-time lookup | `event_ts >= valid_from AND event_ts < valid_to` روی جدول SCD2 |
| بازه‌های هم‌پوشان | `+1` در شروع، `-1` در پایان، جمع تجمعی = همزمانی |

## 🔗 ادامه

- اگر یکی از الگوها برایت مبهم بود، پایه‌ها را در `sql-window-functions`، `sql-nulls` و `sql-datetime` مرور کن.
- شکل‌های زیرکوئری و CTE که در همه‌ی الگوها استفاده می‌شوند: `sql-subqueries-ctes`.
- برای کارایی این کوئری‌ها روی جدول‌های بزرگ `sql-query-optimization` را بخوان. برای جنبه‌ی مدل‌سازی point-in-time lookup، `dimensional-modeling`.
