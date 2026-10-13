---
term: "عبارت جدول مشترک (CTE)"
term_en: "CTE (common table expression)"
aliases: ["cte", "common table expression", "عبارت جدول مشترک"]
category: sql
short: "نتیجه‌ی موقت نام‌دار که با WITH تعریف می‌شود و کوئری می‌تواند به آن ارجاع دهد؛ خوانایی را بالا می‌برد و بازگشت را ممکن می‌کند."
related: ["window-function", "grain"]
---
## به زبان ساده (مبتدی)
وقتی یک مسئله‌ی ریاضی بلند حل می‌کنید نتیجه‌های میانی را با اسم کنار صفحه می‌نویسید: «فرض کن A جمع کل باشد، B می‌شود A تقسیم بر ۳.» CTE همین کار را در SQL می‌کند. به یک نتیجه‌ی میانی اسم می‌دهید و در کوئری نهایی از آن استفاده می‌کنید؛ پس کوئری‌های بلند قدم‌به‌قدم خوانا می‌شوند.

## نحوه‌ی کار (متوسط)
CTE با `WITH name AS (subquery)` شروع می‌شود و در دامنه‌ی همان statement است. می‌توان چندتا را زنجیره کرد که هرکدام به قبلی‌ها ارجاع دهد. خودش داده‌ای ذخیره نمی‌کند. اینکه inline یا materialize شود به موتور بستگی دارد: Postgres از نسخه‌ی ۱۲ به بعد به‌طور پیش‌فرض inline می‌کند مگر چند بار ارجاع شود یا `MATERIALIZED` باشد؛ موتورهای دیگر ممکن است برای هر ارجاع دوباره اجرا یا cache کنند.

```
WITH recent AS (
  SELECT * FROM orders WHERE order_ts >= current_date - 30
),
per_user AS (
  SELECT user_id, SUM(amount) AS total FROM recent GROUP BY user_id
)
SELECT * FROM per_user WHERE total > 1000;

-- recursive: org chart
WITH RECURSIVE tree AS (
  SELECT id, manager_id, 1 AS lvl FROM emp WHERE manager_id IS NULL
  UNION ALL
  SELECT e.id, e.manager_id, t.lvl + 1 FROM emp e JOIN tree t ON e.manager_id = t.id
)
SELECT * FROM tree;
```

`WITH RECURSIVE` یک عضو anchor و یک عضو بازگشتی با `UNION ALL` دارد و تا وقتی سطر جدید نیاید تکرار می‌شود. برای سلسله‌مراتب، پیمایش گراف و تولید دنباله. با محدودیت عمق یا ستون مسیر دیده‌شده جلوی چرخه را بگیرید.

## نگاه سنیور و نکات مصاحبه
فرض نکنید CTE مانع بهینه‌سازی یا cache است؛ در موتورهای مختلف فرق دارد و باید plan را ببینید. CTE که دو بار ارجاع شود در موتورهای inline ممکن است دو بار محاسبه شود (روی shuffleهای بزرگ گران) و نسخه‌ی materialized می‌تواند predicate pushdown را مسدود کند. برای استفاده‌ی مجدد از نتیجه‌ی میانی سنگین، temp table یا materialized view بهتر است. CTE برای خوانایی و در مدل‌های dbt بر subquery تو در تو ترجیح دارد؛ زنجیره‌ی بلند ممکن است پنهان کند که `GROUP BY` قبل از join است که روی grain اثر دارد.

سؤال مصاحبه: «CTE در برابر subquery و temp table؟» پاسخ: CTE و subquery از نظر منطقی معادل‌اند و بیشتر برای خوانایی؛ temp table نتیجه را فیزیکی ذخیره می‌کند و برای استفاده‌ی چندباره یا نیاز به آمار و index مفید است؛ بازگشت تنها کاری است که فقط CTE می‌کند.
