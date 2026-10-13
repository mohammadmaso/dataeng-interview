---
term: "تابع پنجره‌ای (Window function)"
term_en: "Window function"
aliases: ["window function", "توابع پنجره‌ای", "تابع تحلیلی"]
category: sql
short: "تابع SQL که روی مجموعه‌ای از سطرهای مرتبط با سطر جاری حساب می‌شود، بدون ادغام سطرها، با OVER(...)."
related: ["cte", "sessionization", "grain"]
---
## به زبان ساده (مبتدی)
جدول نتایج کلاس را تصور کنید که می‌خواهید کنار نمره‌ی هر دانش‌آموز میانگین کلاس یا رتبه‌اش را ببینید، ولی سطر همه‌ی دانش‌آموزان هم باقی بماند. `GROUP BY` معمولی جدول را به یک سطر برای هر کلاس فشرده می‌کند. تابع پنجره‌ای به هر سطر اجازه می‌دهد همسایه‌ها یا گروهش را ببیند و چیزی حساب کند بدون از دست دادن سطرها.

## نحوه‌ی کار (متوسط)
نحو: `func() OVER (PARTITION BY ... ORDER BY ... frame)`. `PARTITION BY` سطرها را گروه می‌کند، `ORDER BY` درون گروه مرتب می‌کند و frame (`ROWS`/`RANGE BETWEEN`) تعیین می‌کند کدام سطرهای اطراف سطر جاری شمرده شوند.

```
SELECT user_id, order_ts, amount,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_ts DESC) AS rn,
  SUM(amount)  OVER (PARTITION BY user_id ORDER BY order_ts
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
  LAG(amount)  OVER (PARTITION BY user_id ORDER BY order_ts) AS prev_amount
FROM orders;
```

خانواده‌ها: رتبه‌بندی (`ROW_NUMBER`، `RANK`، `DENSE_RANK`، `NTILE`)، offset (`LAG`، `LEAD`، `FIRST_VALUE`) و aggregate روی frame (`SUM`، `AVG`). کاربردها: top-N در هر گروه (فیلتر `rn = 1` در کوئری بیرونی)، جمع تجمعی، میانگین متحرک، gaps and islands، dedup و sessionization. توابع پنجره‌ای بعد از `WHERE`/`GROUP BY` اجرا می‌شوند پس نمی‌شود در همان `WHERE` روی آن‌ها فیلتر کرد؛ از CTE یا `QUALIFY` (در صورت پشتیبانی) استفاده کنید.

## نگاه سنیور و نکات مصاحبه
جزئیاتی که مصاحبه‌گر می‌پرسد: frame پیش‌فرض با `ORDER BY` برابر `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` است که مقدارهای مساوی را همتا می‌گیرد؛ پس جمع تجمعی با timestamp تکراری با `ROWS` فرق می‌کند. `RANK` شکاف می‌گذارد، `DENSE_RANK` نه، و `ROW_NUMBER` تساوی را دلخواه می‌شکند مگر tiebreaker بدهید. کارایی: هر مشخصه‌ی پنجره‌ی متفاوت یک sort/shuffle بر اساس کلید پارتیشن می‌خواهد، پس spec را مشترک کنید و پارتیشن‌های skewed را ببینید. در موتورهای توزیع‌شده نبودن `PARTITION BY` همه‌چیز را به یک نود می‌فرستد.

سؤال مصاحبه: «آخرین سفارش هر مشتری را بگیر.» پاسخ: `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC, order_id DESC)` در subquery و نگه‌داشتن `rn = 1`؛ tiebreaker اضافه نتیجه را قطعی می‌کند.
