---
term: "ایندکس (Index)"
term_en: "Database index"
aliases: ["index", "ایندکس", "فهرست", "covering index", "composite index"]
category: sql
short: "ساختار کمکی که به دیتابیس اجازه می‌دهد بدون اسکن کل جدول سطرها را پیدا کند، به بهای فضای بیشتر و نوشتن کندتر."
related: ["b-tree", "predicate-pushdown", "cardinality"]
---
## به زبان ساده (مبتدی)
فهرست یک کتاب به شما اجازه می‌دهد به‌جای خواندن کل کتاب مستقیم به صفحه‌های درست بروید. ایندکس دیتابیس همین را برای یک ستون جدول می‌کند: دیتابیس می‌تواند «سفارش‌های کاربر ۷» را فوراً پیدا کند. اما هر بار محتوای جدید اضافه شود ایندکس هم باید به‌روز شود؛ پس ایندکس بیشتر یعنی نوشتن کندتر.

## نحوه‌ی کار (متوسط)
بیشتر ایندکس‌ها B+tree روی یک یا چند ستون هستند. ایندکس مرکب روی `(a, b)` اول بر اساس `a` سپس `b` مرتب است؛ پس به فیلتر روی `a` یا `a,b` کمک می‌کند ولی به `b` به‌تنهایی نه (قاعده‌ی پیشوند چپ). covering index همه‌ی ستون‌های لازم کوئری را دارد تا به جدول مراجعه نشود (index-only scan). انواع دیگر: hash، GIN/inverted (جست‌وجوی متن، آرایه، JSON)، GiST/مکانی، bitmap، Bloom، ایندکس جزئی (فقط سطرهای منطبق با شرط) و ایندکس عبارت.

```
CREATE INDEX idx_orders_user_ts ON orders (user_id, order_ts DESC);   -- composite

-- Uses the index (leftmost prefix, then range):
SELECT * FROM orders WHERE user_id = 7 AND order_ts > '2024-05-01';
-- Cannot use it well (skips leading column / wraps column in function):
SELECT * FROM orders WHERE order_ts > '2024-05-01';
SELECT * FROM orders WHERE date(order_ts) = '2024-05-01';

EXPLAIN (ANALYZE) SELECT ...;      -- Index Scan vs Seq Scan
```

optimizer بر اساس آمار selectivity تصمیم می‌گیرد از ایندکس استفاده کند یا نه: برای فیلتری که سهم بزرگی از سطرها را می‌گیرد، sequential scan از lookupهای تصادفی زیاد ارزان‌تر است. با `EXPLAIN` بررسی کنید و آمار را با `ANALYZE` تازه نگه دارید.

## نگاه سنیور و نکات مصاحبه
هر ایندکس write amplification، فضا و نگهداری دارد؛ ایندکس بلااستفاده یا تکراری هدر رایج است. ایندکس را از الگوی کوئری طراحی کنید: ستون‌های برابری اول، سپس ستون‌های بازه/sort؛ «selectivity بالا اول» heuristic است نه قانون. تله‌ها: تابع یا cast ضمنی روی ستون ایندکس‌شده، ستون cardinality پایین به‌تنهایی، شرط `OR`، wildcard ابتدایی در `LIKE`. در مهندسی داده ساخت ایندکس روی جدول بزرگ ممکن است قفل کند یا ساعت‌ها طول بکشد (build هم‌زمان). موتورهای تحلیلی/ستونی به‌جای ایندکس هر سطر از sort key، zone map و skip index استفاده می‌کنند.

سؤال مصاحبه: «با وجود ایندکس کوئری کند است؛ چرا؟» پاسخ: plan را می‌بینم: شاید شرط sargable نیست (تابع یا cast روی ستون)، ستون ابتدایی فیلتر نشده، آمار کهنه است یا فیلتر گزینشی نیست و full scan می‌برد؛ شرط، ترتیب ایندکس را اصلاح یا ایندکس covering/جزئی اضافه می‌کنم.
