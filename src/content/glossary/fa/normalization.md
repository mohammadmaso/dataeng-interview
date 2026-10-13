---
term: "نرمال‌سازی و غیرنرمال‌سازی (Normalization)"
term_en: "Normalization and denormalization"
aliases: ["normalization", "denormalization", "3nf", "نرمال‌سازی", "غیرنرمال‌سازی", "one big table"]
category: modeling
short: "نرمال‌سازی با تقسیم داده به جدول‌های مرتبط افزونگی را حذف می‌کند (مناسب نوشتن OLTP)؛ غیرنرمال‌سازی داده را برای خواندن سریع‌تر تکرار می‌کند (مناسب تحلیل)."
related: ["oltp", "star-schema", "cardinality"]
---
## به زبان ساده (مبتدی)
نگه‌داشتن آدرس مشتری در یک جا و ارجاع به آن از هر سفارش یعنی جابه‌جایی فقط یک ویرایش می‌خواهد (نرمال). کپی آدرس روی هر سفارش جست‌وجو را آسان‌تر می‌کند ولی تغییر آدرس باید در جاهای زیادی اصلاح شود (غیرنرمال). این مصالحه‌ی بین به‌روزرسانی آسان و خواندن آسان است.

## نحوه‌ی کار (متوسط)
فرم‌های نرمال: 1NF (مقادیر اتمیک، بدون گروه تکرارشونده)، 2NF (بدون وابستگی جزئی به بخشی از کلید مرکب)، 3NF (بدون وابستگی انتقالی: صفت‌های غیرکلید فقط به کلید وابسته‌اند). schema نرمال از ناهنجاری update/insert/delete جلوگیری و فضا صرفه‌جویی می‌کند؛ با OLTP و نوشتن‌های کوچک زیاد جور است و بهایش join برای خواندن است.

```
-- Normalized (3NF): each fact stored once
customers(customer_id PK, name, city_id FK)   cities(city_id PK, city, country)
orders(order_id PK, customer_id FK, ordered_at)  order_items(order_id, product_id, qty, price)

-- Denormalized (analytics): one wide table, no joins at query time
fct_orders_wide(order_id, ordered_at, customer_name, city, country, product_name, qty, price)
-- Pro: fewer joins, columnar compression hides repetition. Con: updates must touch many rows.
```

تحلیل مصالحه را برعکس می‌کند: داده به‌صورت batch نوشته، زیاد خوانده و کم به‌روز می‌شود؛ پس مدل‌های ابعادی غیرنرمال (star schema) یا جدول‌های پهن join را کم می‌کنند و فشرده‌سازی ستونی و dictionary encoding تکرار را ارزان می‌کنند. storeهای سند و NoSQL به‌طور پیش‌فرض غیرنرمال‌اند (embedding) و سازگاری را به برنامه واگذار می‌کنند.

## نگاه سنیور و نکات مصاحبه
هزینه‌های غیرنرمال‌سازی: ناهنجاری update (انتشار تغییر)، فضای بیشتر و ریسک کپی‌های ناسازگار؛ پس باید با pipeline از منبع نرمال یا معتبر مشتق شود نه دستی نگهداری. برای تاریخچه SCD2 را به تکرار ویژگی‌های متغیر ترجیح دهید. «one big table» BI را سریع و باگ join را حذف می‌کند ولی grain را پنهان و عرض را منفجر می‌کند؛ آن را لایه‌ی پایین‌دستی (gold) ساخته‌شده از dimensionهای مدل‌شده نگه دارید. موتورهای توزیع‌شده غیرنرمال‌سازی یا broadcast ابعاد کوچک را ترجیح می‌دهند چون joinهای shuffle بزرگ گران‌اند. پاسخ خوب می‌گوید هر سبک در کجای stack می‌نشیند نه اینکه یکی را برگزیند.

سؤال مصاحبه: «برای warehouse تحلیلی نرمال یا غیرنرمال؟» پاسخ: مدل ابعادی غیرنرمال برای لایه‌ی serving تا join کم و SQL ساده شود، ساخته‌شده از منبع نرمال/خام معتبر با pipelineهای خودتوان، با SCD2 برای تاریخچه و تست برای سازگاری کپی‌ها.
