---
term: "شمای ستاره‌ای (Star schema)"
term_en: "Star schema"
aliases: ["star schema", "شمای ستاره‌ای", "مدل ابعادی", "snowflake schema"]
category: modeling
short: "مدل ابعادی با یک جدول fact مرکزی که مستقیم به جدول‌های dimension غیرنرمال وصل است؛ بهینه برای کوئری تحلیلی."
related: ["fact-table", "dimension-table", "grain", "surrogate-key"]
---
## به زبان ساده (مبتدی)
دفتر فروش را وسط یک میز تصور کنید که دورش کارت‌های مرجع کوچک است: یکی برای محصولات، یکی برای مشتری‌ها، تاریخ‌ها و فروشگاه‌ها. هر خط فروش به کارت‌هایش اشاره می‌کند. روی کاغذ شبیه ستاره می‌شود. این چیدمان پاسخ پرسش‌هایی مثل «فروش بر اساس ماه و دسته‌ی محصول» را آسان می‌کند.

## نحوه‌ی کار (متوسط)
مرکز، جدول fact است (اندازه‌گیری‌ها در grain اعلام‌شده: تعداد، مبلغ) با کلید خارجی (معمولاً کلید جانشین) به جدول‌های dimension (زمینه‌ی توصیفی: چه کسی، چه چیزی، کجا، کِی). dimensionها غیرنرمال‌اند (سطر محصول دسته و برند خود را دارد) پس هر کوئری به‌ازای هر dimension یک join می‌خواهد و زنجیره‌ی join بلند ندارد. snowflake schema ابعاد را به زیرجدول نرمال می‌کند، کمی فضا کم می‌کند ولی join بیشتر دارد.

```
                dim_date
                   │
dim_customer ── fct_sales ── dim_product
                   │
                dim_store

SELECT d.month, p.category, SUM(f.net_amount)
FROM fct_sales f
JOIN dim_date d    ON f.date_sk = d.date_sk
JOIN dim_product p ON f.product_sk = p.product_sk
GROUP BY d.month, p.category;
```

فرایند طراحی (Kimball): فرایند کسب‌وکار را انتخاب، grain را اعلام، dimensionها را شناسایی و factها را شناسایی کنید. conformed dimension (همان `dim_customer` در چند جدول fact) drill-across را ممکن می‌کند. degenerate dimension (شماره‌ی سفارش در fact)، junk dimension و role-playing dimension (تاریخ به‌عنوان تاریخ سفارش و ارسال) الگوهای رایج‌اند.

## نگاه سنیور و نکات مصاحبه
ستاره برای BI می‌درخشد: SQL ساده، joinهای قابل‌پیش‌بینی، تناسب عالی با موتورهای ستونی و broadcast join ابعاد کوچک. مصالحه‌ها: هزینه‌ی ETL برای ساخت و نگهداری، مدیریت تاریخچه با SCD، سفتی در مقایسه با schema-on-read، و تکرار ویژگی‌ها به‌خاطر denormalization. در سیستم‌های ستونی مدرن بعضی تیم‌ها برای سرعت و سادگی «one big table» پهن می‌سازند؛ با این حال مدل ستاره متریک‌های governed و استفاده‌ی مجدد می‌دهد. برای مقایسه‌ی Kimball (martهای پایین‌به‌بالا)، Inmon (انبار نرمال سازمانی) و Data Vault آماده باشید.

سؤال مصاحبه: «مدلی برای تحلیل e-commerce طراحی کن.» پاسخ: fact فروش با grain آیتم سفارش، کلیدهای جانشین به dimensionهای تاریخ، محصول، مشتری و فروشگاه، معیارهایی مثل تعداد و مبلغ خالص، SCD2 روی مشتری و محصول جایی که تاریخچه مهم است و یک dimension تاریخ conformed.
