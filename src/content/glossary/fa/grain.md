---
term: "دانه‌بندی (Grain)"
term_en: "Grain"
aliases: ["grain", "گرین", "سطح جزئیات", "دانه‌بندی"]
category: modeling
short: "معنای دقیق یک سطر در جدول fact، مثلاً یک آیتم از هر سفارش؛ اولین تصمیم در طراحی مدل ابعادی."
related: ["fact-table", "dimension-table", "star-schema"]
---
## به زبان ساده (مبتدی)
یک جدول خرید را تصور کنید. در یک نسخه برای هر رسید یک سطر است؛ در نسخه‌ی دیگر برای هر کالای هر رسید یک سطر. فروشگاه یکی است ولی سطح جزئیات فرق دارد. grain جمله‌ای است که دقیقاً می‌گوید یک سطر چه چیزی را نشان می‌دهد. نوشتنش در ابتدا از گیج‌شدن درباره‌ی اینکه چه چیزی را می‌شمارید و جمع می‌زنید جلوگیری می‌کند.

## نحوه‌ی کار (متوسط)
در مدل‌سازی به سبک Kimball مراحل چنین است: فرایند کسب‌وکار را انتخاب کن، grain را اعلام کن، سپس dimensionها و factها را بیاور. grain باید به زبان کسب‌وکار بیان شود («یک سطر برای هر آیتم سفارش در هر روز») و هر ستون جدول fact باید در همان grain درست باشد؛ dimensionهای متصل باید در آن grain تک‌مقداری باشند.

```
-- Grain: one row per order line item
CREATE TABLE fct_order_items (
  order_id BIGINT, line_no INT,           -- together define the grain
  product_sk BIGINT, customer_sk BIGINT, date_sk INT,
  quantity INT, net_amount DECIMAL(12,2),
  PRIMARY KEY (order_id, line_no)
);
```

ریزترین grain اتمیک ممکن را ترجیح دهید چون بالا رفتن (aggregate) ممکن است ولی drill down ممکن نیست. ترکیب grainها در یک جدول (تخفیف سطح سفارش که روی هر آیتم تکرار می‌شود) جمع را خراب می‌کند: `SUM(order_discount)` چندبار می‌شمارد.

## نگاه سنیور و نکات مصاحبه
بیشتر باگ‌های «عدد غلط» به grain برمی‌گردند: joinای که تعداد سطرها را تغییر می‌دهد (fan-out) و جمع را بی‌صدا باد می‌کند. grain را قبل و بعد از هر join بررسی کنید و تست یکتایی کلید اعلام‌شده بنویسید. معیارهای سطح header باید به سطح آیتم تخصیص داده شوند یا در fact جدا بیایند. جدول‌های aggregate و materialized view باید grain درشت‌تر خود را اعلام کنند. factهای transaction، periodic snapshot و accumulating snapshot معنای grain متفاوتی دارند.

سؤال مصاحبه: «بعد از افزودن یک join درآمد دو برابر شد؛ چرا؟» پاسخ: جدول join‌شده grain ریزتر داشت و سطرهای سفارش تکرار شدند (fan-out)؛ طرف «چند» را اول به grain اصلی aggregate می‌کنم یا با کلید کامل grain join می‌زنم و تست یکتایی می‌افزایم.
