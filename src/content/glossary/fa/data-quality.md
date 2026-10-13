---
term: "کیفیت داده (Data quality)"
term_en: "Data quality"
aliases: ["data quality", "کیفیت داده", "data validation", "data tests", "اعتبارسنجی داده"]
category: quality
short: "میزان مناسب‌بودن داده برای استفاده، با بررسی‌هایی مثل کامل‌بودن، اعتبار، یکتایی، تازگی، دقت و سازگاری."
related: ["data-contract", "data-observability", "lineage"]
---
## به زبان ساده (مبتدی)
با مواد اولیه‌ی بد، غذای بد درمی‌آید هرچند دستور پخت عالی باشد. کیفیت داده یعنی بررسی مواد اولیه: چیزی کم است، خراب شده، تکراری یا کهنه است؟ اگر در ورودی چک شود مشکلات قبل از رسیدن به مشتری‌ها (گزارش‌ها و مدل‌ها) گرفته می‌شود.

## نحوه‌ی کار (متوسط)
ابعاد رایج: completeness (نبود null یا سطر غیرمنتظره)، validity (نوع، بازه، قالب، enum)، uniqueness (نبود تکراری روی کلیدها)، consistency (هماهنگی یک واقعیت بین جدول‌ها، یکپارچگی ارجاعی)، accuracy (انطباق با واقعیت؛ سخت‌ترین برای تست) و timeliness/freshness (به‌موقع رسیدن). بررسی‌ها در pipeline به شکل تست پیاده می‌شوند: dbt tests، Great Expectations، Soda، Deequ، به‌علاوه‌ی بررسی ناهنجاری تعداد سطر/حجم و تطبیق با منبع.

```
# dbt tests (schema.yml)
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: customer_sk
        tests: [{relationships: {to: ref('dim_customer'), field: customer_sk}}]
      - name: status
        tests: [{accepted_values: {values: ['NEW','PAID','CANCELLED']}}]
    tests:
      - dbt_utils.recency: {datepart: hour, field: loaded_at, interval: 6}   # freshness
```

جای قرارگیری: هنگام ingestion (اعتبارسنجی schema/قرارداد، quarantine سطرهای بد)، بین لایه‌ها (bronze به silver) و قبل از انتشار (گیت مسدودکننده در DAG تا داده‌ی بد دیده نشود). برای هر بررسی تصمیم بگیرید: شکست pipeline (block)، هشدار یا quarantine سطرها.

## نگاه سنیور و نکات مصاحبه
کیفیت در مقیاس نیاز به مالکیت و شدت دارد: هر بررسی ناموفقی نباید کسی را page کند یا pipeline را متوقف کند. برای «ندانسته‌های ندانسته» از آستانه و تشخیص ناهنجاری (حجم، نرخ null، drift توزیع) استفاده کنید نه فقط قواعد ایستا. الگوی write-audit-publish: نوشتن در branch/جدول staging، audit و سپس انتشار اتمیک (branch در Iceberg، swap پارتیشن). متریک‌ها: نرخ pass، زمان تشخیص، زمان رفع و تعداد حادثه به‌تفکیک منبع. ریشه‌ی خرابی‌ها معمولاً تغییر بالادست است پس با data contract به چپ شیفت دهید. تست‌های کیفیت جلوی joinهای بی‌صدا غلط (fan-out)، رایج‌ترین باگ درستی، را هم می‌گیرند.

سؤال مصاحبه: «چطور کیفیت داده را در pipeline تضمین می‌کنی؟» پاسخ: انتظارات را با مالک تعریف می‌کنم (یکتایی، not-null، یکپارچگی ارجاعی، تازگی، حجم)، در ingestion و بین لایه‌ها تست می‌کنم، با write-audit-publish شکست‌ها را مانع انتشار می‌کنم، با شدت هشدار می‌دهم و متریک کیفیت را ردیابی می‌کنم؛ قراردادها جلوی شکست بالادست را می‌گیرند.
