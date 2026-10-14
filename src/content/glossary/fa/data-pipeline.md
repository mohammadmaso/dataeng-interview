---
term: "Pipeline داده"
term_en: "Data pipeline"
aliases: ["data pipeline","pipeline","پایپ‌لاین"]
category: orchestration
short: "زنجیره‌ای از گام‌های خودکار که داده را از منبع به مقصد منتقل و تبدیل می‌کند."
related: ["etl-elt","dag","idempotency","data-quality"]
---
## به زبان ساده (مبتدی)
یک نوار نقاله: مواد خام از یک سر وارد می‌شوند، در ایستگاه‌ها تمیز و شکل‌دهی می‌شوند و در سر دیگر آماده‌ی استفاده می‌رسند.

## نحوه‌ی کار (متوسط)
pipeline از منابع (دیتابیس، API، رویداد) ingest می‌کند، تبدیل می‌کند (پاک‌سازی، join، aggregate) و به مقصد (warehouse، lake، serving store) بارگذاری می‌کند. به‌صورت DAG orchestrate می‌شود، پایش می‌شود و با retry، idempotency، backfill، تست و هشدار قابل‌اعتماد می‌شود.

## نگاه سنیور و نکات مصاحبه
pipeline را با درستی، تازگی، هزینه، قابلیت بازیابی و observability بسنجید، نه فقط اجرا شدن. برای شکست طراحی کنید: اجرای نیمه‌کاره، تکرار، تغییر schema و داده‌ی دیررسیده. pipeline را نرم‌افزار بدانید: version control، CI، محیط‌ها و code review.

سؤال مصاحبه: «چه چیزی pipeline را production-ready می‌کند؟» پاسخ: اجرای مجدد idempotent، چک‌های کیفیت داده، هشدار تازگی/حجم، lineage و مالک مستند.
