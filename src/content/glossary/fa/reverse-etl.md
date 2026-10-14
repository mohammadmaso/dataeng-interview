---
term: "Reverse ETL"
term_en: "Reverse ETL"
aliases: ["reverse etl","فعال‌سازی داده"]
category: ingestion
short: "همگام‌سازی داده‌ی مدل‌شده‌ی warehouse به ابزارهای عملیاتی مثل CRM و پلتفرم‌های تبلیغاتی."
related: ["etl-elt","data-warehouse","data-pipeline"]
---
## به زبان ساده (مبتدی)
به‌جای فقط نگاه‌کردن به گزارش‌ها، بینش‌ها را به ابزارهایی که مردم هر روز استفاده می‌کنند برمی‌گردانی، مثل فرستادن برچسب «مشتری باارزش» به اپ فروش.

## نحوه‌ی کار (متوسط)
یک job همگام‌سازی جدول‌ها یا مدل‌های warehouse را می‌خواند و از طریق API به سیستم‌های SaaS و عملیاتی (Salesforce، HubSpot، تبلیغات، ایمیل) upsert می‌کند، diff را ردگیری و rate limit را مدیریت می‌کند. ابزارها: Census، Hightouch یا jobهای سفارشی.

## نگاه سنیور و نکات مصاحبه
مسائل: rate limit API، upsert idempotent با شناسه‌ی خارجی، حذف‌ها، نگاشت schema، governance داده‌ی شخصی و اینکه چه کسی مسئول کیفیت داده در مقصد است. warehouse را منبع حقیقت عملیات می‌کند، پس SLA تازگی مهم می‌شود.

سؤال مصاحبه: «امتیاز ریسک ریزش را چطور به CRM sync می‌کنی؟» پاسخ: در warehouse محاسبه می‌کنم، با آخرین وضعیت sync‌شده diff می‌گیرم و ردیف‌های تغییرکرده را با شناسه‌ی حساب و retry و rate limit upsert می‌کنم.
