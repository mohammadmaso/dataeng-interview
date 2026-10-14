---
term: "محدودسازی نرخ (rate limiting)"
term_en: "Rate limiting"
aliases: ["rate limiting","throttling","token bucket"]
category: distributed
short: "سقف‌گذاری روی تعداد درخواست یا عملیاتی که یک کلاینت یا job در واحد زمان می‌تواند بزند."
related: ["backpressure","retry","circuit-breaker"]
---
## به زبان ساده (مبتدی)
گیتی که در هر دقیقه تعداد محدودی نفر را رد می‌کند تا سالن شلوغ نشود.

## نحوه‌ی کار (متوسط)
الگوریتم‌ها: fixed window، sliding window، token bucket (burst مجاز با پرشدن ثابت) و leaky bucket (خروجی هموار). برای هر کاربر، کلید یا IP در API gateway یا داخل کلاینت‌های ingestion که API شخص ثالث را صدا می‌زنند اعمال می‌شود. پاسخ با 429 و Retry-After است.

## نگاه سنیور و نکات مصاحبه
محدودیت توزیع‌شده به state مشترک (Redis) یا تقریب نیاز دارد. کلاینت‌ها باید با jitter عقب‌نشینی کنند. در pipelineهای داده، rate limiting از سیستم‌های مبدأ و APIهای SaaS هنگام backfill محافظت می‌کند.

سؤال مصاحبه: «token در برابر leaky bucket؟» پاسخ: token bucket تا اندازه‌ی bucket burst می‌دهد؛ leaky bucket خروجی ثابت اعمال می‌کند.
