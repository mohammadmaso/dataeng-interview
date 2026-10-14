---
term: "اجرای حدسی (speculative execution)"
term_en: "Speculative execution"
aliases: ["speculative execution","اجرای اسپکیولیتیو"]
category: batch
short: "اجرای نسخه‌ی کپی یک task کند جای دیگر و برداشتن نتیجه‌ی اولین تمام‌شده."
related: ["data-skew","shuffle","dag"]
---
## به زبان ساده (مبتدی)
اگر یکی از دونده‌های امدادی کند است، دونده‌ی دومی را در همان مسیر می‌فرستیم و هر کدام زودتر رسید را می‌پذیریم.

## نحوه‌ی کار (متوسط)
scheduler taskهایی را که بسیار کندتر از میانه‌ی stage خود هستند تشخیص می‌دهد و کپی آن‌ها را روی executorهای دیگر اجرا می‌کند؛ اولی که تمام شود برنده است و دیگری kill می‌شود. برای node خراب و کندی گذرا مفید است. در Spark: spark.speculation.

## نگاه سنیور و نکات مصاحبه
data skew را حل نمی‌کند (کپی هم به همان اندازه کند است) و می‌تواند side effect را تکرار کند، پس taskها باید idempotent و sinkها تحمل‌کننده‌ی retry باشند. با تنظیم غلط منابع را هدر می‌دهد.

سؤال مصاحبه: «یک task عقب‌مانده stage را کند می‌کند؛ speculation یا salting؟» پاسخ: اگر ماشین کند است speculation؛ اگر پارتیشن عظیم است skew را اصلاح کن.
