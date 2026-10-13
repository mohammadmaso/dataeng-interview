---
term: "تلاش مجدد با backoff (Retry)"
term_en: "Retry with backoff"
aliases: ["retry", "retries", "backoff", "تلاش مجدد", "exponential backoff"]
category: distributed
short: "تکرار عملیات ناموفق با تأخیرهای فزاینده و تصادفی تا خطاهای موقت بدون فشار اضافه بر وابستگی بازیابی شوند."
related: ["idempotency", "dead-letter-queue", "backpressure"]
---
## به زبان ساده (مبتدی)
اگر خط تلفن مغازه اشغال باشد، ده بار پشت‌سرهم زنگ زدن کمکی نمی‌کند و خط را شلوغ‌تر می‌کند. کمی صبر می‌کنید، دوباره امتحان می‌کنید، بیشتر صبر می‌کنید و دوباره. افزودن کمی تصادفی بودن به مدت انتظار باعث می‌شود همه در یک ثانیه زنگ نزنند. این retry با backoff و jitter است.

## نحوه‌ی کار (متوسط)
خطاها به موقت (timeout، 503، rate limit، انتخاب leader) و دائمی (ورودی بد، احراز هویت، خطای schema) تقسیم می‌شوند. فقط موقت‌ها را با تعداد تلاش محدود و تأخیر نمایی فزاینده به‌علاوه‌ی jitter retry کنید تا کلاینت‌های هم‌زمان retry storm («thundering herd») نسازند. برای هر تلاش timeout و برای کل کار deadline تعیین کنید.

```
import random, time

def call_with_retry(fn, max_attempts=5, base=0.5, cap=30):
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except TransientError:
            if attempt == max_attempts: raise
            delay = min(cap, base * 2 ** attempt)
            time.sleep(random.uniform(0, delay))     # "full jitter"
# Airflow: retries=3, retry_delay=timedelta(minutes=5), retry_exponential_backoff=True
```

retry باعث می‌شود عملیات بیش از یک بار اجرا شود، پس عملیات باید خودتوان باشد یا کلید idempotency داشته باشد. در سطح pipeline: retry تسک orchestrator (`retries` در Airflow)، retry در producer کافکا (`enable.idempotence` از تکرار و بر‌هم‌خوردن ترتیب جلوگیری می‌کند)، retry topic برای consumer و سیاست‌های کلاینت HTTP. circuit breaker از retry وابستگی که آشکارا down است جلوگیری می‌کند.

## نگاه سنیور و نکات مصاحبه
حالت‌های خرابی: تقویت retry بین لایه‌ها (۳ retry در هر یک از ۴ لایه = ۸۱ فراخوانی)، retry کردن side effectهای غیرخودتوان (شارژ دوباره یا سطر تکراری)، retryهایی که پارتیشن مرتب را نگه می‌دارند (به‌جای بلاک از retry topic و DLQ استفاده کنید) و پنهان‌کردن مشکل سیستمی با retry بی‌صدا؛ همیشه متریک و هشدار روی نرخ retry داشته باشید. هدر `Retry-After` و rate limit را رعایت کنید. بودجه‌ی retry (محدودکردن retry به کسری از ترافیک) در زمان قطعی از وابستگی محافظت می‌کند.

سؤال مصاحبه: «task در Airflow یک API ناپایدار را صدا می‌زند؛ چه تنظیم می‌کنی؟» پاسخ: تعداد کمی retry با exponential backoff و jitter، timeout هر تلاش، درخواست خودتوان (کلید idempotency یا upsert) و هشدار شکست پس از اتمام retryها؛ خطای دائمی بدون retry سریع fail می‌شود.
