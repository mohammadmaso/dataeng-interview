---
term: "Circuit breaker"
term_en: "Circuit breaker"
aliases: ["circuit breaker","قطع‌کننده‌ی مدار"]
category: distributed
short: "محافظی که مدتی فراخوانی یک وابستگی خراب را متوقف می‌کند تا بهبود یابد."
related: ["retry","rate-limiting","backpressure"]
---
## به زبان ساده (مبتدی)
مثل فیوز برق: وقتی چیزی مدام خراب است قطع می‌کند به‌جای اینکه خانه بسوزد.

## نحوه‌ی کار (متوسط)
حالت‌ها: closed (فراخوانی‌ها رد می‌شوند)، open (بعد از آستانه‌ی خطا فوراً fail می‌شوند) و half-open (چند فراخوانی آزمایشی برای تست بهبود). از طوفان retry و تمام‌شدن thread وقتی downstream کند یا خراب است جلوگیری می‌کند.

## نگاه سنیور و نکات مصاحبه
با timeout، retry کران‌دار با jitter و fallback ترکیب کنید. در pipelineها، breaker روی API ناپایدار منبع مانع کوبیدن آن می‌شود و DLQ کار را برای بعد نگه می‌دارد.

سؤال مصاحبه: «retryها خرابی را بدتر کردند، چرا؟» پاسخ: retryهای هم‌زمان بار را تقویت می‌کنند؛ از backoff، jitter و circuit breaker استفاده کن.
