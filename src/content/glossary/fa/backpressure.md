---
term: "فشار برگشتی (Backpressure)"
term_en: "Backpressure"
aliases: ["backpressure", "بک‌پرشر", "فشار برگشتی"]
category: streaming
short: "سازوکار بازخوردی که در آن مصرف‌کننده‌ی کند باعث کند شدن تولیدکننده‌های بالادست می‌شود تا غرق نشود."
related: ["consumer-lag", "checkpoint", "dead-letter-queue"]
---
## به زبان ساده (مبتدی)
سینک شلوغ آشپزخانه را تصور کنید: اگر آب کند تخلیه شود شیر را کم می‌کنید تا سرریز نشود. backpressure همین است برای داده. وقتی مرحله‌ی پایین‌دست نمی‌رسد به بالادست سیگنال می‌دهد که کندتر شود تا چیزی سرریز یا crash نکند.

## نحوه‌ی کار (متوسط)
بدون کنترل جریان، تولیدکننده‌ی سریع بافر مصرف‌کننده‌ی کند را پر می‌کند تا حافظه تمام شود یا داده دور ریخته شود. استراتژی‌ها: بلاک‌کردن تولیدکننده (صف‌های محدود، بافرهای شبکه‌ی Flink با credit-based flow control)، مصرف pull-based (consumerهای Kafka با سرعت خودشان fetch می‌کنند و لاگ خودش burst را جذب می‌کند)، rate limiting، load shedding (حذف یا نمونه‌گیری) یا بافر در ذخیره‌سازی پایدار.

```
source ──► map ──► keyBy/window ──► sink(slow DB)
                                      ▲ buffers fill
          ◄──── credit/queue-full signal propagates upstream ────
# Flink: watch backPressuredTimeMsPerSecond / busyTimeMsPerSecond per task
# Reactive Streams: subscriber calls request(n); producer sends at most n
```

در Flink عملگر گلوگاه بافر ورودی‌اش را پر می‌کند و این فشار task به task تا source بالا می‌رود و source کمتر از Kafka می‌خواند؛ lag در Kafka جمع می‌شود نه حافظه در Flink. Spark Structured Streaming با `maxOffsetsPerTrigger` اندازه‌ی batch را محدود می‌کند.

## نگاه سنیور و نکات مصاحبه
backpressure نشانه است نه راه‌حل: عملگر کند را پیدا کنید (با busy time بالا که بالادستش backpressured است) و علت را رفع کنید: sink کند، کلید skewed، دسترسی سنگین به state، GC یا موازی‌سازی کم. خطرها: barrierهای checkpoint پشت داده‌ی backpressured گیر می‌کنند و checkpoint timeout می‌خورد (unaligned checkpoint کمک می‌کند)، latency بالا می‌رود، و backpressure طولانی به consumer lag و در صورت اتمام retention به از دست رفتن داده می‌انجامد. سیستم‌های push-based بدون کنترل جریان (webhook، UDP) صف صریح یا shedding می‌خواهند.

سؤال مصاحبه: «اگر sink کندتر از source باشد چه می‌شود؟» پاسخ: با بافر محدود و credit-based flow control کندی تا source بالا می‌رود و کمتر می‌خواند؛ lag در لاگ پایدار جمع می‌شود نه حافظه، و سپس گلوگاه را scale یا رفع می‌کنم.
