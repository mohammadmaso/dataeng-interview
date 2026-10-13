---
term: "گروه مصرف‌کننده (Consumer group)"
term_en: "Consumer group"
aliases: ["consumer group", "گروه مصرف‌کننده", "کانسیومر گروپ"]
category: streaming
short: "مجموعه‌ای از consumerها که پارتیشن‌های یک topic را بین خود تقسیم می‌کنند؛ هر پارتیشن هم‌زمان فقط توسط یک عضو خوانده می‌شود."
related: ["consumer-lag", "partition", "backpressure"]
---
## به زبان ساده (مبتدی)
یک پیتزافروشی با چند پیک را تصور کنید. سفارش‌ها بر اساس محله دسته‌بندی می‌شوند و هر محله در هر لحظه فقط دست یک پیک است. اگر پیکی برود یا پیک جدیدی بیاید محله‌ها دوباره تقسیم می‌شوند. consumer group همین‌طور است: اعضای گروه کار خواندن یک topic را بین خود تقسیم می‌کنند.

## نحوه‌ی کار (متوسط)
همه‌ی consumerهای با `group.id` یکسان یک گروه می‌سازند. Kafka هر پارتیشن را دقیقاً به یک عضو می‌دهد؛ پس حداکثر موازی‌سازی برابر تعداد پارتیشن‌هاست (consumerهای اضافه بیکار می‌مانند). گروه offsetهای commit‌شده را به‌ازای هر پارتیشن نگه می‌دارد و بعد از restart از جای قبلی ادامه می‌دهد. گروه‌های مختلف topic را مستقل و با offset خودشان می‌خوانند؛ به همین شکل چند سرویس یک رویداد را مصرف می‌کنند.

```
group.id=billing-service          # same id = same group
enable.auto.commit=false          # commit after processing
partition.assignment.strategy=CooperativeStickyAssignor
session.timeout.ms=45000
max.poll.interval.ms=300000
```

وقتی عضوی اضافه، خارج یا crash شود (از دست رفتن heartbeat یا عبور از `max.poll.interval.ms`)، group coordinator یک rebalance راه می‌اندازد و پارتیشن‌ها را دوباره تخصیص می‌دهد. ترتیب فقط درون هر پارتیشن تضمین می‌شود.

## نگاه سنیور و نکات مصاحبه
rebalance مصرف را متوقف می‌کند؛ rebalanceهای مکرر (پردازش کندتر از poll interval، podهای ناپایدار، autoscaling) پرش lag می‌سازند. راه‌حل: cooperative rebalancing، عضویت ایستا (`group.instance.id`) و تنظیم timeoutها. استراتژی commit offset تضمین تحویل را تعیین می‌کند: commit بعد از پردازش یعنی at-least-once. پردازش باید خودتوان باشد چون rebalance می‌تواند دوباره تحویل بدهد. ترتیب مبتنی بر کلید به نگاشت پایدار کلید به پارتیشن نیاز دارد و افزودن پارتیشن آن را عوض می‌کند.

سؤال مصاحبه: «۱۲ پارتیشن و ۲۰ consumer در یک گروه داریم؛ چه می‌شود؟» پاسخ: فقط ۱۲ consumer پارتیشن می‌گیرند و ۸ تا بیکارند؛ برای scale بیشتر باید پارتیشن اضافه کرد که توزیع کلید و فرض‌های ترتیب را تغییر می‌دهد.
