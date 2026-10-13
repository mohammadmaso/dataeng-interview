---
term: "آفست (Offset)"
term_en: "Offset (Kafka)"
aliases: ["offset", "offsets", "آفست", "committed offset"]
category: streaming
short: "جایگاه ترتیبی یک رکورد در پارتیشن؛ consumerها offset را ثبت (commit) می‌کنند تا از همان‌جا ادامه دهند."
related: ["consumer-group", "consumer-lag", "exactly-once", "partition"]
---
## به زبان ساده (مبتدی)
offset مثل نشانه‌گذار در یک کتاب بسیار بلند است: صفحه‌ای را که رها کرده‌اید ثبت می‌کند. هر خواننده (consumer group) نشانه‌گذار خودش را دارد، پس بعد از استراحت از صفحه‌ی درست ادامه می‌دهد نه از ابتدا.

## نحوه‌ی کار (متوسط)
هر پارتیشن یک لاگ append-only است و هر رکورد offset صعودی یکنواخت می‌گیرد. offset فقط درون پارتیشن یکتاست (آدرس کامل: topic، پارتیشن، offset). consumer group آخرین offset ثبت‌شده را به‌ازای هر پارتیشن در `__consumer_offsets` (یا بیرونی) نگه می‌دارد. در restart یا rebalance از offset ثبت‌شده ادامه می‌دهد؛ پس زمان commit معنای تحویل را تعیین می‌کند.

```
partition 0:  [0][1][2][3][4][5][6][7]
                          ^committed=4   ^ current position=6   ^ log end=8
                          next fetch after restart: starts at 4 (re-reads 4,5 => at-least-once)

consumer.poll() -> process(records) -> consumer.commitSync()   # commit AFTER processing
enable.auto.commit=false
auto.offset.reset=earliest    # used only when no committed offset exists
```

commit قبل از پردازش at-most-once می‌دهد (crash رکورد را از دست می‌دهد)؛ commit بعد از پردازش at-least-once (crash باعث پردازش مجدد می‌شود)؛ commit اتمیک offset همراه خروجی (تراکنش Kafka یا ذخیره‌ی offset در دیتابیس sink) اثر exactly-once می‌دهد. consumer می‌تواند برای replay به offset یا timestamp دلخواه seek کند.

## نگاه سنیور و نکات مصاحبه
تله‌های رایج: `auto.offset.reset=latest` که برای گروه جدید داده را بی‌صدا رد می‌کند؛ commit کردن offset رکوردهای هنوز پردازش‌نشده (پردازش ناهمگام با auto-commit)؛ منقضی‌شدن offset وقتی گروه بیش از `offsets.retention.minutes` بیکار باشد؛ و عدم مقایسه‌پذیری offset بین پارتیشن‌ها یا topicها (ترتیب سراسری وجود ندارد). truncate شدن لاگ با retention می‌تواند offset ذخیره‌شده را خارج از بازه ببرد. به‌خاطر marker تراکنش و compaction در offsetها شکاف هست؛ پس پیوستگی را فرض نکنید و تفاوت offset را تعداد دقیق پیام ندانید.

سؤال مصاحبه: «۶ ساعت اخیر یک topic را چطور دوباره پردازش می‌کنی؟» پاسخ: consumer group را متوقف و offsetها را بر اساس timestamp ریست می‌کنم (`kafka-consumer-groups --reset-offsets --to-datetime`) یا group id جدید می‌گیرم و راه می‌اندازم؛ sink باید خودتوان باشد تا replay تکراری نسازد.
