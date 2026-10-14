---
term: "بازتوزیع پارتیشن (rebalancing)"
term_en: "Rebalancing"
aliases: ["rebalancing","rebalance","ریبالانس"]
category: distributed
short: "توزیع مجدد پارتیشن‌ها یا shardها بین workerها وقتی عضویت تغییر می‌کند."
related: ["consumer-group","partition","consumer-lag"]
---
## به زبان ساده (مبتدی)
وقتی صندوق‌دار جدیدی می‌آید یا یکی می‌رود، صف‌ها دوباره تقسیم می‌شوند تا کار عادلانه بماند. در این جابه‌جایی سرویس مدتی کوتاه می‌ایستد.

## نحوه‌ی کار (متوسط)
در Kafka ورود، خروج یا crash در یک consumer group باعث rebalance و تخصیص مجدد پارتیشن‌ها می‌شود؛ روش کلاسیک «stop the world» همه‌ی مصرف‌کننده‌ها را متوقف می‌کند، اما rebalance تعاونی/افزایشی فقط پارتیشن‌های متأثر را جابه‌جا می‌کند. storeهای توزیع‌شده با افزودن node داده را rebalance می‌کنند.

## نگاه سنیور و نکات مصاحبه
rebalanceهای مکرر (flapping به‌خاطر پردازش کند بیش از max.poll.interval، session timeout بد، deployهای rolling) جهش lag می‌سازند. راه‌حل: static membership، پروتکل cooperative و کوتاه نگه‌داشتن poll loop. rebalance داده بایت جابه‌جا می‌کند و throttle می‌خواهد.

سؤال مصاحبه: «بعد از هر deploy مصرف‌کننده lag می‌گیرد، چرا؟» پاسخ: restart رولینگ rebalance راه می‌اندازد؛ از static membership و rebalance تعاونی استفاده کن.
