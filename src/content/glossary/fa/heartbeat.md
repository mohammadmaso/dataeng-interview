---
term: "Heartbeat"
term_en: "Heartbeat"
aliases: ["heartbeat","ضربان","failure detector"]
category: distributed
short: "سیگنال دوره‌ای که یک node می‌فرستد تا ثابت کند زنده است."
related: ["leader-election","consensus","split-brain"]
---
## به زبان ساده (مبتدی)
مثل نبض: اگر مدت زیادی قطع شود، بقیه فرض می‌کنند node از کار افتاده.

## نحوه‌ی کار (متوسط)
nodeها در فواصل منظم heartbeat می‌فرستند؛ پایشگر یا همتاها پس از timeout از دست رفتن ضربان‌ها خرابی را اعلام می‌کنند. در brokerها و مصرف‌کننده‌های Kafka (session timeout)، leader در Raft، YARN و Kubernetes استفاده می‌شود.

## نگاه سنیور و نکات مصاحبه
تشخیص خرابی در شبکه‌ی ناهمگام احتمالی است: node کند مرده به نظر می‌رسد. timeout کوتاه یعنی failover سریع اما هشدار کاذب (و rebalance)؛ بلند یعنی بازیابی کند. تشخیص‌دهنده‌های phi-accrual با واریانس تطبیق می‌یابند.

سؤال مصاحبه: «چرا GC pause باعث rebalance در Kafka می‌شود؟» پاسخ: مصرف‌کننده بیش از session timeout heartbeat را از دست داد و مرده اعلام شد.
