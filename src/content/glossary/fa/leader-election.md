---
term: "انتخاب رهبر"
term_en: "Leader election"
aliases: ["leader election","انتخاب leader"]
category: distributed
short: "انتخاب یک node برای هماهنگی یا پذیرش نوشتن، و جایگزینی آن هنگام خرابی."
related: ["consensus","replication","quorum"]
---
## به زبان ساده (مبتدی)
گروه یک کاپیتان انتخاب می‌کند تا تصمیم‌ها دوبار گرفته نشود؛ اگر کاپیتان بیمار شد، رأی می‌دهند و یکی تازه انتخاب می‌کنند.

## نحوه‌ی کار (متوسط)
nodeها با پروتکل consensus (Raft، Paxos، قفل ZooKeeper/etcd) روی یک leader توافق می‌کنند، معمولاً با term یا epoch. followerها از leader replicate می‌کنند؛ خرابی با heartbeat و timeout تشخیص داده می‌شود و انتخابات تازه برگزار می‌شود.

## نگاه سنیور و نکات مصاحبه
خطر: دو leader هم‌زمان (split brain) اگر timeout و fencing غلط باشد. از fencing token استفاده کنید تا leader کهنه نتواند بنویسد. انتخابات یک ناپایداری (unavailability) کوتاه می‌سازد و timeout سرعت تشخیص را با false positive معاوضه می‌کند.

سؤال مصاحبه: «چطور جلوی نوشتن دو leader را می‌گیری؟» پاسخ: quorum اکثریت به‌علاوه‌ی شماره‌ی epoch و fencing token.
