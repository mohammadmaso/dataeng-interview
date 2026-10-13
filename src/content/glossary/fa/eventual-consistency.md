---
term: "سازگاری نهایی (Eventual consistency)"
term_en: "Eventual consistency"
aliases: ["eventual consistency", "سازگاری نهایی", "سازگاری قوی", "read-your-writes"]
category: distributed
short: "اگر نوشتن جدیدی نباشد همه‌ی replicaها در نهایت به یک مقدار می‌رسند، ولی در این بین خواندن ممکن است کهنه باشد."
related: ["cap-theorem", "replication", "quorum"]
---
## به زبان ساده (مبتدی)
عکسی منتشر می‌کنید، روی گوشی دیگر رفرش می‌کنید و هنوز نیست؛ یک دقیقه بعد ظاهر می‌شود. سرورهای مختلف تغییر را کمی با تأخیر می‌فهمند اما در نهایت همه توافق می‌کنند. این سازگاری نهایی است و بهای سرعت و دسترس‌پذیری است.

## نحوه‌ی کار (متوسط)
با replication ناهمگام نوشتن اول روی یک replica می‌نشیند و بعداً منتشر می‌شود. در این بازه خواندن از replicaهای دیگر مقدار قدیمی می‌دهد. «نهایی» یعنی همگرایی فقط وقتی تضمین است که به‌روزرسانی‌ها متوقف شوند و سیستم به تحویل ادامه دهد (با anti-entropy، read repair، gossip یا log shipping). تعارض نوشتن‌های هم‌زمان با timestamp از نوع LWW، version vector یا CRDT حل می‌شود.

```
Spectrum (strong -> weak):
linearizable        every read sees the latest write (single-copy illusion)
sequential          all see the same order of operations
causal              cause precedes effect everywhere
read-your-writes    you see your own writes
monotonic reads     never see data go backwards in time
eventual            replicas converge if writes stop
```

تضمین‌های قوی‌تر سطح session را می‌توان روی آن افزود: read-your-writes، monotonic reads و سازگاری causal اغلب برای اپ‌های رو به کاربر کافی‌اند. linearizability (سازگاری قوی) هماهنگی (quorum/consensus) می‌خواهد و در partition به قیمت latency و دسترس‌پذیری است.

## نگاه سنیور و نکات مصاحبه
سازگاری نهایی یک طیف است نه یک چیز؛ بگویید کدام تضمین را لازم دارید. LWW با اختلاف ساعت می‌تواند نوشتن را بی‌صدا دور بریزد؛ CRDT و تابع merge برای شمارنده و مجموعه امن‌ترند. در pipelineهای داده مصرف‌کننده‌ی جدول lake یا store replicate‌شده نمای eventually consistent می‌خواند: داشبوردها موقتاً ناهمخوان می‌شوند و join دو جدولی که زمان‌های متفاوت به‌روز شده‌اند ممکن است ناسازگار باشد. S3 امروز read-after-write قوی دارد ولی فرض‌های مبتنی بر list در storeهای دیگر ممکن است برقرار نباشد. به‌روزرسانی‌های خودتوان و جابه‌جاپذیر طراحی کنید تا ترتیب مهم نباشد.

سؤال مصاحبه: «کاربر پروفایل را به‌روز می‌کند و داده‌ی قدیمی می‌بیند؛ بدون سازگاری قوی در همه‌جا چطور حلش می‌کنی؟» پاسخ: read-your-writes: خواندن همان کاربر را برای مدت کوتاهی به leader (یا replicaای که به نسخه‌ی نوشتن او رسیده) می‌فرستم و برای بقیه سازگاری نهایی را نگه می‌دارم.
