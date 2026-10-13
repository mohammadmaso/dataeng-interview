---
term: "ساخت نشست (Sessionization)"
term_en: "Sessionization"
aliases: ["session window", "sessionization", "نشست کاربر", "پنجره‌ی نشست"]
category: streaming
short: "گروه‌بندی رویدادهای یک کاربر به نشست‌هایی که با یک وقفه‌ی بی‌فعالیتی (مثلاً ۳۰ دقیقه) از هم جدا می‌شوند."
related: ["window-function", "event-time", "watermark"]
---
## به زبان ساده (مبتدی)
وقتی کسی در یک سایت می‌گردد، کلیک می‌کند، برای ناهار می‌رود و برمی‌گردد، ما می‌خواهیم «بازدید» را بشماریم نه تک‌تک کلیک‌ها. اگر کاربر بیش از مثلاً ۳۰ دقیقه بی‌فعالیت باشد می‌گوییم آن بازدید تمام شد و کلیک بعدی بازدید جدید است. sessionization این قاعده را روی رویدادهای خام اعمال می‌کند.

## نحوه‌ی کار (متوسط)
نشست (session) بزرگ‌ترین دنباله از رویدادهای یک کلید است که فاصله‌ی رویدادهای متوالی در آن کمتر از gap باشد. برخلاف پنجره‌های ثابت (tumbling/sliding)، مرز پنجره‌ی نشست به داده بستگی دارد و طول آن برای هر کاربر فرق می‌کند.

```
WITH flagged AS (
  SELECT *, CASE WHEN ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts)
                      > INTERVAL '30 minutes' THEN 1 ELSE 0 END AS new_sess
  FROM events
), numbered AS (
  SELECT *, SUM(new_sess) OVER (PARTITION BY user_id ORDER BY ts
                                ROWS UNBOUNDED PRECEDING) AS sess_no
  FROM flagged
)
SELECT user_id, sess_no, MIN(ts) AS start_ts, MAX(ts) AS end_ts, COUNT(*) AS events
FROM numbered GROUP BY user_id, sess_no;
```

الگوی SQL همان «gaps and islands» است: وقتی فاصله با رویداد قبلی از آستانه بیشتر شد نشست جدید علامت بزن، سپس جمع تجمعی پرچم‌ها را شماره‌ی نشست بگیر. در موتورهای استریم پنجره‌ی نشست داخلی است (`EventTimeSessionWindows.withGap` در Flink، `session_window` در Spark): هر رویداد پنجره‌ی `[ts, ts+gap)` باز می‌کند و پنجره‌های هم‌پوشان ادغام می‌شوند.

## نگاه سنیور و نکات مصاحبه
جزئیات مهم: نشست‌ها در event time حساب می‌شوند پس رویداد دیررس می‌تواند دو نشست پایان‌یافته را ادغام کند (پیچیدگی state و retraction)؛ state هر کلید تا عبور واترمارک از `last_event + gap` نگه داشته می‌شود؛ نشست‌های بی‌کران (بات‌ها) سقف مدت می‌خواهند؛ gap تصمیم محصولی است پس قابل‌تنظیم و مستند باشد. در batch پارتیشن بر اساس کاربر یعنی کاربر سنگین ریسک skew است و هزینه‌ی sort غالب است. متریک‌های نشست (مدت، bounce، صفحه در نشست) به funnel و تحلیل retention می‌روند.

سؤال مصاحبه: «در SQL از جدول کلیک نشست بساز.» پاسخ: با LAG به‌ازای هر کاربر به ترتیب زمان gapهای بیش از ۳۰ دقیقه را علامت می‌زنم، جمع تجمعی پرچم‌ها را شناسه‌ی نشست می‌گیرم و بر اساس کاربر و نشست تجمیع می‌کنم؛ در استریم از پنجره‌ی نشست بومی با واترمارک.
