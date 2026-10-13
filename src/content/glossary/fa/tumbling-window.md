---
term: "پنجره‌های Tumbling، Sliding و Session"
term_en: "Tumbling, sliding and session windows"
aliases: ["tumbling window", "sliding window", "hopping window", "پنجره‌ی زمانی", "windowing"]
category: streaming
short: "روش‌های برش جریان بی‌پایان به گروه‌های محدود زمانی: ثابت و بدون هم‌پوشانی، sliding هم‌پوشان یا session مبتنی بر فعالیت."
related: ["watermark", "event-time", "sessionization", "window-function"]
---
## به زبان ساده (مبتدی)
شمارش بازدیدکننده در هر ساعت مثل خواندن کنتور در هر ساعت کامل است (tumbling). نمایشگر «۱۰ دقیقه‌ی اخیر که هر دقیقه به‌روز می‌شود» پنجره‌ای است که می‌لغزد (sliding). گروه‌بندی کارهای یک نفر در «بازدیدها» که با مکث‌های ساکت جدا می‌شوند پنجره‌ی session است. پنجره‌ها جریان بی‌پایان را به تکه‌های قابل تجمیع تبدیل می‌کنند.

## نحوه‌ی کار (متوسط)
Tumbling: اندازه ثابت، بدون هم‌پوشانی و هر رویداد دقیقاً در یک پنجره (شمارش ساعتی). Sliding (hopping): اندازه ثابت با slide کوچک‌تر از اندازه، پس پنجره‌ها هم‌پوشان‌اند و هر رویداد در size/slide پنجره است (میانگین متحرک، نرخ خطای ۱۰ دقیقه‌ی اخیر). Session: پویا و بسته‌شده با gap بی‌فعالیتی به‌ازای هر کلید. پنجره‌ی global با trigger سفارشی هم ممکن است.

```
Tumbling 5 min:   |--00:00-00:05--|--00:05-00:10--|        each event in exactly 1 window
Sliding 10/5:     |-----00:00-00:10-----|                   size 10 min, slide 5 min:
                          |-----00:05-00:15-----|           each event in 2 windows
Session gap 30m:  |e e e|   gap   |e e e e|                 length varies per key

-- Flink SQL
SELECT window_start, COUNT(*) FROM TABLE(
  TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
GROUP BY window_start, window_end;
```

پنجره‌ها روی event time یا processing time تعریف می‌شوند؛ نتیجه وقتی منتشر می‌شود که واترمارک از انتهای پنجره (به‌علاوه‌ی allowed lateness) بگذرد، یا زودتر با trigger برای نتایج اولیه/به‌روزشونده. state هر پنجره‌ی باز تا بسته‌شدن نگه داشته می‌شود.

## نگاه سنیور و نکات مصاحبه
پنجره‌های sliding حالت و محاسبه را به اندازه‌ی size/slide ضرب می‌کنند؛ پنجره‌ی ۲۴ ساعته با slide یک دقیقه ۱۴۴۰ برابر کار برای هر رویداد است؛ از تجمیع افزایشی/مبتنی بر pane یا pre-aggregate در پنجره‌های tumbling و ترکیب آن‌ها استفاده کنید. معنای emit (فقط نهایی یا نتایج اولیه با retraction) را متناسب با مصرف‌کننده انتخاب کنید. هم‌ترازی پنجره و time zone (پنجره‌ی روزانه در ساعت محلی با DST) باگ کلاسیک است. رویدادهای دیررس پشت واترمارک بر اساس allowed lateness حذف یا اعمال می‌شوند. در SQL batch معادل‌ها `GROUP BY date_trunc(...)` و توابع پنجره‌ای با frame هستند.

سؤال مصاحبه: «نرخ خطای ۵ دقیقه‌ی اخیر که هر ۳۰ ثانیه به‌روز شود؟» پاسخ: پنجره‌ی sliding در event time به اندازه‌ی ۵ دقیقه با slide سی‌ثانیه، تجمیع افزایشی با واترمارک برای تأخیر؛ برای مهار هزینه، جزئی‌های tumbling سی‌ثانیه‌ای حساب و ۱۰ تای آخر را ادغام می‌کنم.
