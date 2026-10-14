---
term: "انواع join در SQL"
term_en: "SQL join types"
aliases: ["join","inner join","left join","outer join","انواع join"]
category: sql
short: "روش‌های ترکیب ردیف‌های دو جدول: inner، left، right، full outer، cross، semi و anti."
related: ["sort-merge-join","broadcast-join","cardinality"]
---
## به زبان ساده (مبتدی)
تطبیق دو لیست مهمان. inner فقط نام‌های مشترک را نگه می‌دارد؛ left همه‌ی لیست اول را نگه می‌دارد و برای ناهمخوان‌ها جای خالی می‌گذارد؛ full همه را.

## نحوه‌ی کار (متوسط)
INNER تطابق‌ها را برمی‌گرداند؛ LEFT/RIGHT همه‌ی ردیف‌های یک سمت را با NULL برای نبود تطابق نگه می‌دارند؛ FULL OUTER هر دو را؛ CROSS ضرب دکارتی است؛ SEMI (EXISTS) یک سمت را بر اساس وجود در سمت دیگر فیلتر می‌کند بدون تکثیر؛ ANTI (NOT EXISTS) ردیف‌های بدون تطابق را نگه می‌دارد. الگوریتم‌های فیزیکی: hash، sort-merge، nested loop و broadcast.

## نگاه سنیور و نکات مصاحبه
باگ‌های کلاسیک: join fan-out (تکثیر ردیف وقتی کلید در یک سمت یکتا نیست)، فیلتر WHERE روی سمت nullable که LEFT را inner می‌کند، و کلید NULL که هرگز match نمی‌شود. همیشه grain هر دو ورودی را بدانید و تعداد ردیف‌ها را اعتبارسنجی کنید.

سؤال مصاحبه: «چرا بعد از join درآمد دو برابر شد؟» پاسخ: سمت راست چند ردیف برای هر کلید داشت (fan-out)؛ اول aggregate کن یا در grain درست join بزن.
