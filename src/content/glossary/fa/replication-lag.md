---
term: "Replication lag"
term_en: "Replication lag"
aliases: ["replication lag","تأخیر replication","read-your-writes"]
category: distributed
short: "تأخیر بین نوشتن روی primary و دیده‌شدن آن روی replica."
related: ["replication","eventual-consistency","cdc"]
---
## به زبان ساده (مبتدی)
عکسی منتشر می‌کنی، از دستگاه دیگر تازه می‌کنی و هنوز نیست: کپی هنوز نرسیده است.

## نحوه‌ی کار (متوسط)
replicaهای ناهمگام لاگ primary را بعداً اعمال می‌کنند، پس خواندن از آن‌ها ممکن است کهنه باشد. lag با نوشتن سنگین، queryهای طولانی که apply را مسدود می‌کنند یا مشکل شبکه زیاد می‌شود. lag را به ثانیه یا بایت پایش کنید.

## نگاه سنیور و نکات مصاحبه
تضمین‌هایی که باید طراحی شوند: read-your-writes (خواندن‌های کاربر را کمی بعد از نوشتن به primary بفرستید)، monotonic reads (نشست را به یک replica بچسبانید) و consistent prefix. مصرف‌کننده‌های CDC که از replica می‌خوانند lag را به ارث می‌برند.

سؤال مصاحبه: «کاربر پروفایل را به‌روز می‌کند ولی قدیمی را می‌بیند. راه‌حل؟» پاسخ: مسیریابی read-your-writes یا چک نسخه/timestamp نسبت به replica.
