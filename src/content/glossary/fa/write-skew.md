---
term: "Write skew"
term_en: "Write skew"
aliases: ["write skew","انحراف نوشتن"]
category: distributed
short: "ناهنجاری‌ای که در آن دو تراکنش هم‌زمان داده‌ی هم‌پوشان را می‌خوانند و ردیف‌های متفاوت می‌نویسند و یک قید را می‌شکنند."
related: ["isolation-level","mvcc","database-transaction"]
---
## به زبان ساده (مبتدی)
دو پزشک کشیک که هرکدام می‌بینند دیگری سر کار است و هر دو هم‌زمان مرخصی می‌گیرند و کسی کشیک نمی‌ماند.

## نحوه‌ی کار (متوسط)
در snapshot isolation هر تراکنش snapshot سازگار را می‌بیند و شرطی برقرار را چک می‌کند، سپس ردیف متفاوتی می‌نویسد؛ تعارض write-write تشخیص داده نمی‌شود اما نتیجه‌ی ترکیبی قاعده را نقض می‌کند. فقط isolation سطح serializable (یا قفل صریح مثل SELECT ... FOR UPDATE) جلوگیری می‌کند.

## نگاه سنیور و نکات مصاحبه
راه‌حل: isolation سطح serializable، قفل ردیف‌های خوانده‌شده، تبدیل تعارض به یک ردیف یا افزودن constraint. مثال کلاسیک نشان‌دهنده‌ی این است که snapshot isolation معادل serializable نیست.

سؤال مصاحبه: «یک ناهنجاری که snapshot isolation اجازه می‌دهد؟» پاسخ: write skew مثل رزرو دوگانه‌ی کشیک.
