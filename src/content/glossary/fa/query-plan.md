---
term: "برنامه‌ی اجرای query"
term_en: "Query plan"
aliases: ["query plan","execution plan","explain","پلن اجرا"]
category: sql
short: "راهبرد گام‌به‌گامی که موتور برای اجرای یک query انتخاب می‌کند."
related: ["cost-based-optimizer","predicate-pushdown","database-index"]
---
## به زبان ساده (مبتدی)
مسیری که GPS انتخاب می‌کند: مبدأ و مقصد یکی است، اما موتور تصمیم می‌گیرد کدام جاده‌ها (scan، index، ترتیب join) ارزان‌ترند.

## نحوه‌ی کار (متوسط)
بهینه‌ساز SQL را به plan منطقی تبدیل می‌کند، بازنویسی‌ها (predicate pushdown، حذف ستون‌ها، تغییر ترتیب join) را اعمال می‌کند و با آمار یک plan فیزیکی انتخاب می‌کند. EXPLAIN آن را نشان می‌دهد و EXPLAIN ANALYZE تعداد ردیف و زمان واقعی را هم.

## نگاه سنیور و نکات مصاحبه
در plan دنبال این‌ها باشید: scan کامل جایی که باید index استفاده شود، اختلاف تخمین و واقعیت ردیف‌ها (آمار کهنه)، عملگرهای shuffle/exchange، ترتیب و نوع join و spill به دیسک. با آمار، index، بازنویسی شرط‌ها و در نهایت hint درست کنید.

سؤال مصاحبه: «query ناگهان کند شد. اولین قدم؟» پاسخ: planهای EXPLAIN ANALYZE را مقایسه می‌کنم، دنبال تغییر نوع join یا تخمین بد cardinality می‌گردم و آمار را تازه می‌کنم.
