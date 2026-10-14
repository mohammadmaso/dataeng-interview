---
term: "اسکیمای Snowflake"
term_en: "Snowflake schema"
aliases: ["snowflake schema","اسکیمای دانه‌برفی"]
category: modeling
short: "یک star schema که dimensionهایش بیشتر به زیرجدول نرمال شده‌اند."
related: ["star-schema","dimension-table","normalization"]
---
## به زبان ساده (مبتدی)
ستاره‌ای که هر نوکش به نوک‌های کوچک‌تر شاخه می‌زند: محصول به دسته، آن به بخش.

## نحوه‌ی کار (متوسط)
جدول‌های dimension به سلسله‌مراتب شکسته می‌شوند (محصول ← زیردسته ← دسته ← بخش) تا افزونگی حذف شود. فضا کمتر مصرف می‌شود و تغییر ویژگی در یک جا انجام می‌شود، به قیمت joinهای بیشتر و مدل پیچیده‌تر.

## نگاه سنیور و نکات مصاحبه
ذخیره‌ی ارزان و joinهای ستونی سریع باعث می‌شود star معمولاً برای سادگی BI برنده شود؛ snowflaking برای dimensionهای خیلی بزرگ با زیر-dimension مشترک یا governance سخت‌گیرانه توجیه دارد. ربطی به محصول دیتابیس Snowflake ندارد که سردرگمی کلاسیک مصاحبه است.

سؤال مصاحبه: «star یا snowflake برای data mart؟» پاسخ: star برای سادگی و سرعت؛ snowflake فقط وقتی افزونگی dimension واقعاً گران است.
