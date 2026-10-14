---
term: "Degenerate dimension"
term_en: "Degenerate dimension"
aliases: ["degenerate dimension","بعد منحط"]
category: modeling
short: "مقدار dimension که مستقیماً در جدول fact ذخیره می‌شود و جدول مستقل ندارد."
related: ["fact-table","dimension-table","grain"]
---
## به زبان ساده (مبتدی)
شماره‌ی سفارشی که روی هر آیتم چاپ شده: ردیف‌ها را گروه می‌کند اما اطلاعات اضافه‌ای برای یک جدول جدا ندارد.

## نحوه‌ی کار (متوسط)
شناسه‌هایی مثل شماره سفارش، شماره فاکتور یا transaction id به‌عنوان ستون در fact می‌مانند. برای گروه‌کردن آیتم‌ها و drill-through مفیدند ولی ویژگی دیگری ندارند، پس dimension جدا خالی می‌ماند.

## نگاه سنیور و نکات مصاحبه
در factهای با grain تراکنش رایج است. اگر بعداً ویژگی پیدا کرد (وضعیت سفارش، کانال) آن را به dimension واقعی ارتقا دهید. فراموش نکنید cardinality بالایی دارد، پس به‌عنوان کلید partition استفاده نشود.

سؤال مصاحبه: «order_id را در star schema کجا می‌گذاری؟» پاسخ: به‌عنوان degenerate dimension روی fact سطر سفارش.
