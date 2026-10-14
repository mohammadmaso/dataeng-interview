---
term: "تراکنش دیتابیس"
term_en: "Database transaction"
aliases: ["transaction","تراکنش","commit","rollback"]
category: distributed
short: "گروهی از عملیات که یک واحد همه‌یا‌هیچ حساب می‌شوند."
related: ["acid","isolation-level","wal","two-phase-commit"]
---
## به زبان ساده (مبتدی)
انتقال پول بین حساب‌ها: برداشت و واریز با هم انجام می‌شوند یا هیچ‌کدام.

## نحوه‌ی کار (متوسط)
BEGIN آن را شروع می‌کند، COMMIT تغییرات را دائمی می‌کند و ROLLBACK دورشان می‌ریزد. دیتابیس برای atomicity و isolation از لاگ و lock یا MVCC استفاده می‌کند. تراکنش‌های طولانی منابع را نگه می‌دارند، undo log را پف می‌کنند و پاکسازی را مسدود می‌کنند.

## نگاه سنیور و نکات مصاحبه
تراکنش‌ها را کوتاه نگه دارید، زمان فکر کاربر را داخلشان نیاورید، روی خطای serialization و deadlock retry کنید و retry را idempotent بسازید. بین سرویس‌ها به‌جای تراکنش توزیع‌شده از saga یا الگوی outbox استفاده کنید.

سؤال مصاحبه: «چرا تراکنش‌های طولانی مضرند؟» پاسخ: lock/snapshot نگه می‌دارند، vacuum و compaction را مسدود می‌کنند و نرخ conflict را بالا می‌برند.
