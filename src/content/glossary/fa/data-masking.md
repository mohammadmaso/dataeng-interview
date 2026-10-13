---
term: "پنهان‌سازی و ناشناس‌سازی داده‌ی شخصی (PII masking)"
term_en: "PII masking and anonymization"
aliases: ["pii", "data masking", "tokenization", "pseudonymization", "ناشناس‌سازی", "gdpr"]
category: quality
short: "محافظت از داده‌ی شخصی با mask، hash، توکن‌سازی یا تعمیم و پشتیبانی از درخواست‌های حذف در کل پلتفرم داده."
related: ["data-catalog", "data-lake", "data-quality"]
---
## به زبان ساده (مبتدی)
هنگام اشتراک‌گذاری فهرست کلاس ممکن است نام‌ها را سیاه یا با شماره‌ی دانش‌آموزی جایگزین کنید تا بتوان نتایج را تحلیل کرد بدون اینکه معلوم باشد چه کسی کیست. پنهان‌سازی داده‌ی شخصی همین کار را می‌کند: تحلیلگرها با داده کار می‌کنند و هویت حساس محافظت می‌شود.

## نحوه‌ی کار (متوسط)
PII (نام، ایمیل، تلفن، شناسه، مکان، IP) با قوانینی مثل GDPR تنظیم می‌شود. تکنیک‌ها: redaction/masking (پنهان‌کردن بخشی)، hashing (یک‌طرفه؛ از hash کلیددار یا HMAC با راز استفاده کنید چون hash ساده‌ی مقادیر کم‌آنتروپی مثل شماره تلفن با brute force برگشت‌پذیر است)، tokenization (جایگزینی با توکن و نگاشت در vault امن)، رمزنگاری (سطح فیلد یا at-rest)، تعمیم/k-anonymity (کاهش دقت) و differential privacy برای aggregate. داده‌ی pseudonymized هنوز داده‌ی شخصی است؛ فقط داده‌ی واقعاً ناشناس قابل بازشناسی نیست.

```
-- Pseudonymize: stable token, joins still work, raw value is not exposed
SELECT sha256(concat(email, :secret_salt)) AS email_token FROM users;     -- keyed hash / HMAC
-- Mask for analysts
SELECT regexp_replace(phone, '\d(?=\d{4})', '*') AS phone_masked FROM users;
-- Generalize
SELECT date_trunc('year', birth_date) AS birth_year, left(zip, 3) AS zip3 FROM users;
-- Column policy (warehouse): analysts see masked, privileged roles see raw
```

کنترل‌های پلتفرم: طبقه‌بندی و tag کردن PII در کاتالوگ، سیاست دسترسی سطح ستون و dynamic masking بر اساس نقش، رمزنگاری in transit و at rest، audit log و نگهداری PII خام در zone محدود با دریافت توکن در لایه‌های پایین‌دست.

## نگاه سنیور و نکات مصاحبه
حق حذف (right to erasure) بخش سخت مهندسی داده است: داده در raw، silver، gold، backup و featureهای ML و در فایل‌های تغییرناپذیر و لاگ‌های Kafka کپی شده است. راهبردها: PII را در جدول کوچک جدا با کلید جانشین نگه دارید تا حذف یک سطر پیوند را قطع کند (crypto-shredding: حذف کلید رمزنگاری یک فرد)، jobهای حذف با `DELETE` در Iceberg/Delta به‌علاوه‌ی compaction و snapshot expiry تا فایل‌های قدیمی فیزیکی حذف شوند، و ردیابی lineage شناسه‌ی فرد. joinهای روی توکن باید بین سیستم‌ها سازگار باشد (salt/کلید یکسان) و چرخش کلید برنامه می‌خواهد. لاگ و خروجی دیباگ نقاط نشت رایج‌اند.

سؤال مصاحبه: «درخواست حذف GDPR را در lakehouse چطور اجرا می‌کنی؟» پاسخ: فرد را با شناسه‌ی پایدارش پیدا می‌کنم، سطرها را در همه‌ی جدول‌ها حذف یا tombstone می‌کنم (ترجیحاً PII در یک جدول ایزوله یا crypto-shred)، سپس compaction و snapshot expiry تا فایل‌های قدیمی حاوی داده فیزیکی حذف شوند و backupها و کپی‌های پایین‌دست را راستی‌آزمایی می‌کنم.
