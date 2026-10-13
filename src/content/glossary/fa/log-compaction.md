---
term: "فشرده‌سازی لاگ (Log compaction)"
term_en: "Log compaction (Kafka)"
aliases: ["log compaction", "compacted topic", "changelog topic", "cleanup.policy=compact"]
category: streaming
short: "حالت retention در Kafka که دست‌کم آخرین رکورد هر کلید را نگه می‌دارد و topic را به changelog پایدار یا snapshot جدول تبدیل می‌کند."
related: ["tombstone", "compaction", "consumer-group"]
---
## به زبان ساده (مبتدی)
فهرست مخاطبین گوشی فقط آخرین شماره‌ی هر دوست را نگه می‌دارد نه همه‌ی شماره‌هایی که تا حالا داشته. topic فشرده‌شده در Kafka هم همین‌طور است: به‌جای منقضی کردن پیام‌های قدیمی با زمان، آخرین پیام هر کلید را نگه می‌دارد تا همیشه بتوانید state فعلی را بازسازی کنید.

## نحوه‌ی کار (متوسط)
با `cleanup.policy=delete` سگمنت‌ها بعد از حد زمان یا اندازه حذف می‌شوند. با `compact` یک cleaner پس‌زمینه رکوردهای قدیمی‌تر همان کلید را از سگمنت‌های بسته حذف می‌کند و آخرین مقدار را نگه می‌دارد؛ رکورد با مقدار null یک tombstone است که در نهایت کلید را حذف می‌کند. offsetها تغییر نمی‌کنند و ترتیب حفظ می‌شود؛ سگمنت فعال compact نمی‌شود.

```
kafka-topics.sh --create --topic user-profiles --partitions 6 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.5 \
  --config delete.retention.ms=86400000

offset: 0   1   2   3   4   5   6
key:    A   B   A   C   B   A   C        before: A@0 B@1 A@2 C@3 B@4 A@5 C@6
value:  v1  v1  v2  v1  v2  v3  null     after:  B@4 A@5 (C@6 tombstone, later removed)
```

کاربردها: changelog جدول دیتابیس (CDC به topic فشرده)، changelog مربوط به state-store در Kafka Streams، offsetهای consumer (`__consumer_offsets`) و داده‌ی مرجع/پیکربندی که consumer جدید از ابتدا می‌خواند تا snapshot کامل را بازسازی کند (KTable).

## نگاه سنیور و نکات مصاحبه
compaction فوری نیست و حذف تکراری‌ها را تضمین نمی‌کند: تا اجرای cleaning خواننده‌ها هنوز چند رکورد برای یک کلید می‌بینند پس consumer باید همچنان last-write-wins اعمال کند. tombstoneها به مدت `delete.retention.ms` می‌مانند تا consumerهای کند حذف را ببینند. پیام‌ها کلید می‌خواهند (رکورد بدون کلید compact نمی‌شود). compaction آخرین مقدار کل فضای کلید را نگه می‌دارد پس اندازه‌ی topic با تعداد کلیدهای متمایز مقیاس می‌گیرد نه تعداد رویداد. تاریخچه نمی‌دهد؛ برای محدودکردن زمانی هم از `compact,delete` استفاده کنید. راه‌اندازی consumer از یک topic فشرده‌ی بزرگ زمان‌بر است.

سؤال مصاحبه: «چطور سرویس جدید جدول مشتری فعلی را از Kafka بازسازی کند؟» پاسخ: تغییرات مشتری را با کلید id در topic فشرده با tombstone برای حذف منتشر می‌کنم؛ سرویس جدید از earliest offset تا انتها می‌خواند و last-write-wins اعمال می‌کند و سپس به استریم به‌روزرسانی‌ها ادامه می‌دهد.
