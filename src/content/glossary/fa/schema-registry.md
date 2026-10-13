---
term: "رجیستری schema (Schema registry)"
term_en: "Schema registry"
aliases: ["schema registry", "رجیستری schema", "avro", "protobuf"]
category: ingestion
short: "سرویس مرکزی که schemaهای نسخه‌دار پیام‌ها را نگه می‌دارد و قواعد سازگاری بین تولیدکننده و مصرف‌کننده را اعمال می‌کند."
related: ["schema-evolution", "data-contract", "parquet"]
---
## به زبان ساده (مبتدی)
schema registry مثل یک دیکشنری مشترک در میز اطلاعات است. به‌جای اینکه هر پیام تعریف کاملش را حمل کند، فقط یک شماره‌ی ارجاع کوتاه دارد و هر که لازم دارد تعریف را از آنجا می‌خواند. میز اطلاعات تعریف‌های جدیدی را که خواننده‌های فعلی را گیج کند نمی‌پذیرد.

## نحوه‌ی کار (متوسط)
پیام‌های Kafka بایت هستند و تولیدکننده و مصرف‌کننده باید روی ساختار توافق کنند. فرمت‌های باینری (Avro، Protobuf، JSON Schema) وقتی schema درونشان نباشد فشرده‌اند. registry به هر schema یک id سراسری می‌دهد و نسخه‌ها را زیر یک subject (معمولاً به‌ازای هر topic) گروه می‌کند. producer شناسه را در پیام می‌گذارد؛ consumer schema را با id می‌گیرد و cache می‌کند. schema خواننده و نویسنده می‌توانند متفاوت باشند و با قواعد سازگاری resolve می‌شوند.

```
Message on the wire: [magic byte][schema id (4 bytes)][avro/protobuf payload]

producer: serialize(record) -> register/lookup schema -> get id -> send id + bytes
consumer: read id -> fetch schema (cached) -> deserialize with writer's schema

Subject: orders-value, versions v1, v2, v3 ...  compatibility: BACKWARD | FORWARD | FULL | NONE
POST /subjects/orders-value/versions   -> rejected (409) if it breaks the compatibility mode
```

registry هنگام ثبت نسخه‌ی جدید حالت سازگاری را اعمال می‌کند و دروازه‌ی تکامل امن است: فیلد با مقدار پیش‌فرض اضافه کنید، نوع را عوض یا فیلد الزامی را حذف نکنید. می‌تواند در CI اجرا شود تا قبل از deploy تغییر شکننده‌ی producer مسدود شود.

## نگاه سنیور و نکات مصاحبه
این زیرساخت حیاتی می‌شود: اگر registry down باشد producerهای جدید نمی‌توانند schema ثبت کنند (consumerها از cache استفاده می‌کنند)؛ پس highly available اجرایش کنید. Avro، Protobuf یا JSON Schema را بر اساس ابزار و قواعد تکامل انتخاب کنید (شماره‌ی فیلد در Protobuf، پیش‌فرض و union در Avro). راهبرد نام‌گذاری subject (topic-name یا record-name) روی چند نوع رویداد در یک topic اثر دارد. حالت‌های `TRANSITIVE` همه‌ی نسخه‌های قبلی را بررسی می‌کنند که برای topicهای طولانی‌عمر با replay مهم است. registry ساختار را اعمال می‌کند نه معنا؛ برای معنا، مالکیت و کیفیت با data contract جفتش کنید. به sinkها (Kafka Connect به Iceberg) هم در نگاشت خودکار schema کمک می‌کند.

سؤال مصاحبه: «چطور جلوی خراب‌شدن consumerها با deploy یک producer را می‌گیری؟» پاسخ: schema registry با سازگاری backward (یا full) که در CI/build تولیدکننده اعمال می‌شود تا schema ناسازگار ثبت نشود؛ برای تغییر backward اول consumerها rollout می‌شوند و برای حذف دوره‌ی deprecation می‌گذارم.
