---
term: "آپاچی Avro"
term_en: "Apache Avro"
aliases: ["avro","فرمت avro"]
category: storage
short: "فرمت دودویی فشرده‌ی سطری با schema تعبیه‌شده، پرکاربرد در Kafka."
related: ["schema-registry","schema-evolution","row-oriented-storage","parquet"]
---
## به زبان ساده (مبتدی)
یک جعبه‌ی ارسال با فهرست محتویات چسبیده روی آن: داده فشرده بسته شده و schema به هر کسی می‌گوید چطور بازش کند.

## نحوه‌ی کار (متوسط)
Avro رکوردها را با schema تعریف‌شده به JSON به‌صورت دودویی ذخیره می‌کند. فایل‌ها schema را تعبیه می‌کنند؛ پیام‌های Kafka معمولاً شناسه‌ی schema را حمل می‌کنند که با schema registry حل می‌شود. از تکامل schema غنی با reader و writer schema، مقدار پیش‌فرض و alias پشتیبانی می‌کند.

## نگاه سنیور و نکات مصاحبه
برای رویداد استریم Avro (یا Protobuf) و برای ذخیره‌ی تحلیلی Parquet/ORC. قواعد سازگاری (backward، forward، full) باید در CI با registry اعمال شوند. افزودن فیلد بدون پیش‌فرض خواننده‌های قدیمی را می‌شکند.

سؤال مصاحبه: «Avro در برابر Parquet؟» پاسخ: Avro سطری و مناسب نوشتن زیاد و استریم رکورد به رکورد است؛ Parquet ستونی و مناسب تحلیل پراسکن.
