---
term: "انبار داده (Data warehouse)"
term_en: "Data warehouse"
aliases: ["data warehouse", "انبار داده", "dwh"]
category: modeling
short: "مخزن مرکزی، مدل‌شده و governed از داده‌ی تاریخی یکپارچه که برای تحلیل و گزارش‌گیری بهینه شده است (schema-on-write)."
related: ["olap", "etl-elt", "star-schema", "data-lake"]
---
## به زبان ساده (مبتدی)
data warehouse کتابخانه‌ی منظم حقایق شرکت است: داده از سیستم‌های مختلف پاک، تطبیق و به شکل یکسان ذخیره می‌شود تا هرکس بپرسد «فروش فصل گذشته چقدر بود؟» پاسخ یکسان و قابل‌اعتماد بگیرد، مستقل از اینکه داده از کدام سیستم آمده.

## نحوه‌ی کار (متوسط)
Inmon آن را موضوع‌محور، یکپارچه، وابسته به زمان و غیرفرار تعریف کرد. داده از سیستم‌های عملیاتی استخراج، به مدل مشترک (ابعادی/star یا نرمال یا Data Vault) تبدیل و با schema-on-write بارگذاری می‌شود؛ پس کیفیت و ساختار هنگام بارگذاری اعمال می‌شود. با SQL روی موتورهای MPP ستونی (Snowflake، BigQuery، Redshift، Synapse) که ذخیره و محاسبه را جدا یا جفت می‌کنند سرو می‌شود.

```
OLTP DBs, SaaS, logs
      │  ETL / ELT / CDC
      ▼
 staging ─► core (integrated, history) ─► marts (star schemas) ─► BI tools
                  ▲
          conformed dimensions, business definitions, access control
```

لایه‌ها: staging (کپی خام)، integration/core (پاک‌سازی و تاریخچه‌دار) و martها (starهای موضوعی). conformed dimension و semantic layer برای هر متریک یک تعریف واحد می‌دهند. رویکردهای طراحی: Kimball (martهای ابعادی، پایین‌به‌بالا)، Inmon (اول EDW نرمال، بالا‌به‌پایین)، Data Vault (hub، link و satellite برای یکپارچه‌سازی قابل‌ممیزی).

## نگاه سنیور و نکات مصاحبه
warehouse در تحلیل SQL با governance و کارایی و همزمانی قوی عالی است، اما نوع کلاسیک آن برای داده‌ی خام/بدون ساختار گران است، به فرمت vendor وابسته است و چون مدل‌سازی پیشاپیش انجام می‌شود دیرتر تغییر می‌کند. warehouseهای ابری مدرن ذخیره و محاسبه را جدا می‌کنند و داده‌ی نیمه‌ساخت‌یافته و ELT را پشتیبانی می‌کنند و فاصله با lakehouse کم شده. دغدغه‌ها: کنترل هزینه (query pruning، clustering، جداسازی workload)، تاریخچه‌ی کند‌تغییر، سازگاری متریک و سیاست دسترسی PII. Reverse ETL داده‌ی warehouse را به ابزارهای عملیاتی برمی‌گرداند.

سؤال مصاحبه: «کِی هنوز warehouse را به lake ترجیح می‌دهی؟» پاسخ: وقتی بار اصلی SQL/BI تحت governance با کاربران هم‌زمان زیاد و سازگاری سخت متریک است و کارایی و سادگی موتور مدیریت‌شده بر باز بودن فرمت می‌چربد؛ در غیر این صورت lakehouse برای چندموتوری و ML.
