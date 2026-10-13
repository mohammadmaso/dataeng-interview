---
term: "جدول بُعد (Dimension table)"
term_en: "Dimension table"
aliases: ["dimension table", "جدول dimension", "جدول بُعد", "dimension"]
category: modeling
short: "جدول مدل ابعادی با ویژگی‌های توصیفی (چه کسی، چه چیزی، کجا، کِی) برای فیلتر، گروه‌بندی و برچسب‌زدن factها."
related: ["fact-table", "star-schema", "scd", "surrogate-key"]
---
## به زبان ساده (مبتدی)
اگر fact دفتر خاطرات رویدادهاست، dimensionها کارت‌های مرجعی‌اند که کلمات آن را توضیح می‌دهند: این محصول چیست، این مشتری کیست، آن تاریخ چه روزی از هفته بود. از آن‌ها برای فیلتر و گروه‌بندی عددها استفاده می‌کنید، مثلاً «مجموع فروش دسته‌ی لوازم الکترونیکی در بهار».

## نحوه‌ی کار (متوسط)
dimensionها کلید اصلی جانشین، کلید طبیعی/کسب‌وکار و ستون‌های توصیفی فراوان دارند، اغلب با سلسله‌مراتب‌های تخت‌شده در یک سطر (محصول، برند، دسته). نسبتاً کوچک و پهن‌اند و برای پرهیز از join اضافه denormalize می‌شوند. برچسب‌ها و ویژگی‌های فیلتر و group-by گزارش‌ها را تأمین می‌کنند.

```
CREATE TABLE dim_product (
  product_sk   BIGINT PRIMARY KEY,
  product_id   VARCHAR,        -- natural key from source
  name         VARCHAR,
  category     VARCHAR,        -- denormalized hierarchy
  brand        VARCHAR,
  valid_from   DATE, valid_to DATE, is_current BOOLEAN   -- SCD2
);
-- special rows: product_sk = -1 -> 'Unknown'
```

انواع مهم: dimension تاریخ (از پیش ساخته‌شده با دوره‌های مالی، تعطیلات و پرچم روز هفته)، conformed dimension مشترک بین چند fact، role-playing dimension (یک جدول تاریخ برای تاریخ سفارش و ارسال)، junk dimension (ترکیب پرچم‌های cardinality پایین)، degenerate dimension (شناسه‌ای که در fact می‌ماند) و mini-dimension برای ویژگی‌های پرتغییر. ویژگی‌های کند‌تغییر با انواع SCD مدیریت می‌شوند.

## نگاه سنیور و نکات مصاحبه
کیفیت dimension کیفیت تحلیل را تعیین می‌کند: کلیدهای پایدار، کلید طبیعی یکتا در هر نسخه، بازه‌های اعتبار بدون هم‌پوشانی و برخورد روشن با عضو نامشخص یا دیررس (سطر stub استنباطی درج و بعداً اصلاح شود). dimension پهن در storeهای ستونی مشکلی ندارد. معیارهای عددی پرتغییر را در dimension نگذارید (جایشان fact است) و بدون دلیل قوی snowflake نکنید. در موتورهای توزیع‌شده dimension کوچک برای join با fact بزرگ broadcast می‌شود؛ dimensionهای عظیم (مشتری با صدها میلیون سطر) co-partition می‌خواهند.

سؤال مصاحبه: «امتیاز اعتباری مشتری fact است یا dimension؟» پاسخ: به نحوه‌ی استفاده و تغییرش بستگی دارد؛ ویژگی توصیفی کند‌تغییر می‌تواند در dimension باشد (SCD) ولی مقدار عددی پرتغییر برای aggregate بهتر است معیار در snapshot fact یا باند در mini-dimension باشد.
