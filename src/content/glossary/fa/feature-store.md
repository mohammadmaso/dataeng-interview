---
term: "فروشگاه ویژگی (Feature store)"
term_en: "Feature store"
aliases: ["feature store", "فیچر استور", "online store", "offline store", "point-in-time join"]
category: ml
short: "سیستمی که featureهای ML را برای آموزش (offline) و استنتاج کم‌تأخیر (online) به‌صورت سازگار ذخیره، سرو و دوباره‌استفاده می‌کند."
related: ["training-serving-skew", "backfill", "event-time"]
---
## به زبان ساده (مبتدی)
feature store انباری مشترک از مواد اولیه‌ی آماده (featureها) برای آشپزخانه‌های data science است. به‌جای اینکه هر تیم مدل «میانگین خرج مشتری در ۳۰ روز» را جداگانه و به شیوه‌ی خودش بپزد، یک تعریف تست‌شده هم هنگام آموزش و هم هنگام پیش‌بینی زنده استفاده می‌شود.

## نحوه‌ی کار (متوسط)
featureها با pipelineهای batch و استریم از رویدادهای خام محاسبه می‌شوند. offline store تاریخچه‌ی کامل را (lake یا warehouse) برای ساخت دیتاست آموزش نگه می‌دارد؛ online store آخرین مقدار هر موجودیت را در store کلید-مقدار کم‌تأخیر برای استنتاج بلادرنگ. registry تعریف featureها، مالک و متادیتا را دارد و jobهای materialization offline را با online همگام می‌کنند.

```
        batch/stream pipelines
                 │  (one feature definition)
        ┌────────┴─────────┐
   OFFLINE store       ONLINE store
 (Parquet/warehouse)   (Redis/DynamoDB)
 history for training   latest value, ms lookups for inference

-- Point-in-time correct training set: for each (entity, label_ts) take the feature
-- value as of label_ts, never later  (ASOF join)
SELECT l.*, f.avg_spend_30d FROM labels l
ASOF JOIN features f ON l.user_id = f.user_id AND f.ts <= l.label_ts;
```

قابلیت کلیدی point-in-time correctness است: هنگام ساخت داده‌ی آموزش هر نمونه باید مقدار feature را همان‌طور که در زمان برچسب بود بگیرد، وگرنه اطلاعات آینده به آموزش نشت می‌کند (label leakage) و مدل offline عالی ولی در production ضعیف است. feature store همچنین استفاده‌ی مجدد، lineage، مانیتورینگ و backfill می‌دهد.

## نگاه سنیور و نکات مصاحبه
چالش‌های مهندسی داده: دقیقاً یک منطق تبدیل برای batch و stream (پیاده‌سازی دوباره در دو کدبیس باعث training-serving skew می‌شود)، رویداد دیررس و درستی event-time، backfill featureها برای تعریف جدید، TTL و تازگی در online store، طراحی کلید موجودیت و هزینه‌ی سرو در مقیاس. هر تیمی به آن نیاز ندارد: برای چند مدل، یک warehouse منظم به‌علاوه‌ی export ساده به key-value ممکن است کافی باشد. جایگزین‌ها: محاسبه‌ی feature در زمان درخواست (on-demand) یا warehouse به‌عنوان offline store با cache آنلاین باریک.

سؤال مصاحبه: «feature store چه مشکلی را حل می‌کند؟» پاسخ: featureهای سازگار و قابل‌استفاده‌ی مجدد بین آموزش و سرو با داده‌ی آموزش point-in-time-correct تضمین می‌کند، که leakage و training-serving skew را حذف می‌کند، و دسترسی آنلاین کم‌تأخیر، مانیتورینگ و lineage می‌دهد.
