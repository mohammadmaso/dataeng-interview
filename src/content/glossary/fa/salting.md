---
term: "سالت‌گذاری (Salting)"
term_en: "Salting"
aliases: ["salting", "salt", "سالت"]
category: batch
short: "افزودن پسوند تصادفی به کلید skewed تا سطرهایش بین چند پارتیشن پخش شود و سپس نتایج دوباره ترکیب شوند."
related: ["data-skew", "hot-key", "shuffle"]
---
## به زبان ساده (مبتدی)
اگر یک صف صندوق بیش از حد طولانی است، مشتری‌های آن را با دادن شماره‌های تصادفی بین چند صف کوچک‌تر تقسیم می‌کنید و در پایان جمع‌ها را ترکیب می‌کنید. salting یک کلید اشباع را به چند زیرکلید کوچک‌تر تقسیم می‌کند تا چند worker بار را بین خود بگیرند.

## نحوه‌ی کار (متوسط)
برای aggregate skewed: یک salt تصادفی `0..N-1` به کلید اضافه کنید، بر اساس `(key, salt)` تجمیع کنید تا نتایج جزئی بین N پارتیشن پخش شود، سپس دوباره بر اساس کلید اصلی تجمیع کنید. این aggregate دومرحله‌ای است و برای توابع تجزیه‌پذیر (sum، count، min، max) کار می‌کند؛ میانگین به sum و count نیاز دارد؛ distinct دقیق مراقبت می‌خواهد.

```
from pyspark.sql import functions as F
N = 16
# 1) skewed side: add random salt
big  = big.withColumn("salt", (F.rand() * N).cast("int"))
# 2) other side: replicate each row N times with every salt value
small = small.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)])))
# 3) join on (key, salt)
joined = big.join(small, ["key", "salt"])

# Aggregation: two-stage
partial = df.withColumn("salt", (F.rand()*N).cast("int")).groupBy("key","salt").sum("v")
final   = partial.groupBy("key").sum("sum(v)")
```

برای join skewed: طرف بزرگ skewed را با salt تصادفی بزنید و طرف دیگر را برای همه‌ی مقادیر salt تکثیر کنید تا هر کلید salt‌دار هنوز جفت خود را پیدا کند. هزینه‌اش تکثیر N برابری طرف کوچک است؛ پس فقط کلیدهای سنگین را salt کنید (کلیدهای داغ را فیلتر و salt کنید و بقیه را عادی join بزنید) یا بگذارید AQE در Spark خودکار تقسیم کند.

## نگاه سنیور و نکات مصاحبه
N را از ضریب skew کلید انتخاب کنید نه کورکورانه؛ بزرگ‌بودن بیش از حد shuffle و حافظه هدر می‌دهد و کوچک‌بودن straggler باقی می‌گذارد. salting پیچیدگی کد اضافه می‌کند و فرض هم‌مکان‌بودن کلید یکسان را می‌شکند؛ پس منطق وابسته به ترتیب (توابع پنجره‌ای، dedup بر اساس جدیدترین) را نمی‌توان ساده salt کرد. اولویت: حذف skew از منبع (فیلتر null، اصلاح کلید پیش‌فرض)، broadcast طرف کوچک، فعال‌کردن AQE و سپس salting. این مفهوم برای دیتابیس و Kafka هم هست (پسوند کلید برای پخش پارتیشن داغ).

سؤال مصاحبه: «salting را با مثال توضیح بده.» پاسخ: اگر ۶۰٪ رویدادها `user_id = NULL` دارند، یک پسوند تصادفی به کلید اضافه می‌کنم، به‌ازای (key, salt) در taskهای زیاد pre-aggregate می‌کنم و سپس بر اساس کلید اصلی تجمیع می‌کنم؛ برای join طرف دیگر را در salt تکثیر می‌کنم، ترجیحاً فقط برای کلیدهای داغ شناخته‌شده.
