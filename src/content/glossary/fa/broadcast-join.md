---
term: "join با broadcast (Broadcast join)"
term_en: "Broadcast join"
aliases: ["broadcast join", "broadcast hash join", "map-side join"]
category: batch
short: "joinای که در آن جدول کوچک به همه‌ی workerها کپی می‌شود تا جدول بزرگ نیازی به shuffle نداشته باشد."
related: ["shuffle", "data-skew", "predicate-pushdown"]
---
## به زبان ساده (مبتدی)
فرض کنید باید ۱۰ میلیون کارت مشتری را با فهرست کوتاهی از ۲۰ نام VIP تطبیق دهید. به‌جای بردن همه‌ی کارت‌ها به یک جا، به هر کارمند یک کپی از فهرست کوتاه می‌دهید و هرکس کارت‌های خودش را چک می‌کند. broadcast یعنی فرستادن جدول کوچک به همه تا جدول بزرگ سر جایش بماند.

## نحوه‌ی کار (متوسط)
joinهای توزیع‌شده‌ی معمولی هر دو طرف را بر اساس کلید join شافل می‌کنند. در broadcast hash join طرف کوچک جمع‌آوری می‌شود، به hash table درون‌حافظه تبدیل می‌شود و به همه‌ی executorها می‌رود؛ هر task پارتیشن خودش از جدول بزرگ را استریم و با hash table محلی تطبیق می‌دهد. طرف بزرگ shuffle نمی‌خواهد و skew روی آن اهمیتی ندارد.

```
from pyspark.sql.functions import broadcast
fact.join(broadcast(dim_country), "country_id")

-- SQL hint
SELECT /*+ BROADCAST(d) */ * FROM fact f JOIN dim_country d ON f.country_id = d.id;

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)
```

Spark وقتی اندازه‌ی تخمینی یک طرف زیر آستانه باشد خودکار انتخابش می‌کند و adaptive execution می‌تواند sort-merge join را در زمان اجرا به broadcast تبدیل کند. برای inner و left (broadcast طرف راست) کار می‌کند؛ برای full outer نه. Trino و StarRocks هم انتخاب بین توزیع broadcast و partitioned دارند.

## نگاه سنیور و نکات مصاحبه
ریسک‌ها: طرف کوچک در واقع کوچک نیست (آمار کهنه یا فیلتر دیرتر اعمال می‌شود) و OOM در driver یا executor یا collect کند می‌شود؛ broadcast مکرر یک جدول در حلقه؛ و تعداد زیاد executor که هزینه‌ی شبکه را چندبرابر می‌کند. driver معمولاً اول جدول را collect می‌کند پس حافظه‌ی آن مهم است. جایگزین وقتی هر دو بزرگ‌اند: shuffle hash یا sort-merge join، bucketing هر دو طرف، یا salting برای skew. dimensionهای star schema نمونه‌ی کلاسیک broadcast هستند.

سؤال مصاحبه: «کِی از broadcast join پرهیز می‌کنی؟» پاسخ: وقتی طرف build بزرگ است یا تخمین اندازه‌اش قابل اعتماد نیست؛ فشار حافظه و کپی شبکه‌ای به همه‌ی executorها از shuffle صرفه‌جویی‌شده بیشتر می‌شود؛ آنگاه sort-merge با پارتیشن‌بندی درست، bucketing یا runtime filter.
