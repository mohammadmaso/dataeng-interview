---
order: 34
tid: "pyspark-api"
track: "3"
section: null
num: "3.5"
title: "PySpark API"
type: "technology"
level: "intermediate"
priority: "P0"
est_hours: 5
summary: "نوشتن جاب با DataFrame API پایتون: transformation و action، UDF و هزینه‌اش، Pandas UDF، window در PySpark، و تست جاب‌ها."
covers: ["Python ↔ JVM serialization overhead","Arrow optimization","explode/struct/array ops"]
interview_qs: ["Rewrite this UDF with built-in functions","Why is my Python UDF slow?"]
extras: []
prerequisites: ["python-core", "spark-architecture"]
related: ["spark-catalyst", "spark-shuffle", "spark-aqe", "python-dataframes", "spark-writes", "spark-memory-tuning", "spark-joins-skew", "spark-ui-debugging"]
---
## 🎯 خلاصه

> PySpark یک **رابط پایتونی** برای یک plan است که در JVM اجرا می‌شود. با توابع داخلیِ DataFrame بنویس تا Spark کل plan را بهینه کند. هر تابع پایتونی که اضافه می‌کنی (UDF) یک جعبه‌ی سیاه است که هزینه‌ی رفت‌وبرگشت بین پایتون و JVM دارد.

- **transformation**ها (`filter`، `select`، `withColumn`، `groupBy`، `join`) فقط plan را می‌سازند. **action**ها (`count`، `collect`، `write`، `show`) آن را اجرا می‌کنند.
- یک **Python UDF** ردیف‌ها را ردیف‌به‌ردیف به یک Python worker می‌فرستد و برمی‌گرداند، و Catalyst داخلش را نمی‌بیند.
- یک **Pandas UDF** (UDF برداری) دسته‌ها را به‌صورت ستون‌های Arrow جابه‌جا می‌کند؛ این خیلی ارزان‌تر از صدا زدن ردیفی است، ولی باز هم از یک تابع داخلی گران‌تر است.
- توابع `F.*`، `explode`، `struct`، `array` و window را به UDF ترجیح بده. جاب‌ها را روی DataFrameهای کوچک محلی با fixture تست کن.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** یک اسکریپت PySpark یک دستور پخت است که تو می‌نویسی و آشپزخانه (JVM) می‌پزد. نوشتن `df.filter(...)` فقط یک خط روی دستور پخت می‌نویسد. تا وقتی یک action مثل `count()` یا `write` نخواهی غذا پخته نمی‌شود.

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").appName("demo").getOrCreate()

df = spark.createDataFrame(
    [("ali", "data", 9000), ("sara", "data", 12000), ("maryam", "web", 8000)],
    ["name", "dept", "salary"],
)

high = df.filter(F.col("salary") > 8500)   # transformation: nothing runs yet
high.explain()                             # prints the physical plan
print(high.count())                        # action: the job runs -> 2
```

DataFrame یک جدول با schema است. ستون‌ها را با `F.col("name")` (یا در بیشتر جاها رشته‌ی ساده‌ی `"name"`) ارجاع می‌دهی. این عادت را نگه دار: هر مرحله را به‌صورت تابعی روی ستون‌ها بیان کن، نه به‌صورت حلقه روی ردیف‌ها.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### plan تنبل و actionها

هر transformation یک DataFrame جدید برمی‌گرداند که به والدش اشاره می‌کند. وقتی یک action اجرا شود، بهینه‌ساز Spark (Catalyst) کل زنجیره را بازنویسی می‌کند: filterها را پایین می‌برد، ستون‌های استفاده‌نشده را حذف می‌کند و استراتژی join را انتخاب می‌کند. به همین دلیل یک `filter` که بعد از `join` نوشته شده هم می‌تواند قبل از آن اعمال شود.

| نوع | مثال‌ها | چه اتفاقی می‌افتد |
|---|---|---|
| transformation باریک (narrow) | `select`، `filter`، `withColumn` | هر partition جداگانه پردازش می‌شود؛ shuffle نیست |
| transformation پهن (wide) | `groupBy().agg`، `join`، `distinct`، `orderBy` | داده بر اساس کلید دوباره توزیع می‌شود: shuffle |
| action | `count`، `collect`، `show`، `write.parquet` | اجرای plan را شروع می‌کند |

هرگز روی یک DataFrame بزرگ `collect()` صدا نزن: همه‌ی ردیف‌ها را به حافظه‌ی driver می‌آورد.

### توابع داخلی در برابر UDF

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Slow path: each row goes to a Python worker and back
@F.udf(returnType=DoubleType())
def with_tax_udf(salary):
    return salary * 1.09 if salary is not None else None

# Fast path: an expression Spark runs and optimises inside the JVM
df_fast = df.withColumn("with_tax", F.col("salary") * 1.09)
df_slow = df.withColumn("with_tax", with_tax_udf("salary"))
```

در Python UDF، Spark ردیف را سریالایز می‌کند، به یک پروسس پایتونی جدا می‌فرستد، نتیجه را دی‌سریالایز می‌کند و برمی‌گرداند. این هزینه‌ی هر ردیف معمولاً کل جاب را غالب می‌کند. UDF برای Catalyst هم مات است: نمی‌تواند filter را از داخلش عبور دهد یا درباره‌ی null هایش استدلال کند. نسخه‌ی `df_fast` هیچ‌کدام از این هزینه‌ها را ندارد.

### Pandas UDF

وقتی منطق را نمی‌شود با توابع داخلی نوشت، از Pandas UDF استفاده کن. این تابع یک دسته از مقادیر را به‌صورت `pandas.Series` از طریق Apache Arrow دریافت می‌کند، پس هزینه به ازای هر دسته پرداخت می‌شود:

```python
import pandas as pd
from pyspark.sql import functions as F

@F.pandas_udf("double")
def with_tax_vec(s: pd.Series) -> pd.Series:
    return s * 1.09

df.withColumn("with_tax", with_tax_vec("salary")).show()
```

Pandas UDF به `pyarrow` روی workerها نیاز دارد. Arrow وقتی تنظیم مربوطه فعال باشد `toPandas()` را هم تسریع می‌کند (`spark.sql.execution.arrow.pyspark.enabled`)؛ برای نسخه‌ی Spark خودت مستندات را چک کن.

### آرایه، struct و explode

داده‌ی واقعی تودرتو است. `explode` یک ردیف با آرایه را به یک ردیف به‌ازای هر عنصر تبدیل می‌کند؛ `explode_outer` ردیف‌هایی را که آرایه‌شان null یا خالی است نگه می‌دارد:

```python
orders = spark.createDataFrame([(1, ["book", "pen"]), (2, ["lamp"]), (3, [])],
                               ["order_id", "items"])

orders.select("order_id", F.explode("items").alias("item")).show()
# (1, book), (1, pen), (2, lamp): order 3 is dropped

orders.select("order_id", F.explode_outer("items").alias("item")).show()
# order 3 kept with item = null

orders.select("order_id", F.size("items").alias("n_items")).show()
# 2, 1, 0
```

`F.struct("a", "b")` یک ستون تودرتو می‌سازد و `F.col("s.a")` یک فیلد را برمی‌گرداند. وقتی یک ردیف به‌ازای هر عنصر لازم داری `explode` را انتخاب کن، و وقتی فقط یک ویژگی از آرایه لازم داری از `F.size` یا `F.array_contains` استفاده کن.

### توابع window در PySpark

همان مفاهیم window در SQL اینجا هم برقرار است، با یک spec پایتونی:

```python
from pyspark.sql import Window, functions as F

w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
top_per_dept = (df.withColumn("rn", F.row_number().over(w))
                  .filter(F.col("rn") == 1)
                  .drop("rn"))
top_per_dept.show()   # sara (data), maryam (web)
```

window بدون `partitionBy` همه‌ی ردیف‌ها را در یک partition می‌ریزد. روی داده‌ی بزرگ این یک گلوگاه با یک task است؛ همان هشداری که در SQL داریم.

### تست جاب‌ها

transformationها را به‌صورت تابع نگه دار که DataFrame می‌گیرند و DataFrame برمی‌گردانند. بعد آن‌ها را با یک session محلی روی ورودی‌های کوچک تست کن:

```python
import pytest
from pyspark.sql import SparkSession, functions as F

@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield s
    s.stop()

def add_tax(df):
    return df.withColumn("with_tax", F.col("salary") * 1.09)

def test_add_tax(spark):
    df = spark.createDataFrame([(100.0,)], ["salary"])
    assert add_tax(df).collect()[0]["with_tax"] == pytest.approx(109.0)
```

## 🧠 عمق و trade-offها (سنیور)

- **plan را بخوان.** `df.explain(True)` هم plan منطقی و هم فیزیکی را نشان می‌دهد. دنبال `Exchange` (یک shuffle)، `BroadcastHashJoin` در برابر `SortMergeJoin` بگرد، و ببین آیا Python UDF به‌صورت `BatchEvalPython` یا `ArrowEvalPython` ظاهر می‌شود. زمان معمولاً همان‌جا می‌رود.
- **هزینه‌ی Python worker.** هر executor کنار JVM پروسس‌های Python worker اجرا می‌کند. حافظه بین heap جاوا، off-heap و Python تقسیم می‌شود. یک UDF که اشیای بزرگ می‌سازد ممکن است توسط YARN یا Kubernetes کشته شود، حتی وقتی heap جاوا خوب به نظر می‌رسد. به [[spark-memory-tuning]] نگاه کن.
- **shuffle و partition.** transformationهای پهن داده را shuffle می‌کنند. تعداد partition (`spark.sql.shuffle.partitions` یا adaptive execution) اندازه‌ی taskها را تعیین می‌کند. partition کم باعث skew و spill می‌شود؛ زیاد باعث سربار زمان‌بندی و فایل‌های کوچک. به [[spark-shuffle]] و [[spark-aqe]] نگاه کن.
- **`groupBy` با UDF.** `groupBy(...).applyInPandas(fn, schema)` یک تابع را برای هر گروه با یک pandas DataFrame اجرا می‌کند. راحت است، اما هر گروه در یک worker materialize می‌شود و کلید skew شده یک task بسیار بزرگ می‌سازد. به [[data-skew]] نگاه کن.
- **حافظه‌ی driver.** `collect()`، `toPandas()` و `.first()` روی داده‌ی بزرگ عملیات driver هستند. در production نتیجه را در storage (مثلاً Parquet) بنویس، نه این‌که به یک ماشین بیاوری.
- **کی از PySpark استفاده نکنیم.** برای داده‌ی کوچک (چند گیگابایتی که روی یک ماشین جا می‌شود) یک موتور تک‌ماشینی ساده‌تر و اغلب سریع‌تر است؛ Spark هزینه‌ی ثابت راه‌اندازی و برنامه‌ریزی دارد.

## 🧪 امتحان کن

1. **UDF را بازنویسی کن.** این UDF اگر حقوق بیشتر از ۱۰۰۰۰ باشد `"high"` برمی‌گرداند، اگر بین ۸۰۰۰ و ۱۰۰۰۰ (شامل هر دو) باشد `"mid"`، و در غیر این صورت `"low"`. آن را با `F.when` بازنویسی کن و نشان بده روی سه ردیف نمونه همان نتایج را می‌دهد.

   ```python
   @F.udf("string")
   def band(salary):
       if salary > 10000:
           return "high"
       if salary >= 8000:
           return "mid"
       return "low"
   ```
2. **explode و شمارش.** با `events = spark.createDataFrame([(1, ["a", "b"]), (2, ["b"])], ["id", "tags"])` تعداد رویدادها به‌ازای هر tag را حساب کن.
3. **دومین حقوق بیشترِ هر دپارتمان** با window و `dense_rank`.

<details><summary>جواب‌ها را ببین</summary>

**1.**
```python
from pyspark.sql import functions as F

rows = spark.createDataFrame([(12000,), (9000,), (7000,)], ["salary"])
banded = rows.withColumn(
    "band",
    F.when(F.col("salary") > 10000, "high")
     .when(F.col("salary") >= 8000, "mid")
     .otherwise("low"),
)
banded.show()   # high, mid, low
```

زنجیره‌ی `F.when` داخل JVM ارزیابی می‌شود و قابل بهینه‌سازی است، پس رفت‌وبرگشت پایتونی ندارد.

**2.**
```python
from pyspark.sql import functions as F

events = spark.createDataFrame([(1, ["a", "b"]), (2, ["b"])], ["id", "tags"])
(events.select(F.explode("tags").alias("tag"))
       .groupBy("tag").count()
       .show())
# b: 2, a: 1
```

**3.**
```python
from pyspark.sql import Window, functions as F

w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
second = (df.withColumn("dr", F.dense_rank().over(w))
            .filter(F.col("dr") == 2)
            .select("dept", "salary")
            .distinct())
second.show()
```

از `dense_rank` استفاده کن تا بالاترین حقوق‌های مساوی، دومین حقوق متمایز را پنهان نکنند. اگر در یک دپارتمان بیش از یک نفر در آن رتبه باشد، `distinct()` را هم اضافه کن.

</details>

## 💬 گوشه‌ی مصاحبه

**«UDF را با توابع داخلی بازنویسی کن.»** متوسط: «تابع پایتونی را با `F.when`، محاسبات ستونی `F.col` یا توابع رشته‌ای عوض می‌کنم تا Catalyst بتواند بهینه‌سازی کند.» سنیور: «UDF منطق را از بهینه‌ساز پنهان می‌کند و برای هر ردیف سریالایز می‌شود. اول بررسی می‌کنم آیا منطق می‌تواند یک عبارت ستونی باشد. اگر نشد، Pandas UDF می‌گیرم و خروجی را برای مدیریت null و تبدیل نوع تست می‌کنم، چون بازنویسی معمولاً همان‌جا خراب می‌شود.»

**«چرا Python UDF من کند است؟»** متوسط: «ردیف‌به‌ردیف در یک پروسس پایتونی اجرا می‌شود، پس هر ردیف مرز JVM را رد می‌کند.» سنیور: «سه هزینه: سریالایز کردن هر ردیف، پروسس Python worker به ازای هر executor، و این‌که Catalyst نمی‌تواند predicate را از داخلش عبور دهد. plan را برای `BatchEvalPython` بررسی می‌کنم، می‌بینم آیا UDF روی کلیدهای skew شده اجرا می‌شود، و آن را با توابع داخلی یا یک Pandas UDF مبتنی بر Arrow عوض می‌کنم.»

> **🎤 این را در مصاحبه بگو:** «transformationها را به‌صورت عبارت ستونی می‌نویسم تا Spark کل جاب را plan کند. UDF را فقط وقتی استفاده می‌کنم که هیچ تابع داخلی وجود نداشته باشد، و آن‌وقت برداری. قبل از بهینه‌سازی plan را می‌خوانم و منطق را روی DataFrameهای کوچک محلی تست می‌کنم.»

## ⚠️ اشتباهات رایج

> **⚠️ مراقب باش:** UDF بدون نوع بازگشتی، یا با نوع اشتباه، برای مقادیر ناسازگار بی‌صدا `null` برمی‌گرداند. `returnType` را صریحاً مشخص کن.

> **⚠️ مراقب باش:** صدا زدن `collect()` یا `toPandas()` روی DataFrame بزرگ فقط برای «یک نگاه انداختن». از `show()` یا `limit()` استفاده کن.

- نوشتن حلقه‌ی `for` پایتونی روی نتیجه‌ی `collect()` برای کاری که یک join یا تجمیع می‌توانست در کلاستر انجام دهد.
- انتظار این‌که `explode` آرایه‌های خالی را نگه دارد؛ از `explode_outer` استفاده کن.
- استفاده از window بدون `partitionBy` روی یک جدول بزرگ.
- فراموش‌کردن این‌که transformationها تنبل‌اند، پس زمانی که بدون action اندازه می‌گیری، هیچ چیزی اندازه نمی‌گیری.
- join بدون فکر کردن به skew کلید یا broadcast کردن طرف کوچک (به [[broadcast-join]] نگاه کن).
- تست فقط روی کل داده؛ روی DataFrameهای کوچک با ردیف‌های مورد انتظار تست کن.

## ✅ خودت را بسنج (کوئیز)

1. کدام فراخوانی واقعاً اجرای جاب را شروع می‌کند؟
   - A) `df.filter(...)`
   - B) `df.withColumn(...)`
   - C) `df.count()`
   - D) `df.groupBy(...)`

<details><summary>جواب</summary>

**C** — `count()` یک action است. بقیه transformation هستند که فقط plan را گسترش می‌دهند.

</details>

2. چرا `F.col("salary") * 1.09` از یک Python UDF که همان محاسبه را انجام می‌دهد سریع‌تر است؟
   - A) UDF نمی‌تواند double برگرداند
   - B) عبارت داخل JVM و بدون رفت‌وبرگشت پایتونی به‌ازای هر ردیف اجرا می‌شود، و Catalyst می‌تواند آن را بهینه کند
   - C) عبارت‌ها روی driver کش می‌شوند
   - D) UDFها همیشه روی driver اجرا می‌شوند

<details><summary>جواب</summary>

**B** — یک عبارت داخلی از سریالایز کردن هر ردیف برای یک Python worker جلوگیری می‌کند و Catalyst معنای آن را می‌بیند.

</details>

3. یک ردیف در ستون `items` یک آرایه‌ی خالی دارد. کدام تابع آن ردیف را با یک عنصر null نگه می‌دارد؟
   - A) `explode`
   - B) `explode_outer`
   - C) `size`
   - D) `struct`

<details><summary>جواب</summary>

**B** — `explode` ردیف‌های با آرایه‌ی null یا خالی را حذف می‌کند. `explode_outer` آن‌ها را با یک عنصر null نگه می‌دارد.

</details>

4. **(سنیور)** یک جاب Pandas UDF روی executorها با خطای حافظه شکست می‌خورد، با این‌که heap جاوا خوب به نظر می‌رسد. علت محتمل چیست و چه چیزی را بررسی می‌کنی؟

   - A) window بدون `partitionBy` است
   - B) حافظه‌ی Python worker خارج از heap جاوا، مثلاً یک دسته‌ی بزرگ در worker؛ تنظیمات memory overhead، اندازه‌ی دسته و plan را برای کلیدهای skew شده بررسی کن
   - C) `explode` تعداد زیادی ردیف تولید کرده
   - D) driver نتیجه را جمع کرده

<details><summary>جواب</summary>

**B** — Python workerها حافظه‌ای خارج از heap جاوا مصرف می‌کنند. تنظیمات overhead، اندازه‌ی هر دسته و این‌که آیا یک کلید skew شده گروه بزرگ بیش‌ازحد تولید می‌کند را بررسی کن.

</details>

5. **(سنیور)** برای دومین حقوق بیشتر متمایز در هر دپارتمان، چرا `dense_rank` را به `row_number` ترجیح می‌دهی؟

   - A) `row_number` در window قابل استفاده نیست
   - B) `row_number` به ردیف‌های مساوی شماره‌های متفاوت می‌دهد، پس یک تساوی در بالا، دومین مقدار واقعی متمایز را پنهان می‌کند؛ `dense_rank` به مقادیر مساوی رتبه‌ی یکسان و بدون فاصله می‌دهد
   - C) `dense_rank` روی هر کلاستری سریع‌تر است
   - D) `row_number` رشته برمی‌گرداند

<details><summary>جواب</summary>

**B** — با `row_number`، دو ردیف که در بالا مساوی‌اند شماره‌های ۱ و ۲ می‌گیرند، پس «دومی» تکراری بالاترین حقوق است. `dense_rank` به مقدار متمایز بعدی رتبه‌ی ۲ می‌دهد.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| transformation در برابر action | transformation پلن تنبل را گسترش می‌دهد؛ action آن را اجرا می‌کند |
| transformation باریک در برابر پهن | باریک داخل یک partition می‌ماند؛ پهن داده را بر اساس کلید shuffle می‌کند |
| چرا توابع داخلی بهتر از UDF‌اند | بدون رفت‌وبرگشت پایتونی به‌ازای هر ردیف؛ Catalyst قابل بهینه‌سازی است |
| Pandas UDF | دسته‌ای از طریق Arrow؛ ارزان‌تر از UDF ردیفی، ولی رایگان نیست |
| `explode` در برابر `explode_outer` | `explode` آرایه‌های null/خالی را حذف می‌کند؛ `explode_outer` نگه می‌دارد |
| window بدون `partitionBy` | همه‌ی ردیف‌ها در یک partition: گلوگاه |
| خطر `collect()` | همه‌ی ردیف‌ها را به حافظه‌ی driver می‌آورد |
| تست جاب‌های Spark | توابع خالص روی DataFrame، با session محلیِ کوچک |

## 🔗 ادامه

- **spark-catalyst** توضیح می‌دهد بهینه‌ساز چطور plan DataFrame تو را بازنویسی می‌کند.
- **python-dataframes** DataFrame در pandas و Spark را برای همین ایده‌ها مقایسه می‌کند.
- **spark-joins-skew** و **spark-shuffle** هزینه‌ی transformationهای پهن را در کلاستر پوشش می‌دهند.
- **spark-writes** نوشتن نتایج در storage را به‌جای آوردن آن‌ها به driver توضیح می‌دهد.
- **spark-ui-debugging** نشان می‌دهد چطور stage و task کند یک جاب پرِ UDF را پیدا کنی.
