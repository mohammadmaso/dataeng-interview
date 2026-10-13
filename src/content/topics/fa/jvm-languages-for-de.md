---
order: 37
tid: "jvm-languages-for-de"
track: "3"
section: null
num: "3.8"
title: "Java/Scala for Data Systems"
type: "technology"
level: "intermediate"
priority: "P2"
est_hours: 3
summary: "Spark و Flink و Kafka بومی JVM هستند. دانستن Java یا Scala در حد خواندن کد، نوشتن UDF یا کانکتور، و فهم stack trace یک برتری است."
covers: ["Scala basics for Spark","Flink DataStream API in Java","Kafka client in Java","when to choose JVM over Python"]
interview_qs: []
extras: []
prerequisites: ["os-jvm-essentials"]
related: ["pyspark-api", "spark-architecture", "spark-catalyst", "flink", "kafka-streams", "kafka-producers", "python-dataframes"]
---

## 🎯 خلاصه

> بخش بزرگی از استک مهندسی داده روی **JVM** (ماشین مجازی جاوا) اجرا می‌شود: Spark، Flink، Kafka و بیشتر کانکتورهایشان. لازم نیست متخصص Java باشی، اما خواندن سورس آن‌ها، نوشتن یک **UDF** (تابع تعریف‌شده توسط کاربر)، یک **کانکتور** یا یک **producer**، و خواندن یک **stack trace** بدون ترس، مزیت واقعی است.

- **Scala** زبان بومی Spark است. API تایپ‌شده‌ی `Dataset` قدرتمند است، اما بیشتر jobها از **DataFrame API** با عبارت‌های ستونی استفاده می‌کنند.
- **Java** زبان API جریانی DataStream در Flink و کلاینت‌های Kafka است. lambdaهایش برای pipelineها کافی‌اند.
- یک **UDF پایتونی** برای هر دسته از سطرها از مرز process عبور می‌کند؛ یک **UDF روی JVM** داخل executor اجرا می‌شود. این مهم‌ترین دلیل کارایی برای انتخاب کد JVM برای منطق سفارشی است.
- JVM را انتخاب کن وقتی کد سطح موتور می‌نویسی، به تأخیر کم در streaming نیاز داری، یا به کانکتورهای مخصوص JVM تکیه داری. وقتی منطق تحلیلی است و مهارت تیم در پایتون است، در پایتون بمان.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** JVM مثل یک آشپزخانه‌ی مشترک با مجموعه‌ی استاندارد وسایل است. Spark، Flink و Kafka همگی در همان آشپزخانه ساخته شده‌اند، پس از همان فر و چاقو استفاده می‌کنند. اگر بخواهی یک غذای جدید داخل آشپزخانه‌شان درست کنی (یک UDF سفارشی یا کانکتور)، خیلی راحت‌تر است که با همان زبانی آشپزی کنی که آن‌ها استفاده می‌کنند.

همین ایده، «همه‌ی رویدادها را بزرگ کن»، در سه جایی که بیشتر احتمال دارد با کد JVM روبه‌رو شوی. اول، job فلینک با Java:

```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UpperCaseJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> events = env.fromElements("click", "view", "click");
        DataStream<String> upper = events.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) {
                return value.toUpperCase();
            }
        });

        upper.print();
        env.execute("upper-case");
    }
}
```

این job یک منبع، یک تبدیل و یک sink (`print`) تعریف می‌کند، و بعد `execute` آن را شروع می‌کند. هر برنامه‌ی Flink همین شکل را دارد: **منبع ← تبدیل‌ها ← sink ← execute**.

نسخه‌ی مبتدی با Scala و Spark همین کار را روی یک جدول انجام می‌دهد:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.upper

val spark = SparkSession.builder().appName("upper").getOrCreate()
import spark.implicits._

Seq("click", "view", "click").toDF("event")
  .withColumn("event", upper($"event"))
  .show()
```

کد کوتاه است چون منطق یک عبارت ستونی است. موتور برنامه‌ریزی و اجرایش را انجام می‌دهد؛ تو می‌گویی چه می‌خواهی.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### چرا این سیستم‌ها روی JVM هستند

Spark، Flink و Kafka موتورهای سمت سرور هستند که به thread، مدیریت حافظه، garbage collector بالغ و اکوسیستم بزرگی از کتابخانه‌ها (سریال‌سازی، شبکه، سیستم فایل سازگار با Hadoop) نیاز دارند. JVM همه‌ی این‌ها را فراهم می‌کند، و کد اصلی موتورها، operatorها و بسیاری از کانکتورها با Java یا Scala نوشته شده‌اند. APIهای پایتون لایه‌های نازکی هستند که کار را به آن هسته می‌فرستند (در PySpark، فرایند پایتون با driver و executorهای JVM حرف می‌زند).

### Scala برای Spark

Scala ویژگی‌های تابعی و شیءگرا را ترکیب می‌کند و به bytecode مربوط به JVM کامپایل می‌شود. سه ایده برای کار با Spark مهم‌اند:

- **`case class`** یک رکورد تغییرناپذیر با برابری و `toString` خوانا می‌دهد. با آن یک سطر تایپ‌شده را توصیف می‌کنی.
- **`Dataset[T]`** یک مجموعه‌ی تایپ‌شده است. کامپایلر چک می‌کند که `o.amount` وجود دارد و `Double` است.
- **`DataFrame`** همان `Dataset[Row]` است. در زمان کامپایل بدون تایپ است، اما عبارت‌های ستونی (`$"amount"`، `col("amount")`) از بهینه‌ساز Catalyst عبور می‌کنند که می‌تواند ترتیب را عوض کند، ستون‌ها را هرس کند و فیلترها را پایین ببرد.

```scala
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Order(orderId: Long, customer: String, amount: Double)

object TopCustomers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("top-customers").getOrCreate()
    import spark.implicits._

    val orders: Dataset[Order] = Seq(
      Order(1L, "ali", 10.0), Order(2L, "sara", 5.5), Order(3L, "ali", 7.0)
    ).toDS()

    // Typed API: the compiler checks fields, but the lambda is opaque to Catalyst
    orders.filter(_.amount > 6.0).map(_.customer.toUpperCase).show()

    // Column API: Catalyst sees the whole expression and can optimize it
    orders
      .groupBy($"customer")
      .agg(sum($"amount").as("total"))
      .orderBy(desc("total"))
      .show()

    spark.stop()
  }
}
```

lambdaهای تایپ‌شده (`_.amount > 6.0`) تابع‌های Scala هستند که Spark نمی‌تواند داخلشان را ببیند. نسخه‌ی ستونی برای بهینه‌ساز قابل مشاهده است. به همین دلیل قاعده‌ی «توابع ستونی داخلی را به lambda تایپ‌شده ترجیح بده» توصیه‌ی خوبی برای کارایی است.

### UDFها: جایی که کد JVM ارزشش را ثابت می‌کند

UDF منطق سفارشی است که Spark روی مقادیر اعمال می‌کند. سه سطح دارد، از سریع‌ترین به کندترین:

1. **توابع داخلی** (`trim`، `lower`، `to_date`، `when`) داخل موتور بهینه‌شده‌ی Spark اجرا می‌شوند. اول همیشه این‌ها را امتحان کن.
2. **UDF های JVM** (Scala یا Java) داخل JVM مربوط به executor اجرا می‌شوند. بدون سریال‌سازی از مرز process. برای بهینه‌ساز مبهم‌اند، اما سریع‌اند.
3. **UDF های پایتونی** داخل یک process کارگر پایتون کنار هر executor اجرا می‌شوند. سطرها از JVM به پایتون و برگشت سریال‌سازی می‌شوند، که هزینه‌ی CPU و حافظه دارد. Pandas UDFها (برداری، مبتنی بر Arrow) این هزینه را کم می‌کنند.

```scala
import org.apache.spark.sql.functions.udf

val normalize = udf((s: String) => Option(s).map(_.trim.toLowerCase).orNull)
// df.withColumn("customer_norm", normalize($"customer"))
// Better when possible: df.withColumn("customer_norm", lower(trim($"customer")))
```

به مدیریت صریح null دقت کن: UDF برای یک ستون null، مقدار `null` می‌گیرد، و صدا زدن `s.trim` روی آن داخل executor یک `NullPointerException` می‌دهد.

### DataStream API فلینک در Java

Flink جریان‌های نامحدود را با زمان و وضعیت صریح پردازش می‌کند. DataStream API سطح پایین‌تر است که در آن کلیدها، پنجره‌ها و وضعیت را خودت کنترل می‌کنی. یک شمارش پنجره‌ای کلیددار در Java چنین شکلی دارد (شکل pipeline که در مصاحبه توصیف می‌کنی):

```text
env.fromSource(kafkaSource, watermarks, "orders")
   .keyBy(order -> order.customerId)          // partition by key
   .window(TumblingEventTimeWindows.of(Time.minutes(5)))
   .aggregate(new SumAmount())                // per key, per 5-minute window
   .sinkTo(warehouseSink);
```

ایده‌های مهم در همه‌ی زبان‌ها یکی‌اند: یک **کلید** تعیین می‌کند کدام task موازی مالک وضعیت است، **زمان رویداد** و **watermark** تعیین می‌کنند کی یک پنجره بسته شود، و **sink** تضمین تحویل را تعیین می‌کند ([[watermark]]، [[tumbling-window]]، [[exactly-once]]). نحو Java فقط این انتخاب‌ها را صریح می‌کند.

### کلاینت‌های Kafka در Java

کلاینت Java کافکا پیاده‌سازی مرجع است. producer چند تنظیم حیاتی را پیکربندی می‌کند و بعد رکوردها را با `send` می‌فرستد؛ ارسال ناهمگام است و یک `Future` برمی‌گرداند:

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                new ProducerRecord<>("orders", "order-42", "{\"amount\":10.5}");
            RecordMetadata meta = producer.send(record).get(); // block only in a demo
            System.out.printf("partition=%d offset=%d%n", meta.partition(), meta.offset());
        }
    }
}
```

سه جزئیات را بفهم: **کلید** (`"order-42"`) پارتیشن را تعیین می‌کند، پس همان کلید همیشه به همان پارتیشن می‌رود و ترتیبش را حفظ می‌کند؛ `acks=all` منتظر replicaهای همگام می‌ماند؛ و `enable.idempotence` از تکراری شدن در اثر retry خود producer جلوگیری می‌کند. بلاک `try` شیء producer را می‌بندد و رکوردهای معلق را flush می‌کند. در production، از `.get()` برای هر `send` پرهیز کن و به‌جایش از callback استفاده کن، تا throughput سریال نشود.

### کی JVM را به پایتون ترجیح بدهیم

| وضعیت | گرایش به JVM | گرایش به پایتون |
|---|---|---|
| operator سفارشی Spark یا Flink روی میلیون‌ها سطر در ثانیه | بله | خیر |
| نوشتن یک کانکتور یا plugin برای Kafka Connect | بله | معمولاً پشتیبانی نمی‌شود |
| خواندن کد سورس موتور برای دیباگ | بله | معمولاً لازم نیست |
| مهندسی ویژگی، notebook، تحویل به ML | ممکن است | بله |
| توان عملیاتی UDF پایتونی گلوگاه است | منطق را به Scala/Java یا توابع داخلی ببر | از Pandas UDF و Arrow استفاده کن |
| تیم از قبل سرویس‌های پایتونی دارد | یک زبان دیگر برای نگهداری اضافه می‌شود | بله |

## 🧠 عمق و trade-offها (سنیور)

- **سریال‌سازی و closure.** یک تابع Spark یا Flink که یک شیء غیرقابل سریال‌سازی را capture کند (اتصال دیتابیس، logger ساخته‌شده در driver) در زمان اجرا با `NotSerializableException` شکست می‌خورد. چنین اشیائی را داخل task، یک بار برای هر partition، بساز، نه داخل closure.
- **تعارض classloading و وابستگی‌ها.** Spark و Flink نسخه‌های خودشان از Jackson، Guava، Netty و کتابخانه‌های Hadoop را همراه دارند. یک fat jar که نسخه‌ی دیگری را bundle کند می‌تواند روی کلاستر `NoSuchMethodError` بدهد، ولی روی لپ‌تاپ نه. وابستگی‌های موتور را `provided` علامت بزن و بسته‌های متعارض را shade یا relocate کن.
- **هم‌ترازی نسخه‌ها.** نسخه‌ی JDK و نسخه‌ی باینری Scala باید با چیزی که انتشار Spark یا Flink برایش ساخته شده بخوانند (مثلاً `spark-sql_2.12` در برابر `_2.13`). یادداشت‌های انتشار نسخه‌ات را چک کن؛ ناهم‌خوانی به شکل خطاهای لینک‌کردن گیج‌کننده ظاهر می‌شود.
- **Garbage collection و حافظه.** executorهای JVM تنظیمات heap دارند که به اندازه‌ی کد مهم‌اند. توقف‌های طولانی GC شبیه taskهای معلق دیده می‌شوند. قبل از تنظیم کد، لاگ GC و تب حافظه‌ی Spark UI را بخوان ([[spark-memory-tuning]]).
- **خواندن stack trace.** این مهارت عملی است: اولین frame که مال کد خودت است، بعد اولین frame موتور، و زنجیره‌ی «Caused by» در پایین. بیشتر خرابی‌ها `NullPointerException` از یک ستون null، `ClassCastException` از ناهم‌خوانی اسکیما، یا خطای سریال‌سازی از یک شیء capture‌شده‌اند.
- **trade-off تایپ‌شده در برابر بدون‌تایپ.** Datasetهای تایپ‌شده نام فیلدهای غلط را در زمان کامپایل می‌گیرند، اما بعضی بهینه‌سازی‌ها را از دست می‌دهند و می‌توانند کندتر باشند. برای ETL production، عبارت‌های ستونی DataFrame را ترجیح بده و کد تایپ‌شده را به لبه‌ها محدود کن.
- **رصد آنچه می‌نویسی.** متریک‌های سفارشی (شمارنده برای رکوردهای ردشده، timer برای فراخوانی‌های خارجی) را از طریق سیستم متریک موتور منتشر کن، و کنار lag و مدت checkpoint دنبالشان کن ([[consumer-lag]]).
- **کی کد JVM ننویس.** اگر یک تابع داخلی یا یک عبارت SQL کار را انجام می‌دهد، یک UDF سفارشی JVM هزینه‌ی build، deploy و تست اضافه می‌کند بدون سود. کد سفارشی را فقط وقتی بنویس که توابع داخلی منطق را بیان نمی‌کنند یا شکاف کارایی اندازه‌گیری شده است.

## 🧪 امتحان کن

1. **خروجی را پیش‌بینی کن.** با `Seq(Order(1L,"ali",10.0), Order(2L,"sara",5.5), Order(3L,"ali",7.0))`، کد `orders.filter(_.amount > 6.0).map(_.customer.toUpperCase).show()` چه چیزی چاپ می‌کند؟ بعد معادلش را با `$"amount" > 6.0` و `upper($"customer")` بنویس و توضیح بده کدام‌یک را Catalyst بهتر می‌تواند بهینه کند.
2. **باگ را پیدا کن.** یک `MapFunction` در Flink یک اتصال JDBC را در فیلدی که در `main` ساخته شده نگه می‌دارد و داخل `map` استفاده می‌کند. توضیح بده چه اشتباهی می‌شود و چطور رفعش می‌کنی (راهنمایی: به سریال‌سازی و اینکه اتصال کجا باز شود فکر کن).
3. **کلید را عوض کن.** در producer کافکا، کلید رکورد را از `"order-42"` به `null` تغییر بده. توضیح بده پارتیشن‌بندی و تضمین ترتیب برای رویدادهای یک مشتری چه می‌شود.

<details><summary>راه‌حل‌ها را نشان بده</summary>

**۱.** فیلتر مقادیر بزرگ‌تر از ۶.۰ را نگه می‌دارد: `10.0` و `7.0`، که هر دو مال `ali` هستند. خروجی دو سطر است، هر دو `ALI`. (مقدار ۵.۵ مال sara حذف می‌شود.) نسخه‌ی ستونی، `orders.filter($"amount" > 6.0).select(upper($"customer")).show()`، همین مقادیر را می‌دهد، اما بهینه‌ساز شرط و projection را مستقیم می‌بیند و می‌تواند آن‌ها را به منبعی که پشتیبانی می‌کند پایین ببرد. lambda تایپ‌شده آن‌ها را پنهان می‌کند.

**۲.** اتصال روی driver ساخته و توسط تابع capture شده است. تابع سریال می‌شود و برای هر task فرستاده می‌شود، و اتصال JDBC قابل سریال‌سازی نیست، پس job با `NotSerializableException` شکست می‌خورد. راه‌حل این است که اتصال داخل task باز شود، مثلاً در `open(Configuration)` یک `RichMapFunction`، و در `close()` بسته شود.

**۳.** با کلید `null`، producer پارتیشن را round-robin یا با sticky partitioner انتخاب می‌کند (بسته به نسخه‌ی کلاینت). رویدادهای یک مشتری ممکن است بین پارتیشن‌ها پخش شوند و ترتیبشان دیگر تضمین نمی‌شود. کلیددار کردن با `customer_id` باعث می‌شود رویدادهای هر مشتری در یک پارتیشن بمانند و ترتیب آن مشتری حفظ شود، با هزینه‌ی احتمالی skew اگر یک مشتری خیلی بزرگ باشد ([[hot-key]]).

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «وقتی تیم با PySpark کار می‌کند، چرا یک مهندس داده Scala یا Java یاد بگیرد؟»**
جواب متوسط: «برای نوشتن سریع‌تر UDFها و خواندن سورس Spark.»
جواب سنیور: «PySpark یک لایه‌ی نازک روی کد JVM است. وقتی به یک operator سفارشی، یک کانکتور یا تشخیص یک خرابی سریال‌سازی یا shuffle نیاز دارم، جواب در استک JVM است. UDFهای JVM هزینه‌ی سریال‌سازی پایتون را حذف می‌کنند. با این حال اول از توابع داخلی و عبارت‌های DataFrame شروع می‌کنم و فقط وقتی اندازه‌گیری‌ها توجیه کنند به JVM می‌روم.»

**سؤال: «یک job Spark با `NotSerializableException` شکست می‌خورد. چه چیزی را چک می‌کنی؟»**
متوسط: «اینکه آیا یک شیء غیرقابل سریال‌سازی در closure capture شده یا نه.»
سنیور: «شیء را در stack trace پیدا می‌کنم و بررسی می‌کنم lambda چه چیزی را capture می‌کند. ساختن شیء را داخل task (برای هر partition) می‌برم، یا از یک factory قابل سریال‌سازی استفاده می‌کنم، و چک می‌کنم آیا فیلد سطح کلاس شیء والد وضعیت بیشتری از حد لازم می‌کشد.»

> **🎤 این را در مصاحبه بگو:** «PySpark را درِ ورودی می‌بینم و JVM را ساختمان. پیش‌فرضم پایتون است، و وقتی به کد سطح موتور، یک کانکتور یا یک تابع سفارشی سریع نیاز دارم به Scala یا Java می‌روم، و وقتی بتوانم سود را نشان بدهم.»

## ⚠️ اشتباهات رایج

> **⚠️ هشدار:** UDFی که null را نادیده می‌گیرد. مقدار null یک ستون به کد تو می‌رسد و یک `.trim` روی آن task را می‌کشد. null را صریح مدیریت کن یا از توابع داخلی null-safe استفاده کن.

> **⚠️ هشدار:** ساختن یک اتصال، کلاینت یا logger در driver و استفاده از آن داخل task. داخل task و یک بار برای هر partition بساز.

- نوشتن UDF برای منطقی که `lower`، `trim`، `when` یا `regexp_extract` از قبل بیانش می‌کنند.
- استفاده از UDF پایتونی برای کار سنگین سطربه‌سطر بدون اندازه‌گیری، در حالی که یک Pandas UDF یا UDF روی JVM خیلی سریع‌تر بود.
- ترکیب نسخه‌های باینری Scala (`_2.12` و `_2.13`) در یک build.
- bundle کردن وابستگی‌های Spark یا Flink داخل یک fat jar بدون علامت‌گذاری `provided`.
- صدا زدن `.get()` روی هر ارسال Kafka در یک حلقه‌ی پرتکرار، که producer را سریال می‌کند.
- کلیددار کردن رکوردهای Kafka با `null` وقتی ترتیب در هر موجودیت مهم است.

## ✅ خودت را بسنج

۱. چرا یک UDF روی JVM معمولاً برای منطق سطربه‌سطر در Spark سریع‌تر از یک UDF پایتونی است؟
   - A) Java کامپایل می‌شود و پایتون نه، پس کد همیشه سریع‌تر اجرا می‌شود
   - B) UDF روی JVM داخل process executor اجرا می‌شود، در حالی که UDF پایتونی نیاز دارد سطرها را به یک کارگر پایتون سریال کند و برگرداند
   - C) UDFهای JVM بررسی null را رد می‌کنند
   - D) UDFهای پایتونی نمی‌توانند رشته بخوانند

<details><summary>جواب</summary>

**B** — هزینه از مرز process و سریال‌سازی می‌آید. UDF روی JVM داخل executor می‌ماند؛ UDF پایتونی هر دسته از سطرها را به یک process جدا می‌فرستد و برمی‌گرداند. توابع داخلی هنوز سریع‌تر هستند.

</details>

۲. در Kafka، چه چیزی تعیین می‌کند یک رکورد کلیددار به کدام پارتیشن می‌رود؟
   - A) تنظیم `acks` در producer
   - B) کلید رکورد، با یک هش (partitioner پیش‌فرض)، پس همان کلید به همان پارتیشن می‌رود
   - C) نام consumer group
   - D) اندازه‌ی مقدار

<details><summary>جواب</summary>

**B** — partitioner پیش‌فرض کلید را هش می‌کند. کلید یکسان، پارتیشن یکسان، و در نتیجه تحویل مرتب برای آن کلید در داخل پارتیشن.

</details>

۳. اصلی‌ترین دلیلی که lambda ‏`filter(_.amount > 6.0)` در Scala سخت‌تر از `filter($"amount" > 6.0)` بهینه می‌شود چیست؟
   - A) lambdaها نمی‌توانند boolean برگردانند
   - B) lambda تایپ‌شده یک تابع مبهم Scala است که Catalyst نمی‌تواند داخلش را ببیند؛ عبارت ستونی یک درخت است که Catalyst می‌تواند بازنویسی و پایین ببرد
   - C) lambdaها همیشه روی driver اجرا می‌شوند
   - D) عبارت‌های ستونی به پایتون کامپایل می‌شوند

<details><summary>جواب</summary>

**B** — Catalyst روی درخت‌های عبارت کار می‌کند. یک تابع دلخواه جعبه‌ی سیاه است، پس شرط‌ها نمی‌توانند به منبع پایین برده یا بازچینی شوند.

</details>

۴. **(سنیور)** یک job فلینک روی لپ‌تاپ درست اجرا می‌شود، اما روی کلاستر با `NoSuchMethodError` در یک کلاس Jackson شکست می‌خورد. علت محتمل و رفع آن چیست؟

   - A) job حافظه‌ی زیادی مصرف می‌کند
   - B) fat jar نسخه‌ی دیگری از Jackson را نسبت به آنچه کلاستر فراهم می‌کند bundle کرده و تعارض classpath ایجاد شده؛ وابستگی‌های موتور را `provided` کن، نسخه‌ها را هم‌تراز کن، یا بسته‌ی متعارض را relocate کن
   - C) Flink از کتابخانه‌های Java پشتیبانی نمی‌کند
   - D) بروکر Kafka خیلی قدیمی است

<details><summary>جواب</summary>

**B** — classpath لپ‌تاپ و کلاستر فرق دارند. کتابخانه‌هایی که موتور فراهم می‌کند نباید داخل jar برنامه تکرار شوند، و تعارض‌ها با هم‌ترازی یا relocation حل می‌شوند.

</details>

۵. **(سنیور)** تیم می‌خواهد یک UDF سفارشی به Scala برای تبدیلی بنویسد که الان با Pandas UDF در پایتون انجام می‌شود و job کند است. قبل از موافقت چه چیزی را چک می‌کنی؟

   - A) هیچ؛ JVM همیشه انتخاب درست است
   - B) اول اندازه بگیر زمان کجا می‌رود (سریال‌سازی، GC، خود منطق)، بررسی کن آیا توابع داخلی یا SQL منطق را بیان می‌کنند، و هزینه‌ی UDF روی JVM، شامل build و deploy، را با سود اندازه‌گیری‌شده مقایسه کن
   - C) job را روی یک thread اجرا کن
   - D) همه‌ی UDFها را حذف کن و هر زمان اجرا را بپذیر

<details><summary>جواب</summary>

**B** — تصمیم به شواهد نیاز دارد. اغلب راه‌حل یک تابع داخلی، پارتیشن‌بندی بهتر، یا عبور سطرهای کمتر از مرز است. UDF روی JVM وقتی توجیه می‌شود که اندازه‌گیری‌ها نشان دهند مرز پایتون گلوگاه است و توابع داخلی نمی‌توانند منطق را بیان کنند.

</details>

## 🗂️ فلش‌کارت

| جلوی کارت | پشت کارت |
|---|---|
| چرا Spark، Flink و Kafka روی JVM هستند؟ | به thread، مدیریت حافظه و runtime بالغ نیاز دارند؛ هسته و کانکتورهایشان Java/Scala است |
| `Dataset[T]` در برابر `DataFrame` | مجموعه‌ی تایپ‌شده از T در برابر `Dataset[Row]` با عبارت‌های ستونی که Catalyst بهینه می‌کند |
| چرا lambda تایپ‌شده را در مسیر داغ نباید به کار برد؟ | Catalyst نمی‌تواند داخلش را ببیند، پس شرط‌ها و projectionها بهینه نمی‌شوند |
| ترتیب سرعت برای منطق سطری در Spark | توابع داخلی، بعد UDF روی JVM، بعد Pandas UDF، بعد UDF پایتونی ساده |
| `NotSerializableException` معمولاً از چه چیزی است؟ | یک شیء غیرقابل سریال‌سازی در closure، اغلب اتصالی که در driver ساخته شده |
| پارتیشن Kafka از روی کلید | هش کلید (partitioner پیش‌فرض): کلید یکسان، پارتیشن یکسان، ترتیب درونش |
| `acks=all` چه می‌کند؟ | producer منتظر تأیید replicaهای همگام می‌ماند |
| شکل job فلینک چیست؟ | منبع، بعد تبدیل‌ها (keyBy، پنجره، تجمیع)، بعد sink، بعد `execute` |

## 🔗 ادامه

- **pyspark-api** همان موتور را از سمت پایتون نشان می‌دهد، تا ببینی لایه‌ی JVM چه کاری برایت انجام می‌دهد.
- **spark-catalyst** بهینه‌سازی را توضیح می‌دهد که عبارت‌های ستونی را بر lambdaهای مبهم ترجیح می‌دهد.
- **flink** و **kafka-streams** موتورهای streaming را پوشش می‌دهند که APIهایشان در این درس آمده است.
- **kafka-producers** عمیق‌تر به batching، acks، idempotence و retry برای producerها می‌پردازد.
- **os-jvm-essentials** مفاهیم runtime جاوا (heap، GC، classloading) را پوشش می‌دهد که بیشتر خرابی‌های production اینجا را توضیح می‌دهند.
- قبل از نوشتن فایل build، نسخه‌های JDK و Scala پشتیبانی‌شده را برای انتشار دقیق Spark یا Flink خودت چک کن.
