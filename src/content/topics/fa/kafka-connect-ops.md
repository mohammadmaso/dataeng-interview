---
order: 92
tid: "kafka-connect-ops"
track: "8"
section: null
num: "8.8"
title: "Kafka Connect & Operations"
type: "technology"
level: "advanced"
priority: "P1"
est_hours: 4
summary: "Kafka Connect (source/sink, distributed workers, SMTs), MirrorMaker 2 for cross-DC replication, cluster sizing, monitoring (under-replicated partitions, lag), and Strimzi on Kubernetes."
covers: ["rack awareness","partition reassignment","quotas","Cruise Control"]
interview_qs: []
extras: []
prerequisites: ["kafka-architecture", "kafka-producers", "kafka-consumers"]
related: ["kafka-retention-compaction", "schema-registry", "dlq-bad-records", "cdc", "kubernetes-for-data", "fault-tolerance"]
---

## 🎯 خلاصه

> **Kafka Connect** داده را با تنظیمات (نه با کد) بین Kafka و سیستم‌های دیگر جابه‌جا می‌کند. **عملیات** Kafka یعنی سالم، replicate‌شده، متعادل و قابل‌مشاهده نگه داشتن brokerها، و دانستن این‌که کدام metric اولین نشانه‌ی مشکل است.

- یک **source** connector داده را از یک سیستم بیرونی به Kafka می‌آورد. یک **sink** connector داده را از Kafka به سیستم بیرونی می‌نویسد.
- در **حالت distributed**، workerها کار را بین خودشان تقسیم می‌کنند. config، offset و وضعیت connector در topicهای Kafka ذخیره می‌شوند، پس اگر یک worker خراب شود، taskهایش به worker دیگری می‌روند.
- **MirrorMaker 2** replication مبتنی بر Connect بین کلاسترهاست، برای disaster recovery و چندمنطقه‌ای.
- metricهایی که اول مهم‌اند: **under-replicated partitions**، **offline partitions**، **consumer lag** و **latency درخواست‌ها**.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 Analogy:** Kafka Connect یک **سرویس پیک** با مسیرهای از پیش‌ساخته است. راننده را خودت نمی‌نویسی. فرم را پر می‌کنی: از این انبار برداشت کن (source)، به آن فروشگاه تحویل بده (sink). پیک‌ها (workerها) مسیرها را اداره می‌کنند و وقتی یکی مریض می‌شود، مسیرها را به بقیه می‌دهند.

یک config ساده‌ی sink برای PostgreSQL که به REST API ارسال می‌شود:

```json
{
  "name": "orders-to-postgres",
  "config": {
    "connector.class": "io.aiven.connect.jdbc.JdbcSinkConnector",
    "topics": "orders",
    "connection.url": "jdbc:postgresql://db:5432/shop",
    "connection.user": "loader",
    "connection.password": "${file:/secrets/db.properties:password}",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "tasks.max": "3",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "orders.dlq"
  }
}
```

سه نکته را ببین:

- `tasks.max` **موازی‌سازی** است. یک سقف بالایی است؛ connector خودش تعیین می‌کند چند task واقعاً می‌سازد.
- `errors.tolerance=all` همراه با dead-letter queue باعث می‌شود رکورد بد به `orders.dlq` برود، به‌جای این‌که connector را متوقف کند.
- رمز عبور از یک فایل secret می‌آید، نه متن ساده داخل config.

> **⚠️ Watch out:** `tasks.max=3` یعنی سه برابر توان عملیاتی نیست. یک sink connector فقط بین partitionهای topic موازی می‌شود، و یک source connector به این بستگی دارد که سیستم بیرونی چطور قابل تقسیم است.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### standalone در برابر distributed

- **Standalone** یک پروسه با config در یک فایل اجرا می‌کند. برای توسعه خوب است، ولی تحمل خرابی ندارد.
- **Distributed** چند worker با `group.id` یکسان اجرا می‌کند. connectorها از طریق REST API (به‌صورت پیش‌فرض پورت 8083) ساخته می‌شوند. workerها از طریق سه topic داخلی برای config، offset و status هماهنگ می‌شوند.

اگر یک worker بمیرد، گروه دوباره توزیع می‌شود و taskها روی بقیه اجرا می‌شوند. نسخه‌های جدید از cooperative rebalancing استفاده می‌کنند که فقط taskهای تحت تأثیر را جابه‌جا می‌کند، نه این‌که همه را متوقف کند.

### منبع‌ها، offsetها و sinkها

| سمت | جایی که پیشرفت ذخیره می‌شود | هنگام restart چه می‌شود |
|---|---|---|
| Source | offsetهای تعریف‌شده توسط connector (مثلاً موقعیت یک log) در topic offsetها | از موقعیت source ذخیره‌شده ادامه می‌دهد |
| Sink | offsetهای consumer group خود connector (`connect-<name>`) | از offset commit شده‌ی Kafka ادامه می‌دهد |

یک sink که در دیتابیس می‌نویسد به‌صورت پیش‌فرض **at-least-once** است: ممکن است رکوردی را بنویسد، قبل از commit کردن offset کرش کند، و دوباره بنویسدش. راه‌حل نوشتن idempotent است، مثل upsert روی کلید اصلی. این همان استدلال [[kafka-exactly-once]] است.

### converterها و transformها

- **Converterها** (`key.converter`، `value.converter`) تعیین می‌کنند بایت‌ها چطور به ساختار تبدیل شوند. با Avro از converter آگاه از registry استفاده می‌کنی که schema را در [[schema-registry]] نگه می‌دارد.
- **SMT** (single message transform) روی هر رکورد تغییر می‌دهد: نام فیلد را عوض می‌کند، مقدار را mask می‌کند، به topic دیگری می‌فرستد. ساده نگهشان دار. هر کاری که state یا join لازم دارد باید در یک stream processor باشد.

### مدیریت خطا

با `errors.tolerance=none` (پیش‌فرض)، یک رکورد بد task را متوقف می‌کند. با `all` و یک topic dead-letter، رکورد بد همراه با هدرهای خطا به DLQ می‌رود و بقیه جریان دارند. الگوی کلی را در [[dlq-bad-records]] ببین. DLQ فقط وقتی مفید است که کسی آن را بخواند و برنامه‌ی replay داشته باشد.

### MirrorMaker 2

MirrorMaker 2 (MM2) روی Kafka Connect اجرا می‌شود. topicها را از یک کلاستر source به یک کلاستر target کپی می‌کند و سه کار اضافه هم دارد:

- **نام topicها را با alias کلاستر مبدأ تغییر می‌دهد.** به‌صورت پیش‌فرض topic `orders` از کلاستر `eu` در target می‌شود `eu.orders`، پس مبدأ همیشه قابل تشخیص است و حلقه‌ها جلوگیری می‌شوند.
- **offsetهای consumer groupها را sync می‌کند** تا consumerها بتوانند به کلاستر target منتقل شوند و نزدیک نقطه‌ی توقف ادامه دهند.
- **checkpoint و heartbeat می‌نویسد** تا بتوانی lag replication را اندازه بگیری و زنده بودن لینک را چک کنی.

offsetها در target با offsetهای source یکی نیستند. MM2 آن‌ها را با داده‌ی offset-sync ترجمه می‌کند، پس failover به offsetهای ترجمه‌شده نیاز دارد، نه offsetهای خام.

```bash
# Start MM2 with a properties file that describes the clusters and flows
connect-mirror-maker.sh mm2.properties
```

## 🧠 عمق و trade-offها (سنیور)

### اندازه‌گذاری

اندازه‌گذاری از توان عملیاتی، retention و replication می‌آید. یک تخمین ساده:

```text
write throughput      = 50 MB/s
replication factor    = 3          → 150 MB/s of disk writes across the cluster
retention             = 3 days = 259200 s
raw data on disk      = 50 MB/s × 259200 s ≈ 12,960,000 MB ≈ 12.96 TB
with replication      = 12.96 TB × 3 ≈ 38.9 TB
per broker (6 brokers) ≈ 6.5 TB, plus 30–40% headroom → plan ~9 TB each
```

بعد محدودیت‌های دیگر را چک کن: تعداد partition روی هر broker (زیادی بودن leader election را کند و تعداد file handle را زیاد می‌کند)، پهنای باند شبکه (replication و consumerها ترافیک را چندبرابر می‌کنند) و حافظه برای page cache. قبل از اعتماد به هر تخمینی اندازه بگیر.

### rack awareness

`broker.rack` به Kafka می‌گوید هر broker به کدام failure domain تعلق دارد (rack، zone، دیتاسنتر). با جای‌گذاری آگاه از rack، replicaهای یک partition به rackهای مختلف می‌روند، پس از دست رفتن یک rack همه‌ی کپی‌ها از بین نمی‌روند. در cloud معمولاً همان availability zone است. قبل از اولین deploy درستش کن، چون تغییر دیرتر یعنی جابه‌جایی replicaها.

### reassignment پارتیشن و quota

- **Reassignment** replicaهای partition را بین brokerها جابه‌جا می‌کند، مثلاً بعد از اضافه کردن broker. داده را روی شبکه کپی می‌کند، پس یک throttle برای replication تنظیم کن و در دسته‌های کوچک اجرایش کن. وگرنه با ترافیک زنده رقابت می‌کند و کلاستر کند می‌شود.
- **Quota** producer و consumer را بر اساس client id یا user محدود می‌کند: بایت بر ثانیه (`producer_byte_rate`، `consumer_byte_rate`) و سهم درخواست (`request_percentage`). quota اولین دفاع در برابر یک تیم پرمصرف روی کلاستر مشترک است.

### Cruise Control

Cruise Control ابزار متن‌باز برای پایش بار کلاستر و پیشنهاد جای‌گذاری متعادل replicaهاست. می‌تواند broker اضافه یا حذف کند با کمترین اختلال، و ناهنجاری‌هایی مثل خرابی broker یا عدم تعادل دیسک را تشخیص دهد. وقتی reassignment دستی کند یا مستعد خطاست از آن استفاده کن. جای monitoring را نمی‌گیرد: هنوز هشدار لازم است.

### Strimzi روی Kubernetes

Strimzi یک Kubernetes operator است که کلاسترهای Kafka، کلاسترهای Connect، connectorها و MirrorMaker 2 را به‌صورت custom resource مدیریت می‌کند. operator ریستارت‌های rolling، چرخش certificate و scale را انجام می‌دهد. وقتی از قبل Kubernetes داری، operatorها مناسب‌اند؛ trade-offهای کلی را در [[kubernetes-for-data]] ببین. ریسک این است که تیمی رفتار Kafka زیرِ operator را نفهمد و کورکورانه به آن اعتماد کند.

### چه چیزی را پایش کنی

| سیگنال | معنی | اولین اقدام |
|---|---|---|
| Under-replicated partitions > 0 | یک follower عقب است یا یک broker خراب است | پیدا کردن broker؛ چک دیسک، شبکه و GC |
| Offline partitions > 0 | برای بعضی partitionها leader نیست. producer و consumer برای آن‌ها خطا می‌گیرند | فوری هشدار بده؛ سلامت broker و unclean leader election را چک کن |
| consumer lag رو به رشد | consumerها کندتر از producerها هستند | توان consumer، rebalanceها و sinkهای پایین‌دستی را چک کن |
| task connector در وضعیت `FAILED` | یک task روی یک رکورد یا اتصال کرش کرده | خطا را در endpoint وضعیت بخوان؛ بعد از رفع علت restart کن |
| latency درخواست (p99) | brokerها یا دیسک‌ها اشباع شده‌اند | استفاده‌ی دیسک و صف درخواست را چک کن |

partition با replication ناکامل بهترین هشدار زودهنگام است. افزایش این عدد روی تعداد زیادی partition معمولاً یعنی یک broker در تنگنا است.

### trade-offها و حالت‌های خرابی

- **Connect در برابر کد سفارشی.** Connect تلاش مجدد، ردیابی offset و scale را رایگان می‌دهد. هزینه‌اش یک runtime برای عملیات و یک اکوسیستم connector برای بررسی است. برای یک سیستم خاص و غیرمعمول، یک سرویس consumer کوچک ممکن است ساده‌تر باشد.
- **restart و rebalance.** rebalanceهای مکرر پردازش را متوقف می‌کنند. taskهای طولانی که از session timeout بگذرند بیرون انداخته می‌شوند و دوباره شروع می‌شوند و یک حلقه می‌سازند. timeoutها را تنظیم کن و poll loopها را کوتاه نگه دار.
- **تکرار بی‌صدای sink** وقتی مقصد upsert را پشتیبانی نمی‌کند. قبل از این‌که به کسب‌وکار exactly-once قول بدهی، تضمین‌های تحویل connector را چک کن.
- **drift در schema.** converterای که نمی‌تواند schema جدید را بخواند، هر رکورد را به خطا تبدیل می‌کند. converterها را با registry و قواعد سازگاری همراه کن.

## 🧪 امتحان کن

از Connect REST API روی یک کلاستر محلی استفاده کن. مسیرهای زیر استاندارد هستند؛ برای فیلدهای اضافه‌ی نسخه‌ی خودت docs را چک کن.

```bash
CONNECT=http://localhost:8083

# List connectors and their state
curl -s $CONNECT/connectors

# Get status: connector and each task's state
curl -s $CONNECT/connectors/orders-to-postgres/status

# Pause a connector (tasks stop, offsets are kept)
curl -s -X PUT $CONNECT/connectors/orders-to-postgres/pause

# Restart a failed task (id 0)
curl -s -X POST $CONNECT/connectors/orders-to-postgres/tasks/0/restart
```

بعد سلامت کلاستر را توصیف کن:

```bash
# Partitions with fewer in-sync replicas than the replication factor
kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions

# Consumer lag per group
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connect-orders-to-postgres
```

تمرین: یک broker را در یک کلاستر محلی سه‌brokerی متوقف کن و اولین دستور را دوباره اجرا کن. چه چیزی عوض می‌شود و روی کدام metric هشدار می‌گذاری؟

> **🎤 Say this in the interview:** «من Connect را در حالت distributed اجرا می‌کنم و taskها را بین workerها پخش می‌کنم، و sinkها را idempotent می‌کنم چون Connect at-least-once است. رکوردهای بد به یک DLQ با هدرهای خطا می‌روند. برای DR از MirrorMaker 2 و ترجمه‌ی offsetهای آن استفاده می‌کنم، نه offsetهای خام. برای سلامت روزانه اول under-replicated و offline partitions را پایش می‌کنم، بعد consumer lag را، به همین ترتیب.»

## 💬 گوشه‌ی مصاحبه

**سؤال: یک connector وضعیت `RUNNING` نشان می‌دهد ولی داده نمی‌رسد. چه چیزهایی را چک می‌کنی؟**
- **مبتدی:** این‌که خود connector و taskهایش هر دو در حال اجرا هستند. connector می‌تواند `RUNNING` باشد با یک task که خراب شده.
- **متوسط:** وضعیت taskها، offset منبع (آیا پیش رفته؟)، end offset topic و config مربوط به `topics` یا فیلتر. خطاهای converter را در لاگ‌ها چک کن.
- **سنیور:** مطمئن شو source واقعاً داده تولید می‌کند. topic offsetها را برای موقعیت‌های گیرکرده‌ی source چک کن، دنبال فیلتر بی‌صدا در SMTها بگرد و مسیر commit کردن sink را بسنج. بعد خود هشدار را اصلاح کن: وضعیت `RUNNING` به‌تنهایی نشانه‌ی سلامت نیست.

**سؤال: چطور به یک منطقه‌ی دوم failover کنی؟**
- **متوسط:** MirrorMaker 2 topicها و offsetهای consumer groupها را replicate می‌کند. consumerها و producerها را با استفاده از offsetهای ترجمه‌شده به کلاستر target ببر.
- **سنیور:** تمرینش کن. قبل از cutover، lag replication را چک کن، duplicateها را در consumerها هندل کن و مسیر برگشت (failback) را مستند کن. failover‌ای که هرگز تست نشده یک طرح است، نه یک قابلیت.

**جواب نمونه (سنیور):** من Connect توزیع‌شده را با sinkهای idempotent و یک DLQ برای رکوردهای بد اجرا می‌کنم. برای چندمنطقه‌ای از MirrorMaker 2 با ترجمه‌ی offset استفاده می‌کنم و failover را مرتب تست می‌کنم. برای عملیات اول under-replicated و offline partitions را پایش می‌کنم، بعد consumer lag را، و برای متعادل ماندن کلاستر از rack awareness، quota و reassignment کنترل‌شده استفاده می‌کنم.

## ⚠️ اشتباهات رایج

- **دیدن `RUNNING` به‌عنوان سلامت.** taskها می‌توانند خراب باشند و منبع‌ها می‌توانند گیر کرده باشند.
- **فرض این‌که `tasks.max` خطی مقیاس می‌شود.** موازی‌سازی به partitionها و طراحی connector بستگی دارد.
- **استفاده از پیش‌فرض `errors.tolerance=none`** و بعد از دست دادن کل یک pipeline به‌خاطر یک رکورد خراب.
- **ذخیره‌ی secretها در JSON config** به‌جای استفاده از config provider.
- **استفاده از offsetهای خام برای failover با MM2** به‌جای offsetهای ترجمه‌شده.
- **اجرای reassignment بدون throttle** که ترافیک زنده را کند می‌کند.
- **نادیده گرفتن rack awareness** و فهمیدن آن در اولین قطعی zone.

## ✅ خودت را بسنج

1. هدف اصلی هشدار under-replicated partition چیست؟
   - A) نشان می‌دهد topic partition زیادی دارد
   - B) هشدار زودهنگام است که یک follower عقب است یا یک broker سالم نیست
   - C) یعنی داده از دست رفته
   - D) فقط برای topicهای فشرده مهم است

   <details><summary>Answer</summary>

   **B** — partition تعداد in-sync replicaهایی کمتر از replication factor دارد. این به‌تنهایی از دست رفتن داده نیست، ولی حاشیه‌ی امنیت را کم می‌کند و اغلب قبل از یک outage می‌آید.

   </details>

2. یک sink connector در دیتابیس می‌نویسد و بعد از نوشتن ولی قبل از commit کردن offset کرش می‌کند. هنگام restart چه می‌شود؟
   - A) رکورد از بین می‌رود
   - B) رکورد دوباره نوشته می‌شود، پس sink باید idempotent باشد
   - C) Kafka نوشتن در دیتابیس را rollback می‌کند
   - D) connector برای همیشه متوقف می‌شود

   <details><summary>Answer</summary>

   **B** — تحویل at-least-once است. نوشتن با upsert روی کلید باعث می‌شود تکرار بی‌ضرر باشد.

   </details>

3. **سناریو:** لینک MirrorMaker 2 سالم است، ولی بعد از failover، consumerها در منطقه‌ی جدید بازه‌ی بزرگی از داده را دوباره پردازش می‌کنند. علت محتمل چیست؟
   - A) compaction روی target اجرا شده
   - B) consumerها offsetهای خام source را به‌جای offsetهای ترجمه‌شده استفاده کرده‌اند، یا offsetها دیر commit شده‌اند
   - C) rack awareness replicaها را جابه‌جا کرده
   - D) REST API مربوط به Connect ریستارت شده

   <details><summary>Answer</summary>

   **B** — MM2 offsetها را بین کلاسترها ترجمه می‌کند. استفاده از offsetهای ترجمه‌نشده یا offsetهای گروه قدیمی، consumer را به موقعیتی می‌برد که با log هدف هم‌خوان نیست، پس داده را دوباره پخش می‌کند.

   </details>

4. **trade-off سنیور:** چرا روی یک کلاستر مشترک از cooperative rebalancing و quota استفاده می‌کنیم؟
   - A) replication factor را زیاد می‌کنند
   - B) شعاع اثر یک تغییر یا یک کلاینت پرمصرف را محدود می‌کنند
   - C) نیاز به monitoring را حذف می‌کنند
   - D) MirrorMaker را غیرضروری می‌کنند

   <details><summary>Answer</summary>

   **B** — cooperative rebalancing از متوقف کردن همه‌ی taskها در هر تغییر جلوگیری می‌کند، و quota مانع می‌شود یک کلاینت ظرفیت مشترک را ببلعد. هر دو اثر یک اشتباه واحد را کم می‌کنند.

   </details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| Source connector | داده‌ی بیرونی را به Kafka می‌آورد |
| Sink connector | داده‌ی Kafka را به سیستم بیرونی می‌نویسد |
| `tasks.max` | سقف بالایی taskهای موازی؛ محدود به partitionها یا طراحی connector |
| Under-replicated partitions | partitionهایی با in-sync replicaهای کمتر از replication factor |
| alias در MirrorMaker 2 | topicهای target با alias کلاستر مبدأ پیشوند می‌گیرند، مثل `eu.orders` |
| `broker.rack` | برچسب failure domain برای جای‌گذاری replicaها آگاه از rack |
| Cruise Control | ابزاری که جای‌گذاری متعادل پیشنهاد می‌دهد و ناهنجاری بار را تشخیص می‌دهد |
| Strimzi | Kubernetes operator برای Kafka، Connect و MM2 |
| Connect REST API | پورت پیش‌فرض 8083؛ مدیریت connectorها، taskها و وضعیتشان |

## 🔗 ادامه

- [[kafka-architecture]] — brokerها، replicaها و controller
- [[kafka-exactly-once]] — چطور sinkهای at-least-once را امن کنیم
- [[schema-registry]] — converterها و سازگاری schema
- [[cdc]] — رایج‌ترین الگوی source در Connect
- [[fault-tolerance]] — استدلال کلی پشت replicaها و failover
- docs مربوط به Kafka و Connect برای نام metricها و endpointهای REST نسخه‌ی خودت
