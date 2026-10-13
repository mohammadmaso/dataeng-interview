---
order: 91
tid: "schema-registry"
track: "8"
section: null
num: "8.7"
title: "Schema Registry & Serialization"
type: "technology"
level: "advanced"
priority: "P1"
est_hours: 2
summary: "Schema Registry با Avro یا Protobuf، حالت‌های سازگاری، و جلوگیری از شکستن مصرف‌کننده‌ها. حل مسئله‌ی «producer فیلدی را حذف کرد و پایپ‌لاین شکست»."
covers: ["Confluent vs Apicurio vs Karapace","subject naming strategies"]
interview_qs: []
extras: []
prerequisites: ["kafka-producers"]
related: ["schema-evolution", "pattern-schema-evolution", "kafka-connect-ops", "data-formats", "governance-contracts-mesh"]
---

## 🎯 خلاصه

> **schema registry** سرویسی است که نسخه‌های schema پیام‌های شما را ذخیره می‌کند و هر نسخه‌ی جدید را با یک **قاعده‌ی سازگاری** چک می‌کند. جلوی این را می‌گیرد که producer قبل از نوشتن داده‌ی بد، consumerها را خراب کند.

- پیام‌های یک topic در Kafka بایت هستند. Avro و Protobuf آن‌ها را به رکوردهای تایپ‌شده تبدیل می‌کنند و schema می‌گوید بایت‌ها را چطور بخوانی.
- هر پیام فقط یک **schema id** کوچک حمل می‌کند. registry آن id را به schema کامل نگاشت می‌کند.
- **حالت سازگاری** (BACKWARD، FORWARD، FULL، NONE و شکل‌های `_TRANSITIVE` آن‌ها) مشخص می‌کند کدام تغییرات مجازند.
- بیشتر حادثه‌های «بعد از deploy پایپ‌لاین خراب شد» تغییر schemaای هستند که یک registry آن را رد می‌کرد.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 Analogy:** registry مثل یک **دفتر نگهداری قالب قرارداد** است. هر بار که یک تیم فرم قرارداد را عوض می‌کند، دفتر نسخه‌ی جدید را با نسخه‌ی قبلی مقایسه می‌کند. اگر خواننده‌های قدیمی دیگر فرم جدید را نفهمند، دفتر ثبتش نمی‌کند.

فرض کن producer این رکورد Avro را به topic `orders` می‌فرستد:

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "long"}
  ]
}
```

بدون registry، producer schema را در کدش می‌نویسد و امیدوار است هر consumer نسخه‌ی یکسانی داشته باشد. یک deploy، `amount` را حذف می‌کند و یک consumer ساعت ۳ صبح کرش می‌کند. با registry، producer اول schema را ثبت می‌کند. registry تغییر را چک می‌کند و اگر consumerها خراب می‌شدند، آن را رد می‌کند.

بایت‌های یک پیام روی سیم این شکل را دارند:

```text
[ magic byte = 0 ][ schema id = 4 bytes ][ Avro-encoded payload ]
```

consumer شناسه‌ی schema را می‌خواند، schema را می‌گیرد (و کش می‌کند) و payload را decode می‌کند. پیام کل schema را حمل نمی‌کند، پس کوچک می‌ماند.

> **⚠️ Watch out:** registry داده‌ی شما را چک نمی‌کند؛ **تغییر schema** را چک می‌کند. یک فیلد می‌تواند از نظر نوع سازگار باشد و معنای اشتباه داشته باشد. بازبینی schema هنوز لازم است.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### اجزای اصلی

- **Subject:** لیستی نام‌دار از نسخه‌های schema. با استراتژی نام‌گذاری پیش‌فرض، subject برابر `<topic>-value` است (و `<topic>-key` برای کلیدها). پس topic `orders` دارای subject به نام `orders-value` با نسخه‌های ۱، ۲، ۳ و ... است.
- **Serializer / deserializer:** کتابخانه‌های کلاینت (مثلاً `KafkaAvroSerializer` در کلاینت‌های Confluent) schema را ثبت یا جست‌وجو می‌کنند، schema id را به بایت‌ها اضافه می‌کنند و در خواندن معکوسش می‌کنند.
- **Schema id:** سراسری است. همان schema حتی بین subjectها id یکسان می‌گیرد، و به همین دلیل پیام‌ها کوچک می‌مانند.

کلاینت‌ها با `schema.registry.url` (یا تنظیم معادل در کتابخانه‌ی کلاینت) به registry وصل می‌شوند.

### استراتژی‌های نام‌گذاری subject

| استراتژی | نام subject | مناسب وقتی |
|---|---|---|
| TopicNameStrategy (پیش‌فرض) | `<topic>-value` | یک نوع رکورد به‌ازای هر topic |
| RecordNameStrategy | `<fully.qualified.RecordName>` | چند نوع رکورد یک topic را شریک هستند |
| TopicRecordNameStrategy | `<topic>-<RecordName>` | چند نوع در هر topic، ولی محدود به همان topic |

اگر در یک topic چند نوع رکورد را با استراتژی پیش‌فرض ترکیب کنی، خطاهای گیج‌کننده می‌گیری، چون هر subject فقط می‌تواند یک «خانواده» schema را نگه دارد. استراتژی را موقع ساخت topic انتخاب کن، نه بعداً.

### حالت‌های سازگاری

حالت سازگاری برای هر subject (یا به‌صورت سراسری) تنظیم می‌شود. registry نسخه‌ی جدید را با نسخه‌ی (یا نسخه‌های) قبلی مقایسه می‌کند.

| حالت | قاعده | تغییر امن برای نسخه‌ی جدید |
|---|---|---|
| BACKWARD (پیش‌فرض در Confluent) | schema جدید می‌تواند داده‌ی نوشته‌شده با schema قدیمی را بخواند | حذف فیلد؛ اضافه کردن فیلد اختیاری با default |
| FORWARD | schema قدیمی می‌تواند داده‌ی نوشته‌شده با schema جدید را بخواند | اضافه کردن فیلد؛ حذف فیلدی که default داشت |
| FULL | هر دو جهت هم‌زمان | فقط تغییراتی که در هر دو جهت امن‌اند، مثل اضافه کردن فیلد با default |
| NONE | بدون چک | هر چیزی؛ حادثه‌ها از این‌جا شروع می‌شوند |
| `*_TRANSITIVE` | در برابر **همه‌ی** نسخه‌های قبلی چک می‌کند، نه فقط آخری | وقتی داده‌ی قدیمی ممکن است سال‌ها در topic بماند |

در Avro، «اضافه کردن فیلد» یعنی فیلد جدید باید `default` داشته باشد. بدون default، داده‌ی قدیمی را نمی‌شود با schema جدید خواند، پس registry در حالت BACKWARD آن را رد می‌کند. تغییر نام فیلد سازگار نیست، مگر این‌که `alias` اضافه کنی.

در Protobuf، سازگاری از **شماره‌ی فیلد** می‌آید. هرگز شماره‌ی فیلد حذف‌شده را دوباره استفاده نکن و شماره‌های بازنشسته را `reserved` علامت بزن. فیلدهای جدید باید `optional` باشند یا مقدار پیش‌فرض داشته باشند.

### یک مثال کامل

نسخه‌ی ۱ از `orders-value` همان schema بالاست. نسخه‌ی ۲ یک فیلد با default اضافه می‌کند:

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "currency", "type": "string", "default": "IRR"}
  ]
}
```

در حالت BACKWARD این پذیرفته می‌شود: یک consumer جدید وقتی پیام قدیمی را می‌خواند، `currency` را با `IRR` پر می‌کند. نسخه‌ی ۳ فیلد `amount` را حذف می‌کند:

- در حالت BACKWARD، registry حذف فیلد را می‌پذیرد، چون خواننده‌ی جدید داده‌ی قدیمی غیرضروری را نادیده می‌گیرد. ولی کدی که **انتظار** `amount` را دارد الان خراب می‌شود. سازگاری با امنیت معنایی یکی نیست.
- در حالت FORWARD، اگر consumerهای قدیمی برای `amount` مقدار پیش‌فرض نداشته باشند، registry آن را رد می‌کرد، چون خواننده‌های قدیمی نمی‌توانستند داده‌ی جدید را بخوانند.

به همین دلیل انتخاب حالت یک تصمیم تیمی است، نه یک پیش‌فرضی که فراموشش کنی.

### تغییر رد‌شده، از ابتدا تا انتها

registry یک خطای HTTP برمی‌گرداند و producer قبل از نوشتن هر رکوردی شکست می‌خورد. همین برد است: schema بد هرگز به topic نمی‌رسد.

## 🧠 عمق و trade-offها (سنیور)

**انتخاب حالت.** برای topicهایی که داده‌ی بلندمدت به lake یا جدول‌های Iceberg می‌دهند از BACKWARD_TRANSITIVE یا FULL_TRANSITIVE استفاده کن. consumerهای تو ممکن است روی نسخه‌های خیلی قدیمی باشند و بررسی transitive از همه‌شان محافظت می‌کند. برای topicهای داخلی کوتاه‌عمر که تیم همه‌ی خواننده‌ها را کنترل می‌کند، BACKWARD ساده کافی است.

**ترتیب deploy.** حالت تعیین می‌کند کدام طرف اول ارتقا پیدا کند:

- BACKWARD: اول consumerها، بعد producerها.
- FORWARD: اول producerها، بعد consumerها.
- FULL: هر ترتیبی، ولی مجموعه‌ی تغییرات کوچک‌تر است.

این را در فرایند تغییرات بنویس. بیشتر outageها از ترتیب اشتباه deploy می‌آیند، حتی وقتی schema معتبر است.

**registry به‌عنوان وابستگی.** هر producer از آن می‌نویسد و هر consumer در cache miss از آن می‌خواند. آن را highly available اجرا کن (چند نود پشت load balancer)، storage آن را به یک topic replicate شده‌ی Kafka بسپار (پیش‌فرض topic داخلی `_schemas` در Confluent) و cacheهای کلاینت را طوری تنظیم کن که یک قطعی کوتاه registry جلوی ترافیکی را که schema id هایش کش شده‌اند نگیرد.

**Confluent در برابر Apicurio در برابر Karapace.**

- **Confluent Schema Registry:** پیاده‌سازی مرجع. بیشترین پشتیبانی اکوسیستم را دارد؛ سرور تحت Confluent Community License است، پس برای استفاده‌ات license را چک کن.
- **Apicurio Registry:** متن‌باز، با Avro، Protobuf، JSON Schema و OpenAPI/AsyncAPI. جزئیات REST API متفاوت است، پس سازگاری کلاینت‌ها باید تست شود.
- **Karapace:** متن‌باز، با هدف سازگاری رفتار REST و wire با Confluent. endpointها و حالت‌هایی را که لازم داری تأیید کن.

هر کدام را انتخاب کنی، serializerی را که کلاینت‌هایت استفاده می‌کنند با آن تست کن، چون فرمت wire و REST API چیزی هستند که باید تطابق داشته باشند.

**تکامل schema فراتر از سازگاری.** registry ساختار را چک می‌کند. این‌ها را تشخیص نمی‌دهد:

- تغییر واحد (`amount` از ریال به تومان برود)،
- تغییر معنای یک کد وضعیت،
- فیلدی که در عمل مقدار null پیدا می‌کند.

این‌ها را در data contract و تست بگذار. برای تصویر گسترده‌تر [[schema-evolution]] و [[data-contract]] را ببین.

**حالت‌های خرابی.**

- producerهایی با حالت `NONE`، معمولاً چون کسی چک‌ها را «موقتاً» خاموش کرده.
- کلاینت‌هایی که schema را در کد هاردکد می‌کنند و registry را دور می‌زنند، پس registry هرگز تغییر را نمی‌بیند.
- cache miss در زمان قطعی registry که سیل درخواست‌ها می‌سازد.
- نام‌گذاری subjectها که بین تیم‌ها فرق می‌کند، پس دو subject دو نسخه از «همان» داده دارند.

**چه چیزی را پایش کنی.** خطاهای ثبت و کدهایشان، نرخ نسخه‌های جدید در هر subject (جهش ناگهانی معمولاً یعنی باگ)، خطاهای چک سازگاری در CI، latency registry و تعداد lookupها در ثانیه.

## 🧪 امتحان کن

registry را به‌صورت محلی اجرا کن (مثلاً با یک container از Confluent یا Karapace) و REST API را امتحان کن. دستورها endpointهای سبک Confluent را به کار می‌برند؛ مسیرهای دقیق را در docs سرورت چک کن.

```bash
REG=http://localhost:8081
SUBJ=orders-value

# 1. Set compatibility for this subject
curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' $REG/config/$SUBJ

# 2. Register version 1 (schema is a JSON string inside the body)
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"long\"}]}"}' \
  $REG/subjects/$SUBJ/versions

# 3. Ask whether a candidate version (drops amount) is compatible with the latest
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"}]}"}' \
  $REG/compatibility/subjects/$SUBJ/versions/latest
```

سؤال‌هایی که بعد از اجرا باید جواب بدهی:

1. آیا چک مرحله‌ی ۳ تحت BACKWARD رد می‌شود؟ تحت FORWARD چطور؟
2. حالت را روی `FORWARD` بگذار و مرحله‌ی ۳ را دوباره اجرا کن. چه چیزی عوض می‌شود و چرا؟
3. قبل از این تغییر به کدام تیم اول خبر می‌دهی و deploy را به چه ترتیبی انجام می‌دهی؟

> **🎤 Say this in the interview:** «من schema را در یک registry نگه می‌دارم و حالت سازگاری را برای هر topic انتخاب می‌کنم. برای topicی که به lake می‌رود از شکل transitive استفاده می‌کنم تا consumerهای قدیمی امن بمانند. registry تغییرات ناسازگار را در CI و هنگام ثبت متوقف می‌کند. با این حال تغییرات معنایی را بازبینی می‌کنم، چون registry فقط ساختار را چک می‌کند، و ترتیب deployای را که حالت انتخابی لازم دارد مستند می‌کنم.»

## 💬 گوشه‌ی مصاحبه

**سؤال: یک producer فیلدی را حذف کرد و پایپ‌لاین شکست. چه می‌کنی؟**
- **مبتدی:** فیلد را برمی‌گردانم، یا consumer را طوری به‌روز می‌کنم که دیگر آن را نخواند. بعد برای deploy بعدی چک registry می‌خواهم.
- **متوسط:** حالت سازگاری subject را چک می‌کنم. اگر NONE بود، یا تغییر تحت BACKWARD مجاز بود ولی consumer به فیلد تکیه داشت، تست قرارداد اضافه می‌کنم و ترتیب deploy را درست می‌کنم.
- **سنیور:** این را یک incident با راه‌حل فرایندی می‌بینم. حالت transitive تنظیم می‌کنم، چک‌های registry را در CI اضافه می‌کنم، ترتیب deploy را در runbook می‌نویسم و برای فیلدهای معنایی data contract می‌گذارم. بعد خسارت را بررسی می‌کنم: پیام‌هایی که با شکل خراب نوشته شده‌اند را پیدا می‌کنم و اگر جدول‌های پایین‌دستی غلط‌اند، backfill برنامه‌ریزی می‌کنم.

**سؤال: BACKWARD یا FORWARD؟**
- **متوسط:** BACKWARD خواننده‌ی جدید را در برابر داده‌ی قدیمی محافظت می‌کند. FORWARD خواننده‌ی قدیمی را در برابر داده‌ی جدید محافظت می‌کند. حالت، ترتیب امن deploy را تعیین می‌کند.
- **سنیور:** به این بستگی دارد که اول چه چیزی را می‌توانی ارتقا بدهی، و به داده‌ای که در storage با نگه‌داری بلندمدت می‌ماند. برای lake از شکل transitive استفاده کن.

**جواب نمونه (سنیور):** من یک schema registry با Avro یا Protobuf استفاده می‌کنم تا producer نتواند schema ناسازگار منتشر کند. حالت سازگاری را برای هر subject تنظیم می‌کنم، برای داده‌ی با نگه‌داری بلندمدت از شکل transitive استفاده می‌کنم و ترتیب deploy متناظر را می‌نویسم. registry ساختار را چک می‌کند، پس برای تغییرات معنایی هنوز از data contract و تست استفاده می‌کنم.

## ⚠️ اشتباهات رایج

- **نگه داشتن compatibility روی NONE** با این توجیه که «فقط برای این release». و بعد همان‌جا می‌ماند.
- **اضافه کردن فیلد بدون default** در Avro. در حالت BACKWARD رد می‌شود و در هر حالت transitive هم رد می‌شود.
- **استفاده‌ی دوباره از شماره‌ی فیلد Protobuf** بعد از حذف فیلد. حالا داده‌ی قدیمی و جدید درباره‌ی معنای آن شماره اختلاف دارند.
- **فرض این‌که سازگاری یعنی درستی.** یک تغییر سازگار از نظر نوع می‌تواند معنا را عوض کند.
- **نادیده گرفتن نام‌گذاری subject.** استراتژی پیش‌فرض وقتی یک topic چند نوع رکورد دارد می‌شکند.
- **deploy با ترتیب اشتباه** برای حالت انتخابی، حتی وقتی schema معتبر است.
- **دور زدن registry** با هاردکد کردن schema در consumerها.

## ✅ خودت را بسنج

1. کدام تغییر در Avro تحت سازگاری BACKWARD پذیرفته می‌شود؟
   - A) اضافه کردن فیلد بدون default
   - B) اضافه کردن فیلد با default
   - C) تغییر `long` به `string`
   - D) تغییر نام `amount` به `total` بدون alias

   <details><summary>Answer</summary>

   **B** — خواننده‌ی جدید وقتی داده‌ی قدیمی را می‌خواند، فیلد جدید را از default آن پر می‌کند. گزینه‌ی A رد می‌شود چون داده‌ی قدیمی برای آن مقداری ندارد. گزینه‌های C و D یا نوع را عوض می‌کنند یا نام را بدون راهی برای خواندن داده‌ی قدیمی.

   </details>

2. علاوه بر payload، پیام روی سیم چه چیزی حمل می‌کند؟
   - A) کل schema
   - B) یک magic byte و یک schema id
   - C) یک timestamp و checksum از schema
   - D) هیچ‌چیز؛ consumer schema را حدس می‌زند

   <details><summary>Answer</summary>

   **B** — فرمت شامل یک magic byte، یک schema id چهار بایتی و بعد payload کدشده است. consumer schema را با id می‌گیرد و کش می‌کند.

   </details>

3. **سناریو:** یک تیم می‌خواهد `amount` را از topicی حذف کند که یک جدول lake دوساله را تغذیه می‌کند. کدام حالت احتمال گرفتن این تغییر را بیشتر می‌کند و کدام را انتخاب می‌کنی؟
   - A) NONE، چون همه‌چیز را مجاز می‌کند
   - B) FORWARD_TRANSITIVE یا FULL_TRANSITIVE، چون خواننده‌های قدیمی در همه‌ی نسخه‌ها باید کار کنند
   - C) BACKWARD بدون transitive، چون فقط آخرین نسخه را چک می‌کند
   - D) هر حالتی به یک اندازه خوب است

   <details><summary>Answer</summary>

   **B** — حالت‌های transitive با همه‌ی نسخه‌های قبلی مقایسه می‌کنند، پس داده‌ای که سال‌ها پیش نوشته شده هنوز قابل خواندن می‌ماند. BACKWARD ساده فقط آخرین نسخه را چک می‌کند و ممکن است زنجیره‌ای از تغییرات تک‌تک امن، که با هم داده‌ی قدیمی را می‌شکنند، را از دست بدهد.

   </details>

4. **trade-off سنیور:** چرا سازگاری از هر حادثه‌ای محافظت نمی‌کند؟
   - A) چون registry schemaها را ذخیره نمی‌کند
   - B) چون ساختار را چک می‌کند، نه معنا مثل واحد یا کدهای وضعیت
   - C) چون Kafka شناسه‌های schema را نادیده می‌گیرد
   - D) چون serializerها همیشه سازگاری را نادیده می‌گیرند

   <details><summary>Answer</summary>

   **B** — سازگاری ساختاری تغییر واحد یا تغییر معنای یک مقدار را تشخیص نمی‌دهد. این‌ها به data contract، تست و بازبینی نیاز دارند.

   </details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| Subject (نام پیش‌فرض) | `<topic>-value`، نگه‌دارنده‌ی لیستی از نسخه‌های schema |
| پیشوند فرمت wire | magic byte و schema id چهار بایتی، بعد payload کدشده |
| BACKWARD | schema جدید می‌تواند داده‌ی قدیمی را بخواند |
| FORWARD | schema قدیمی می‌تواند داده‌ی جدید را بخواند |
| FULL | هر دو جهت |
| `*_TRANSITIVE` | چک در برابر همه‌ی نسخه‌ها، نه فقط نسخه‌ی قبلی |
| قاعده‌ی افزودن فیلد در Avro | فیلد جدید برای سازگاری به default نیاز دارد |
| قاعده‌ی Protobuf | شماره‌ی فیلد حذف‌شده را هرگز دوباره استفاده نکن؛ آن را `reserved` علامت بزن |
| TopicRecordNameStrategy | subject به شکل `<topic>-<RecordName>`، برای چند نوع رکورد در هر topic |

## 🔗 ادامه

- [[schema-evolution]] — مسئله‌ی کلی و الگوهای پشت آن
- [[pattern-schema-evolution]] — یک الگوی طراحی برای تغییر امن schema
- [[kafka-connect-ops]] — converterهای آگاه از registry در Kafka Connect
- [[data-formats]] — مقایسه‌ی Avro، Protobuf و Parquet
- docs مربوط به registry خودت برای endpointها و حالت‌های دقیق سازگاری در نسخه‌ی خودت
