---
order: 89
tid: "kafka-exactly-once"
track: "8"
section: null
num: "8.5"
title: "Kafka Exactly-Once & Transactions"
type: "concept"
level: "senior"
priority: "P0"
est_hours: 3
summary: "idempotent producer + transaction + read_committed. الگوی read-process-write اتمی، و محدودیت آن وقتی sink خارج از Kafka است."
covers: ["transactional.id","zombie fencing","EOS in Kafka Streams and Flink"]
interview_qs: []
extras: []
prerequisites: ["kafka-producers", "kafka-consumers"]
related: ["delivery-semantics", "kafka-streams", "flink", "pattern-exactly-once", "distributed-transactions"]
---

## 🎯 خلاصه

> **exactly-once** در Kafka برای **خواندن → پردازش → نوشتن داخل خود Kafka** است. سه چیز را ترکیب می‌کند: **idempotent producer** (retry ها تکرار ایجاد نمی‌کنند)، **transaction** (چند نوشتن و commit آفست‌های ورودی با هم انجام می‌شوند یا هیچ‌کدام)، و consumerهای **`read_committed`** (هرگز نوشته‌ی abortشده را نمی‌بینند).

- idempotence تکرارهای ناشی از **retry** یک producer را حذف می‌کند.
- transaction یک **دسته نوشتن به‌علاوه‌ی commit آفست‌ها** را اتمی می‌کند.
- **fencing** جلوی یک نمونه‌ی قدیمی و «زامبی» را می‌گیرد که بعد از شروع نمونه‌ی جدید هنوز commit کند.
- وقتی خروجی از Kafka بیرون می‌رود (دیتابیس، API، S3)، Kafka دیگر کمکی نمی‌کند. به sink ای idempotent یا two-phase commit نیاز داری.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 Analogy:** یک انتقال بانکی دو پای دارد: از حساب A کم کن، به حساب B اضافه کن. یا هر دو اتفاق می‌افتد یا هیچ‌کدام، و کسی نباید پولی را «در مسیر» ببیند. یک transaction در Kafka همین کار را برای گروهی از پیام‌ها و برای نشانک (آفست) آن‌چه خوانده‌ای انجام می‌دهد.

اول مسئله. producer پیامی می‌فرستد، شبکه timeout می‌شود، و producer دوباره تلاش می‌کند. ارسال اول ممکن بوده موفق باشد، پس الان پیام دو بار وجود دارد. این تحویل **at-least-once** است. به همین شکل، consumerی که بعد از نوشتن خروجی و قبل از commit آفست crash کند، همان ورودی را دوباره پردازش می‌کند.

```text
at-most-once  : may lose      (commit first, then process)
at-least-once : may duplicate (process first, then commit)   <- what plain Kafka gives you
exactly-once  : no loss, no duplicates of the effect (within Kafka)
```

Kafka حالت سوم را **فقط** برای محدوده‌ای که کنترل می‌کند می‌دهد. بقیه‌ی این درس دربارهٔ این است که آن محدوده کجا تمام می‌شود.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

**گام ۱: producer idempotent.** مقدار `enable.idempotence=true` را بگذار (در کلاینت‌های جدید پیش‌فرض است). broker به هر نشست producer یک **producer ID** می‌دهد. هر رکورد یک **شماره‌ی توالی per-partition** همراه خودش دارد. اگر retry ای با شماره‌ی توالی‌ای برسد که broker قبلاً ذخیره کرده، کنار گذاشته می‌شود.

- تضمین: تکرار ناشی از retry، برای هر partition، در یک نشست producer.
- هیچ تضمینی بین ری‌استارت برنامه ندارد. پروسه‌ی ری‌استارت‌شده producer ID جدید می‌گیرد.

**گام ۲: transaction.** یک `transactional.id` ثابت تنظیم کن. سپس producer:

1. `initTransactions()`: ID را نزد **transaction coordinator** ثبت می‌کند و **epoch** را بالا می‌برد.
2. `beginTransaction()` و بعد `send(...)` به هر تعداد partition.
3. `sendOffsetsToTransaction(...)`: آفست‌های ورودی consumer را به همان transaction اضافه می‌کند.
4. `commitTransaction()` یا `abortTransaction()`.

هنگام commit، coordinator **control marker** ها را در هر partition می‌نویسد. consumerهایی که `isolation.level=read_committed` دارند رکوردهای transactionهای abortشده را رد می‌کنند و هرگز از **last stable offset** (LSO) جلوتر نمی‌روند؛ LSO اولین آفست یک transaction است که هنوز باز است.

**گام ۳: fencing زامبی‌ها.** هر `transactional.id` به یک epoch فعلی وصل است. وقتی نمونه‌ی جدیدی با همان ID فراخوانی `initTransactions()` می‌کند، epoch بالا می‌رود. نمونه‌ی قدیمی، که شاید با یک GC pause طولانی یا پارتیشن شبکه متوقف شده، هنوز خودش را مالک می‌داند. درخواست بعدی‌اش با `ProducerFencedException` رد می‌شود و نمی‌تواند خروجی کهنه‌اش را commit کند.

**گام ۴: read-process-write.** اسکلت Java زیر الگوی استاندارد است. این فقط یک اسکلت است، پس `enrich()` با توست.

```java
props.put("transactional.id", "billing-enricher-1");   // stable per instance, not per run
props.put("enable.idempotence", "true");
// consumer side:
// enable.auto.commit=false, isolation.level=read_committed

producer.initTransactions();
while (true) {
    ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
    if (recs.isEmpty()) continue;
    producer.beginTransaction();
    try {
        for (ConsumerRecord<String, String> r : recs) {
            producer.send(new ProducerRecord<>("enriched", r.key(), enrich(r.value())));
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition tp : recs.partitions()) {
            List<ConsumerRecord<String, String>> list = recs.records(tp);
            offsets.put(tp, new OffsetAndMetadata(list.get(list.size() - 1).offset() + 1));
        }
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
        producer.commitTransaction();
    } catch (ProducerFencedException e) {
        producer.close();   // we are a zombie: do not abort, just stop
        throw e;
    } catch (Exception e) {
        producer.abortTransaction();
    }
}
```

خط کلیدی `sendOffsetsToTransaction` است. رکوردهای خروجی و پیشرفت ورودی بعد از commit یا هر دو قابل دیدن‌اند، یا هیچ‌کدام. crash قبل از commit یعنی ورودی دوباره خوانده می‌شود و خروجی دور ریخته می‌شود.

**APIهای سطح بالاتر همین کار را برایت انجام می‌دهند.**

- **Kafka Streams:** مقدار `processing.guarantee=exactly_once_v2` را بگذار (تنظیم قدیمی `exactly_once` منسوخ شده است). برای نسخه‌ی خودت مستندات را بررسی کن.
- **Flink:** sink کافکا با `DeliveryGuarantee.EXACTLY_ONCE` برای هر checkpoint یک transaction باز می‌کند و وقتی checkpoint کامل شد آن را commit می‌کند. checkpointing باید فعال باشد و خواننده‌های پایین‌دستی باید از `read_committed` استفاده کنند.

## 🧠 عمق و trade-offها (سنیور)

**مرز محدوده کل داستان است.** exactly-once داخل Kafka یعنی *اثرهای جانبی داخل Kafka* اتمی‌اند. یک sink خارجی، مثل دیتابیس رابطه‌ای یا API مبتنی بر HTTP، جزئی از transaction نیست. crash بین «نوشتن در DB» و «commit کردن transaction کافکا» می‌تواند باعث شود بعد از retry ردیف دو بار نوشته شود. گزینه‌ها از ارزان به قوی:

1. **sink idempotent:** نوشتن را upsert با کلید business قرار بده (`INSERT ... ON CONFLICT DO UPDATE`)، یا `(topic, partition, offset)` کافکا را داخل ردیف ذخیره کن تا replay جای ردیف قبلی را بگیرد و اضافه نکند. این بیشتر pipelineهای واقعی را پوشش می‌دهد.
2. **transactional outbox:** نوشتن business و رویداد خروجی در یک transaction دیتابیس انجام می‌شود؛ یک relay، outbox را به Kafka منتشر می‌کند. مرز به دیتابیس منتقل می‌شود.
3. **sink با two-phase commit:** الگوی `TwoPhaseCommitSinkFunction` در Flink، با sinkی که prepare و commit را پشتیبانی کند. کار می‌کند، اما گران و شکننده بین سیستم‌هاست.

**هزینه‌هایی که باید حساب کنی.**

- **تأخیر:** consumerهای `read_committed` داده را فقط موقع commit transaction می‌بینند. در Flink این تقریباً همان بازه‌ی checkpoint است؛ در طراحی‌های دیگر بازه‌ی commit. بازه‌ی کوتاه‌تر یعنی transaction بیشتر و overhead بیشتر.
- **timeoutها:** `transaction.timeout.ms` یک producer باید کمتر از سقف broker باشد. یک transaction باز (producer گیرکرده) LSO را برای خواننده‌های `read_committed` نگه می‌دارد، پس lag برای خواننده‌های `read_uncommitted` طبیعی به نظر می‌رسد ولی برای خواننده‌های committed بالا می‌رود.
- **throughput:** هر transaction رفت‌وبرگشت coordinator و marker اضافه می‌کند. اگر تأخیر اجازه می‌دهد، رکوردها را در transactionهای کمتر و بزرگ‌تر دسته‌بندی کن.
- **شکنندگی عملیاتی:** `transactional.id` باید برای هر نمونه‌ی منطقی ثابت باشد. اگر دو نمونه‌ی زنده ID مشترک داشته باشند، همدیگر را در حلقه fence می‌کنند. اگر ID در هر ری‌استارت عوض شود، transactionهای قدیمی هرگز پاک نمی‌شوند.

**چه چیزهایی را مانیتور کنیم.** خطاهای fenced producer (باید کم و قابل توضیح باشند)، نرخ transactionهای abortشده، زمان از باز شدن transaction تا commit، LSO lag برای consumerهای committed، و نسبت خروجی به ورودی در هر مرحله.

**کی نباید از transaction کافکا استفاده کنی.** اگر sink idempotent است و at-least-once همراه با dedup در sink را می‌پذیری، transaction فقط تأخیر و پیچیدگی اضافه می‌کند بدون سود. اگر pipeline هرگز در Kafka نمی‌نویسد، `read_committed` در سمت producer معنی ندارد؛ به جای آن نوشتن idempotent در مقصد استفاده کن.

## 🧪 امتحان کن

اول پیش‌بینی کن. یک برنامه‌ی Kafka Streams از `payments` می‌خواند، `payment-summaries` (در Kafka) می‌نویسد و برای هر رویداد یک API تقلب را هم صدا می‌زند. با فعال بودن `processing.guarantee=exactly_once_v2`:

- کدام بخش‌ها exactly-once هستند؟
- اگر API تقلب پرداخت را پذیرفته باشد، اما برنامه قبل از commit کافکا crash کند، چه اتفاقی برای آن پرداخت می‌افتد؟

<details><summary>حدست چیست؟</summary>

نوشته‌های Kafka در `payment-summaries` و آفست‌های consumer به‌صورت exactly-once هستند: یا اتمی commit می‌شوند یا دور ریخته می‌شوند. فراخوانی API تقلب بیرون از transaction است. اگر برنامه بعد از فراخوانی API و قبل از commit crash کند، همان پرداخت را دوباره می‌خواند و API را دوباره صدا می‌زند. exactly-once این اثر جانبی را پوشش نمی‌دهد. راه‌حل این است که API تقلب idempotent و کلیدش شناسه‌ی پرداخت باشد، یا فراخوانی را از مسیر اصلی بیرون ببری و به مرحله‌ای جدا و deduplicated تبدیل کنی.

</details>

## 💬 گوشه‌ی مصاحبه

**س: آیا Kafka تحویل exactly-once می‌دهد؟**

> **🎤 Say this in the interview:** «Kafka پردازش exactly-once را *داخل خودش* می‌دهد: producerهای idempotent تکرار retry را متوقف می‌کنند، transactionها آفست‌های خروجی و ورودی را اتمی commit می‌کنند، و consumerهای `read_committed` داده‌ی abortشده را پنهان می‌کنند. این تضمین خودکار به سیستم‌های خارجی نمی‌رسد. برای sink دیتابیسی از upsert idempotent با شناسه‌ی business یا transactional outbox استفاده می‌کنم و می‌گویم تضمین end-to-end فقط به اندازه‌ی sink قوی است.»

**س: producer زامبی چیست و Kafka چطور با آن برخورد می‌کند؟**

> **🎤 Say this in the interview:** «نمونه‌ی قدیمی‌ای است که بعد از این‌که نمونه‌ی جدید جایش را گرفته هنوز اجرا می‌شود، مثلاً بعد از یک GC pause طولانی. Kafka برای هر `transactional.id` یک epoch نگه می‌دارد. وقتی نمونه‌ی جدید initialize می‌شود، epoch بالا می‌رود و درخواست‌های زامبی با `ProducerFencedException` شکست می‌خورند. آن نمونه باید نوشتن را متوقف کند، پس خروجی کهنه‌اش هرگز commit نمی‌شود.»

## ⚠️ اشتباهات رایج

- **«exactly-once یعنی پیام یک بار روی شبکه سفر کرده.»** درباره‌ی *اثر* در Kafka است، بعد از حذف تکرارها. retry همچنان رخ می‌دهد.
- **استفاده‌ی مجدد از یک `transactional.id` بین نمونه‌های مختلف.** نمونه‌ها همدیگر را fence می‌کنند و throughput به ری‌استارت‌های دائمی تبدیل می‌شود.
- **فرض این‌که نوشتن در دیتابیس پوشش داده شده.** نشده است، مگر این‌که sink idempotent باشد یا بخشی از یک پروتکل two-phase باشد.
- **فراموش کردن `read_committed` در پایین‌دست.** خواننده‌هایی با `read_uncommitted` پیش‌فرض، رکوردهای abortشده و در حال انجام را می‌بینند و تضمین برای آن‌ها می‌شکند.
- **تنظیم `transaction.timeout.ms` بالاتر از سقف broker.** producer هنگام شروع خطا می‌دهد.
- **روشن کردن exactly-once همه‌جا «برای احتیاط».** تأخیر و ریسک عملیاتی اضافه می‌کند. آن را جایی به کار ببر که خروجی Kafka است و تکرار هزینه دارد.

## ✅ خودت را بسنج

1. producer idempotent چه چیزی را تضمین می‌کند؟
   - A) هیچ پیامی بین ری‌استارت برنامه گم نمی‌شود
   - B) retryهای یک نشست producer در یک partition رکورد تکراری نمی‌سازند
   - C) consumerها هرگز یک پیام را دو بار پردازش نمی‌کنند
   - D) رکوردها به ترتیب سراسری بین partitionها تحویل می‌شوند

<details><summary>پاسخ</summary>

**B** — broker با استفاده از producer ID و شماره‌های توالی per-partition، retryهای تکراری را کنار می‌گذارد. درباره‌ی ری‌استارت (پروسه‌ی جدید producer ID جدید می‌گیرد)، consumerها، یا ترتیب بین partitionها چیزی نمی‌گوید.

</details>

2. کدام تنظیم consumer باعث می‌شود رکوردهای transactionهای abortشده نادیده گرفته شوند؟
   - A) `enable.auto.commit=false`
   - B) `isolation.level=read_committed`
   - C) `auto.offset.reset=latest`
   - D) تنظیم `group.instance.id`

<details><summary>پاسخ</summary>

**B** — `read_committed` رکوردهای abortشده را فیلتر می‌کند و در last stable offset می‌ایستد. بقیه‌ی گزینه‌ها commit، نقطه‌ی شروع و عضویت را کنترل می‌کنند، نه دیده شدن داده‌ی transactional را.

</details>

3. دو نمونه با یک `transactional.id` شروع می‌شوند. چه اتفاقی می‌افتد؟
   - A) هر دو عادی می‌نویسند و broker خروجی‌ها را ادغام می‌کند
   - B) نمونه‌ی جدیدتر epoch را بالا می‌برد و نمونه‌ی قدیمی fence می‌شود
   - C) broker نمونه‌ی دوم را در شروع برای همیشه رد می‌کند
   - D) هر دو بی‌صدا یک producer ID مشترک دارند

<details><summary>پاسخ</summary>

**B** — initialize کردن برای آن ID epoch را بالا می‌برد. درخواست‌های نمونه‌ی قدیمی‌تر با `ProducerFencedException` شکست می‌خورند. اگر دو نمونه مدام شروع شوند و هم‌پوشانی داشته باشند، همدیگر را مدام fence می‌کنند؛ به همین دلیل ID باید برای هر نمونه‌ی منطقی ثابت و غیرمشترک باشد.

</details>

4. *(سنیور)* یک pipeline از Kafka می‌خواند، هر رکورد را غنی می‌کند و در PostgreSQL upsert می‌کند. Kafka روی exactly-once تنظیم شده. آیا نتیجه‌ی end-to-end هم exactly-once است؟ در دو جمله توضیح بده.

<details><summary>پاسخ</summary>

نه. خروجی و آفست‌های Kafka اتمی‌اند، اما نوشتن در PostgreSQL بیرون از transaction است، پس crash بین نوشتن در DB و commit کافکا باعث replay رکورد و نوشتن دوباره‌ی آن می‌شود. فقط وقتی به‌طور مؤثر exactly-once می‌شود که upsert با یک کلید business پایدار انجام شود یا نوشتن بخشی از یک outbox یا پروتکل two-phase باشد.

</details>

5. هزینه‌ی اصلی تأخیر برای consumerهای `read_committed` چیست؟

<details><summary>پاسخ</summary>

رکوردها فقط وقتی قابل دیدن می‌شوند که transactionشان commit شود، پس دیده شدنشان به بازه‌ی commit (در Flink بازه‌ی checkpoint) تأخیر دارد. یک transaction که مدت زیادی باز مانده هم LSO را برای این consumerها نگه می‌دارد، پس حتی وقتی topic داده‌ی زیادی دارد، lag نشان داده می‌شود.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| محدوده‌ی exactly-once در Kafka | read → process → write **داخل Kafka**؛ sinkهای خارجی به idempotency خودشان نیاز دارند |
| producer idempotent | producer ID + شماره‌ی توالی per-partition؛ تکرار retry را حذف می‌کند |
| `transactional.id` | برای هر نمونه‌ی منطقی ثابت؛ producer را به یک epoch وصل می‌کند |
| `sendOffsetsToTransaction` | آفست‌های ورودی را داخل همان transaction خروجی قرار می‌دهد |
| fencing زامبی | epoch بالاتر برنده است؛ نمونه‌ی قدیمی `ProducerFencedException` می‌گیرد |
| `read_committed` | consumerها داده‌ی abortشده را رد می‌کنند و در last stable offset می‌ایستند |
| تنظیم Kafka Streams | `processing.guarantee=exactly_once_v2` (نسخه‌ی خودت را چک کن) |
| راهبرد sink | upsert idempotent، transactional outbox، یا two-phase commit |

## 🔗 ادامه

- **kafka-consumers** سمت consumer را توضیح می‌دهد: rebalance، commit آفست و lag، که این درس به آن‌ها وابسته است.
- **delivery-semantics** سه تحویل at-most-once، at-least-once و exactly-once را بین سیستم‌ها مقایسه می‌کند و پرسش sink را عمیق‌تر بررسی می‌کند.
- **pattern-exactly-once** همین ایده‌ها را به‌صورت یک الگوی system design با یک pipeline کامل نشان می‌دهد.
- **distributed-transactions** توضیح می‌دهد two-phase commit چیست و چرا گران است.
- مدخل‌های glossary به نام‌های [[exactly-once]] و [[idempotency]] تعریف‌های کوتاهی دارند که در مصاحبه می‌توانی دوباره استفاده کنی.
