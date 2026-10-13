---
order: 87
tid: "kafka-producers"
track: "8"
section: null
num: "8.3"
title: "Kafka Producers"
type: "technology"
level: "advanced"
priority: "P0"
est_hours: 3
summary: "تنظیمات acks، idempotent producer، batching (linger.ms، batch.size)، فشرده‌سازی، partitioner و نقش کلید در ترتیب، retry و مشکل تغییر ترتیب."
covers: ["max.in.flight.requests","delivery.timeout.ms","hot partitions from skewed keys"]
interview_qs: []
extras: []
prerequisites: ["kafka-architecture"]
related: ["kafka-exactly-once", "delivery-semantics", "dedup-idempotent-loads", "kafka-consumers"]
---
## 🎯 خلاصه

> producer در Kafka هر رکورد را تنها نمی‌فرستد. رکوردها را برای هر partition در **batch** جمع می‌کند، هر batch را به leader همان partition می‌فرستد و منتظر **تأیید (acknowledgement)** می‌ماند. تنظیمات `acks`، idempotence، retry و تعداد درخواست‌های in-flight تعیین می‌کنند چه چیزی ممکن است گم، تکراری یا جابه‌جا شود.

- `acks=all` همراه با **idempotence** یعنی retry امن و بدون duplicate داخل یک session از producer.
- `linger.ms`، `batch.size` و `compression.type` تأخیر کمی را در برابر throughput بسیار بیشتر معامله می‌کنند.
- **partitioner** با استفاده از کلید partition را انتخاب می‌کند. این ترتیب بر اساس کلید را می‌دهد، ولی می‌تواند **partition داغ** بسازد.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** کارمند پست نامه‌ها را برای هر آدرس داخل یک کیسه جمع می‌کند. کیسه وقتی پر شود یا تایمر تمام شود حرکت می‌کند. رسید مرکز مرتب‌سازی می‌گوید «تحویل شد» یا «ناموفق»، و کارمند باید درباره‌ی ناموفق‌ها تصمیم بگیرد.

فراخوانی producer ساده به نظر می‌رسد:

```python
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 10,
    "batch.size": 65536,
    "compression.type": "zstd",
    "delivery.timeout.ms": 120000,
})

def on_delivery(err, msg):
    if err is not None:
        print("failed:", err)
    else:
        print("ok, partition", msg.partition(), "offset", msg.offset())

# Same key -> same partition -> ordered for this user
producer.produce("orders", key="user-42", value=b'{"amount": 10}', callback=on_delivery)
producer.flush()
```

هر تنظیم به یک مفهوم این درس وصل است. `key` پارتیشن را انتخاب می‌کند، `linger.ms` و `batch.size` شکل batch را تعیین می‌کنند، `acks` معنای «تحویل شد» را مشخص می‌کند، و callback جایی است که از خطا خبردار می‌شوی. `enable.idempotence` را صریح بنویس تا نیت در کد دیده شود. `produce` ناهمزمان است، و به همین دلیل قبل از خروج به `flush()` نیاز داری.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

مسیر یک رکورد:

```mermaid
flowchart LR
    A[send call] --> B[Serializer]
    B --> C[Partitioner: pick partition]
    C --> D[Per-partition batch buffer]
    D -->|"batch full or linger.ms reached"| E[Sender thread]
    E --> F[Leader broker]
    F -->|"ack per acks setting"| G[Callback]
```

**Partitioner.** اگر رکورد کلید داشته باشد، partitioner پیش‌فرض کلید را hash می‌کند (در کلاینت Java، murmur2) و باقی‌مانده‌ی تقسیم بر تعداد partition را می‌گیرد. پس تا وقتی تعداد partition ثابت است، یک کلید همیشه به یک partition می‌رود. رکوردهای بدون کلید از یک استراتژی «چسبنده» (sticky) استفاده می‌کنند: batch یک partition را پر می‌کنند و بعد به سراغ بعدی می‌روند، که در طول زمان بار را متعادل می‌کند. تغییر تعداد partition یعنی `hash % N` عوض می‌شود و کلیدهای موجود جابه‌جا می‌شوند.

**Batching.** batch یک partition وقتی ارسال می‌شود که به `batch.size` بایت برسد یا `linger.ms` از زمان بازشدنش گذشته باشد. با `linger.ms=0` batch به محض آزاد شدن sender می‌رود: کمترین تأخیر و کوچک‌ترین batchها. با `linger.ms=10` رکوردهای بیشتری به هر batch می‌پیوندند: درخواست کمتر، فشرده‌سازی بهتر، throughput بیشتر، و حداکثر ۱۰ میلی‌ثانیه انتظار اضافه.

**فشرده‌سازی** برای هر batch انجام می‌شود، و batchهای بزرگ‌تر بهتر فشرده می‌شوند. در حالت معمول broker batch را همان‌طور که producer فشرده کرده ذخیره می‌کند و مصرف‌کننده آن را باز می‌کند.

**`acks`:**

| `acks` | producer وقتی «موفق» می‌گیرد که | ریسک |
|---|---|---|
| `0` | رکورد به شبکه تحویل داده شده | گم شدن بی‌صدا با هر خطا |
| `1` | leader آن را نوشته | گم شدن اگر leader پیش از کپی followerها بمیرد |
| `all` | همه‌ی replicaهای ISR آن را دارند (و بررسی min ISR رد شده) | امن‌ترین؛ تأخیر بیشتر |

**Retry و duplicate.** خطاهای گذرا (تغییر leader، timeout، لرزش شبکه) retry را فعال می‌کنند. producer تا وقتی `delivery.timeout.ms` تمام شود تلاش می‌کند، بعد callback خطا می‌گیرد. بدون idempotence، retry بعد از گم شدن تأیید می‌تواند همان رکورد را دوبار بنویسد: broker نوشته، پاسخ گم شده، و producer دوباره فرستاده است.

**Idempotent producer.** با `enable.idempotence=true`، producer یک شناسه‌ی producer می‌گیرد و برای هر batch یک شماره‌ی توالی در هر partition پیوست می‌کند. broker هر batchی را که شماره‌اش قبلاً ذخیره شده کنار می‌گذارد. این duplicateهای ناشی از retry را داخل یک session از producer حذف می‌کند. اما برای producer ری‌استارت‌شده (session جدید) کاری نمی‌کند، و نوشتن روی چند partition را اتمیک نمی‌کند. آن کار به transaction نیاز دارد که در `kafka-exactly-once` آمده است.

**Ordering و درخواست‌های in-flight.** `max.in.flight.requests.per.connection` تعداد درخواست‌های تأییدنشده‌ای است که می‌توانند همزمان به یک broker باز باشند. فرض کن batch 1 به‌طور گذرا شکست می‌خورد، batch 2 تأیید می‌شود، و سپس batch 1 دوباره فرستاده می‌شود. حالا partition اول batch 2 و بعد batch 1 را دارد. دو راه امن وجود دارد: idempotence را روشن کنی (broker شماره‌های توالی را بررسی می‌کند، پس تا ۵ درخواست in-flight ترتیب را حفظ می‌کنند)، یا in-flight را ۱ بگذاری بدون idempotence که ترتیب را حفظ می‌کند اما throughput را شدیداً پایین می‌آورد.

## 🧠 عمق و trade-offها (سنیور)

- **دو timeout، یک بودجه.** `delivery.timeout.ms` کل زمان از `send` تا موفقیت یا شکست نهایی را محدود می‌کند، شامل انتظار batching و retryها. باید آن‌قدر بزرگ باشد که `linger.ms` به‌علاوه‌ی یک request timeout را پوشش دهد، و کلاینت Java ترکیب‌های غیرممکن را رد می‌کند. آن را روی زمانی بگذار که می‌توانی صبر کنی، و شکست‌ها را به مسیر dead-letter یا هشدار بفرست.
- **تأخیر در برابر throughput.** برای ingest حجیم (لاگ، clickstream): `acks=all`، idempotence روشن، `linger.ms` در بازه‌ی ۵ تا ۲۰ میلی‌ثانیه، batchهای بزرگ و codec مثل zstd یا lz4. برای مسیرهای حساس به تأخیر: batchهای کوچک و `linger.ms=0` و پذیرفتن فشرده‌سازی کمتر. با اندازه‌ی واقعی رکورد خودت اندازه بگیر؛ اعداد درست به آن بستگی دارند.
- **partitionهای داغ از کلیدهای نامتوازن.** بار هر partition برابر ترافیک کلیدهایی است که به آن نگاشت شده‌اند. فرض کن یک تاجر ۴۰٪ ترافیک را تولید کند و ۱۲ partition داریم. partition حامل آن تاجر ۴۰٪ + ۶۰٪ ÷ ۱۲ = ۴۵٪ بار را می‌گیرد، در حالی که سهم عادلانه حدود ۸.۳٪ است. leader آن پر می‌شود و مصرف‌کننده‌اش عقب می‌افتد. راه‌حل‌ها: کلید ریزتر (مثلاً `merchant_id` همراه `order_id`) اگر نیاز به ترتیب اجازه دهد، پخش کلید داغ بین زیرکلیدها (که ترتیب هر کلید داغ را فدا می‌کند)، یا یک topic جدا برای تاجر غالب. قبل از هر کار، نامتوازن بودن را با متریک‌های هر partition تأیید کن؛ ببین [[data-skew]].
- **backpressure در producer.** در کلاینت Java، وقتی بافر batchهای ارسال‌نشده (`buffer.memory`) پر شود، `send` تا `max.block.ms` منتظر می‌ماند و بعد خطا می‌دهد. میزان آزاد بودن بافر را ببین: producer مسدودشده معمولاً یعنی broker یا شبکه کند است. مستندات کلاینتت را بررسی کن.
- **Exactly-once سرتاسری** به transaction در producer (`transactional.id`)، مصرف‌کننده‌هایی که فقط داده‌ی committed می‌خوانند، و یک sink idempotent برای هر سیستم بیرونی نیاز دارد. producer idempotent به‌تنهایی این کار را نمی‌کند.
- **بهره‌برداری با متریک.** نرخ خطای ارسال، نرخ retry، میانگین اندازه‌ی batch، نسبت فشرده‌سازی، تأخیر درخواست و فشار بافر را بپا. میانگین کوچک شدن batch معمولاً یعنی ترافیک تُنک است یا `linger.ms` خیلی کم است.
- **Schema.** از schema (Avro، Protobuf یا JSON Schema، همراه با schema registry) استفاده کن تا producer بی‌صدا مصرف‌کننده‌ها را خراب نکند. قواعد آن بیشتر یک مسئله‌ی [[data-contract]] است تا فقط serialization.
- **کی `acks=0` قابل قبول است؟** متریک‌های best-effort که کمی از دست رفتن قابل قبول است و throughput مهم‌تر از قطعیت است. آن را تصمیم آگاهانه بگیر، نه پیش‌فرض.

## 🧪 امتحان کن

1. **انتخاب تنظیمات.** برای هر سناریو، `acks`، idempotence و `linger.ms` را انتخاب کن: (الف) رویدادهای پرداخت که نه باید گم شوند و نه تکراری، (ب) رویدادهای کلیک با ۲ میلیون در ثانیه که از دست رفتن ۱٪ آن قابل قبول است، (ج) مسیر هشدار تقلب با بودجه‌ی ۵۰ میلی‌ثانیه.
2. **جابه‌جایی ترتیب.** تنظیمات: idempotence خاموش، retry روشن، `max.in.flight.requests.per.connection=5`. batch A (توالی ۱) به‌طور گذرا شکست می‌خورد، batch B تأیید می‌شود، و سپس A دوباره فرستاده می‌شود. رکوردها در partition به چه ترتیبی می‌رسند؟ کدام دو تنظیم جلوی آن را می‌گیرند؟
3. **محاسبه‌ی کلید داغ.** یک تاجر ۴۰٪ رویدادها را تولید می‌کند، روی ۱۲ partition، و بقیه‌ی کلیدها به‌طور یکنواخت hash می‌شوند. partition داغ چه سهمی از بار دارد؟ سهم عادلانه چقدر است؟ سهم داغ چند برابر سهم عادلانه است؟
4. **استدلال batch.** producer ای ۱۰ رکورد در ثانیه می‌فرستد و `linger.ms=10` دارد. هر batch به‌طور میانگین چند رکورد دارد، و آیا linger گلوگاه است؟

<details><summary>جواب‌ها را نشان بده</summary>

**1.** (الف) `acks=all`، idempotence روشن و linger کم، چون درستی اولویت دارد. در پایین‌دست هم یک مصرف‌کننده یا sink idempotent بگذار. (ب) `acks=1` یا `acks=0` با batchهای بزرگ، linger ۲۰ تا ۵۰ میلی‌ثانیه و فشرده‌سازی روشن، چون از دست رفتن قابل تحمل است و هدف throughput است. (ج) `acks=all` با idempotence برای درستی می‌ماند، با `linger.ms=0` یا خیلی کم و batchهای کوچک، تا بودجه‌ی ۵۰ میلی‌ثانیه شبکه و پردازش را پوشش دهد.

**2.** partition سرانجام B و بعد A را دارد. idempotence را روشن کن (که ترتیب را با تا ۵ درخواست in-flight حفظ می‌کند) یا in-flight را بدون idempotence روی ۱ بگذار.

**3.** partition داغ ۴۰٪ + ۶۰٪ ÷ ۱۲ = ۴۰٪ + ۵٪ = ۴۵٪ بار را حمل می‌کند. سهم عادلانه ۱ ÷ ۱۲ ≈ ۸.۳٪ است. سهم داغ حدود ۵.۴ برابر سهم عادلانه است.

**4.** با ۱۰ رکورد در ثانیه، هر ۱۰۰ میلی‌ثانیه یک رکورد می‌رسد. linger ده میلی‌ثانیه‌ای تقریباً همیشه با یک رکورد تمام می‌شود، پس batchها حدود یک رکورد دارند. linger اینجا کاری نمی‌کند و تأخیر اضافه‌اش حداکثر ۱۰ میلی‌ثانیه است. batching فقط در نرخ‌های بالاتر اهمیت دارد.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «چطور هنگام produce از duplicate جلوگیری می‌کنی؟»** جواب متوسط: «idempotent producer را با `acks=all` روشن می‌کنم، تا retryها یک رکورد را دوبار ننویسند.» جواب سنیور: «idempotence duplicateهای retry را داخل یک session و در هر partition حذف می‌کند. اگر producer ری‌استارت شود، یا نوشتن اتمیک روی چند partition لازم باشد، به transaction نیاز دارم. در پایین‌دست هم consumer یا sink را idempotent می‌کنم، چون کرش بین نوشتن و commit باعث replay رکوردها می‌شود.»

**سؤال: «چرا پیام‌ها جابه‌جا می‌شوند؟»** جواب متوسط: «retry همراه چند درخواست in-flight ممکن است یک batch دوباره فرستاده‌شده را بعد از batch بعدی قرار دهد.» جواب سنیور: «فقط داخل یک partition، و فقط وقتی retry و چند درخواست in-flight بدون idempotence با هم ترکیب شوند. idempotence با تا ۵ درخواست in-flight ترتیب را حفظ می‌کند. ترتیب بین partitionها هرگز تضمین نمی‌شود.»

**سؤال: «کلید داغ را چطور مدیریت می‌کنی؟»** جواب متوسط: «از کلید ریزتر استفاده می‌کنم، یا کلید داغ را بین زیرکلیدها پخش می‌کنم.» جواب سنیور: «اول نامتوازن بودن را با متریک هر partition تأیید می‌کنم، بعد تصمیم می‌گیرم چه ترتیبی را می‌توانم کنار بگذارم. salting یا کلید ریزتر ترتیب هر کلید را با تعادل بار عوض می‌کند. وقتی یک تاجر غالب است، topic جدا ساده‌تر است.»

> **🎤 این را در مصاحبه بگو:** «producer برای هر partition batch می‌سازد و منتظر تأیید می‌ماند. برای درستی `acks=all` را با idempotence می‌گذارم، برای throughput `linger.ms` و اندازه‌ی batch را تنظیم می‌کنم، و کلیدها را طوری انتخاب می‌کنم که ترتیب جایی که مهم است حفظ شود. کلید داغ مسئله‌ی طراحی است که در متریک هر partition دیده می‌شود.»

## ⚠️ اشتباهات رایج

> **⚠️ هشدار:** `acks=1` در staging امن به نظر می‌رسد و در production وقتی leader درست بعد از تأیید بمیرد داده را گم می‌کند. برای داده‌ای که نمی‌توانی از دست بدهی، `acks=all` همراه با idempotence انتخاب پیش‌فرض است.

- برگشتن `send()` را به معنای «تحویل شد» گرفتن. این فراخوانی ناهمزمان است؛ فقط callback یا flush نتیجه را می‌گوید.
- نادیده گرفتن خطاهای تحویل (بدون callback و بدون لاگ)، که رکوردها را بی‌صدا گم می‌کند.
- بالا بردن `max.in.flight` بدون idempotence و انتظار ترتیب.
- گذاشتن `delivery.timeout.ms` کمتر از `linger.ms` به‌علاوه‌ی request timeout.
- اضافه کردن partition به یک topic keyed و انتظار اینکه یک کاربر روی همان partition بماند.
- استفاده از کلید تصادفی (UUID جدید برای هر رویداد) وقتی ترتیب هر کاربر لازم است. این کار ترتیب را کاملاً از بین می‌برد.
- فرض اینکه idempotence یعنی exactly-once برای کل pipeline.

## ✅ خودت را بسنج

1. کدام تنظیم `acks` می‌تواند حتی وقتی همه‌ی brokerها سالم‌اند داده را بی‌صدا گم کند؟
   - A) `acks=0`
   - B) `acks=1`
   - C) `acks=all`
   - D) هیچ‌کدام

<details><summary>جواب</summary>

**A** — با `acks=0` producer اصلاً تأییدی نمی‌گیرد، پس یک خطای شبکه می‌تواند رکوردها را بی‌صدا بیندازد. `acks=1` فقط وقتی داده را گم می‌کند که leader پیش از replication بمیرد.

</details>

2. دو رکورد با یک کلید باید به ترتیب بمانند و retry ممکن است رخ دهد. کدام پیکربندی ترتیب را حفظ می‌کند؟
   - A) `linger.ms=0`
   - B) `compression.type=zstd`
   - C) idempotence روشن با تا ۵ درخواست in-flight، یا in-flight برابر ۱ بدون idempotence
   - D) `batch.size` بزرگ‌تر

<details><summary>جواب</summary>

**C** — ترتیب هنگام retry یا به بررسی توالی broker (idempotence) نیاز دارد یا به یک درخواست in-flight. بقیه‌ی گزینه‌ها روی ترتیب اثری ندارند.

</details>

3. یک topic با ۲۴ partition داری و ۸ تای دیگر اضافه می‌کنی. برای کلیدی که رکورد دارد چه می‌شود؟
   - A) هیچ؛ کلید همچنان به همان partition می‌رود
   - B) ممکن است کلید به partition دیگری برود، پس رکوردهای جدیدش ممکن است ترتیبشان با رکوردهای قدیمی فرق کند
   - C) producer ارسال رکوردهای keyed را رد می‌کند
   - D) رکوردهای قدیمی به partitionهای جدید منتقل می‌شوند

<details><summary>جواب</summary>

**B** — نگاشت `hash % N` است، پس تغییر N کلیدها را جابه‌جا می‌کند. رکوردهای موجود سر جای خودشان می‌مانند و رکوردهای جدید همان کلید ممکن است جای دیگری بنشینند.

</details>

4. **(سنیور)** `delivery.timeout.ms` یک producer سی ثانیه است. brokerها کندند و بعضی callbackها timeout گزارش می‌کنند. کدام گزاره دقیق‌تر است؟
   - A) رکورد قطعاً نوشته نشده
   - B) ممکن است نوشته شده باشد یا نه؛ آن را نامشخص در نظر بگیر و قبل از retry در سطح برنامه، نوشتن را idempotent یا قابل بررسی کن
   - C) رکورد دوبار نوشته شده
   - D) timeout فقط تأخیر شبکه را نشان می‌دهد

<details><summary>جواب</summary>

**B** — timeout ثابت نمی‌کند که شکست خورده: ممکن است broker رکورد را نوشته باشد و پاسخ گم شده باشد. نوشتن idempotent یا بررسی قبل از retry این ابهام را برطرف می‌کند.

</details>

5. **(سنیور)** یک تاجر ۴۰٪ ترافیک را روی ۱۲ partition می‌فرستد، و مصرف‌کننده‌ها ترتیب هر سفارش را لازم دارند، نه ترتیب هر تاجر. مستقیم‌ترین راه‌حل کدام است؟
   - A) افزایش `linger.ms`
   - B) کلیدگذاری بر اساس `order_id` به جای `merchant_id`
   - C) خاموش کردن فشرده‌سازی
   - D) `acks=0`

<details><summary>جواب</summary>

**B** — کلیدگذاری بر اساس `order_id` ترافیک تاجر را بین partitionها پخش می‌کند و ترتیب رکوردهای هر سفارش را حفظ می‌کند. گزینه‌های دیگر انتخاب partition را تغییر نمی‌دهند.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| `acks=0` | بدون تأیید؛ ممکن است گم شدن بی‌صدا باشد. |
| `acks=all` | leader بعد از اینکه همه‌ی replicaهای ISR رکورد را دارند جواب می‌دهد. |
| Idempotent producer | شناسه‌ی producer به‌علاوه‌ی شماره‌ی توالی در هر partition؛ broker duplicateهای retry را داخل session حذف می‌کند. |
| `linger.ms` | مدت انتظار batch برای رکوردهای بیشتر قبل از ارسال. |
| `batch.size` | اندازه‌ی بایتی که با رسیدن به آن batch فوراً ارسال می‌شود. |
| Partitioner | partition را انتخاب می‌کند؛ hash کلید باعث می‌شود یک کلید همیشه به یک partition برود. |
| `delivery.timeout.ms` | بودجه‌ی زمانی کل از `send` تا موفقیت یا شکست نهایی، شامل retry. |
| Hot partition | partitionی با ترافیک بسیار بیشتر از سهمش، ناشی از کلید نامتوازن. |

## 🔗 ادامه

- `kafka-architecture`: leaderها، ISR و `min.insync.replicas` که معنای `acks=all` را می‌سازند.
- `kafka-exactly-once`: transaction، مصرف‌کننده‌های read-committed و تضمین‌های سرتاسری.
- `delivery-semantics`: at-most-once، at-least-once و exactly-once به‌عنوان نمای کل pipeline.
- `dedup-idempotent-loads`: ساختن sinkی که duplicate را تحمل کند.
- واژه‌نامه: [[data-skew]]، [[idempotency]]، [[data-contract]].

