---
order: 94
tid: "streaming-state"
track: "8"
section: null
num: "8.10"
title: "State Management in Streaming"
type: "concept"
level: "senior"
priority: "P0"
est_hours: 4
summary: "پردازش stateful: state store، RocksDB، TTL برای state، checkpoint و savepoint، مقیاس‌دهی state و مهاجرت schema آن."
covers: ["keyed vs operator state","incremental checkpoints","state size growth problems"]
interview_qs: []
extras: []
prerequisites: ["stream-concepts", "time-and-ordering"]
related: ["flink", "kafka-exactly-once", "stream-joins", "spark-structured-streaming", "storage-engines"]
---
## 🎯 خلاصه

> **State** چیزی است که یک job استریمینگ بین رویدادها به خاطر می‌سپارد: شمارنده‌های در حال اجرا، پنجره‌های باز، بافرهای join و کلیدهای deduplication. بدون state فقط می‌توانی هر رویداد را جدا از بقیه پردازش کنی.

- state به‌ازای هر **کلید** (مثلاً هر کاربر یا هر کارت) نگهداری می‌شود، پس می‌تواند بین workerهای موازی تقسیم شود.
- state در یک **state backend** ذخیره می‌شود: یا روی heap جاوا (سریع، محدود به حافظه) یا در RocksDB روی دیسک محلی (بزرگ، ولی هر دسترسی کندتر).
- **checkpoint**ها به‌طور دوره‌ای کل state را snapshot می‌گیرند، تا job خراب‌شده از یک نقطه‌ی سازگار ادامه دهد و از صفر شروع نکند.
- دو مشکل در production غالب‌اند: state که **بی‌حد رشد** می‌کند، و checkpointهایی که از فاصله‌ی checkpoint بیشتر طول می‌کشند. راه‌حل‌ها TTL، نگهداری محدود (retention) و مانیتورینگ اندازه‌ی state هستند.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** صندوق‌دار یک فروشگاه دفترچه‌ای دارد با یک صفحه برای هر مشتری: «علی امروز ۳ قلم خریده». اگر صندوق‌دار دفترچه را گم کند، هر مشتری دوباره «تازه‌وارد» به نظر می‌رسد. در استریمینگ، این دفترچه **state** است و قطع برق، **crash**.

فرض کن یک جریان کلیک داری. می‌خواهی تعداد کلیک‌های هر کاربر تا آن لحظه را داشته باشی:

```text
event: user=ali   click
event: user=sara  click
event: user=ali   click   -> شمارش ali الان 2 است (این به یاد داشتن کلیک قبلی نیاز دارد)
```

برای تولید عدد `2`، job باید از رویداد اول، `ali -> 1` را به خاطر بسپارد. آن عدد نگه‌داشته‌شده همان state است. سه نکته را نگه دار:

1. state به یک **کلید** تعلق دارد (اینجا `user`). شمارش علی روی شمارش سارا اثری ندارد.
2. state باید از **ری‌استارت** جان سالم به در ببرد. اگر job کرش کند و `ali -> 2` را فراموش کند، کلیک بعدی دوباره `1` می‌شود که غلط است.
3. اگر هیچ‌چیز را دور نریزی، state می‌تواند **برای همیشه** رشد کند. شمارش به‌ازای هر کاربر کوچک است؛ لیست همه‌ی رویدادهای هر کاربر این‌طور نیست.

بقیه‌ی این درس درباره‌ی این سه نکته است: state چطور تقسیم می‌شود، چطور ذخیره می‌شود و چطور محدود می‌شود.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### keyed state و operator state

- **keyed state** به‌ازای هر کلید وجود دارد و فقط وقتی دیده می‌شود که رویداد همان کلید در حال پردازش است. Flink آن را به‌صورت `ValueState`، `ListState`، `MapState`، `ReducingState` و `AggregatingState` در اختیار می‌گذارد. Kafka Streams هم با key-value store به همین شکل عمل می‌کند.
- **operator state** به یک نمونه‌ی موازی تعلق دارد، نه به کلید. sourceها از آن برای چیزهایی مثل offset پارتیشن‌های Kafka استفاده می‌کنند. وقتی parallelism عوض شود، به‌صورت لیست بین نمونه‌ها توزیع می‌شود.

keyed state همان چیزی است که در منطق کسب‌وکار استفاده می‌کنی. کلیدها به **key group** هش می‌شوند. هر task موازی بازه‌ی پیوسته‌ای از key groupها را در اختیار دارد و همین باعث می‌شود بتوان مقیاس را تغییر داد.

### state backendها

| Backend | محل نگهداری state | نقطه‌ی قوت | محدودیت |
|---|---|---|---|
| Heap (HashMap) | شیء‌های JVM در حافظه | خواندن و نوشتن سریع | محدود به heap؛ heap بزرگ، GC طولانی ایجاد می‌کند |
| RocksDB (embedded) | سریالایز‌شده در یک LSM tree روی دیسک | state می‌تواند خیلی بزرگ‌تر از RAM باشد | سریال‌سازی و I/O دیسک در هر دسترسی؛ به‌ازای هر رکورد کندتر |

با RocksDB، خواندن و نوشتن از LSM tree شرح‌داده‌شده در [[lsm-tree]] عبور می‌کند و state یک task روی دیسک محلی همان task قرار دارد.

### checkpoint و savepoint

**checkpoint** یک snapshot خودکار و دوره‌ای است. coordinator یک barrier را به sourceها تزریق می‌کند؛ هر operator وقتی barrier به آن می‌رسد state خود را snapshot می‌گیرد و barrier را جلو می‌فرستد. وقتی همه‌ی operatorها تأیید کنند، checkpoint کامل است. این همان خانواده‌ی الگوریتم Chandy-Lamport است که در [[flink]] و [[kafka-exactly-once]] بررسی شده است.

```java
env.enableCheckpointing(60_000);                              // every 60 s
env.getCheckpointConfig().setCheckpointTimeout(10 * 60_000);  // give up after 10 min
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
```

- **checkpoint کامل** هر بار کل state را کپی می‌کند.
- **checkpoint افزایشی** (فقط با RocksDB) فقط فایل‌های SST را که از checkpoint قبلی تغییر کرده‌اند آپلود می‌کند. هر checkpoint به فایل‌هایی که هنوز لازم دارد ارجاع می‌دهد، پس هزینه با میزان تغییر متناسب است، نه با کل حجم.

**savepoint** یک checkpoint است که عمداً و در قالبی قابل‌حمل گرفته می‌شود. قبل از ارتقا، تغییر schema یا rescale یکی می‌گیری و job جدید را از آن شروع می‌کنی.

### TTL برای state

اگر TTL نداشته باشی، state یک کلید تا پایان job می‌ماند. TTL یعنی «بعد از N زمان بدون نوشتن، این مقدار را فراموش کن».

```java
StateTtlConfig ttl = StateTtlConfig
    .newBuilder(Duration.ofHours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();

ValueStateDescriptor<Long> clicks = new ValueStateDescriptor<>("clicks", Types.LONG);
clicks.enableTimeToLive(ttl);
```

- `OnCreateAndWrite` تایمر را فقط با ساخت و نوشتن تازه می‌کند؛ `OnReadAndWrite` با خواندن هم تازه می‌کند.
- `NeverReturnExpired` باعث می‌شود مقدار منقضی‌شده فوراً قابل‌دیدن نباشد. حذف فیزیکی از storage **best effort** است: مدخل‌های منقضی‌شده ممکن است تا خواندن، cleanup پس‌زمینه یا RocksDB compaction روی دیسک بمانند. فرض نکن TTL فوراً فضا آزاد می‌کند.
- TTL در state نوع لیست و map به‌تفکیک هر عنصر اعمال می‌شود، پس هر عنصر جداگانه منقضی می‌شود.

> **⚠️ مراقب باش:** TTL به‌طور پیش‌فرض بر اساس processing time است. حافظه را محدود می‌کند، ولی جایگزین منطق event time مثل پنجره‌ها با allowed lateness نیست. قبل از تکیه بر یک استراتژی cleanup مشخص، مستندات نسخه‌ی خودت را بخوان.

## 🧠 عمق و trade-offها (سنیور)

**رشد state بزرگ‌ترین ریسک production است.** اندازه‌ی state تقریباً برابر است با `تعداد کلیدهای زنده × بایت به‌ازای هر کلید`. فرض کن ۱۰ میلیون کاربر داری که هر کدام مقداری حدود ۱۰۰ بایت به‌علاوه‌ی overhead دارند؛ یعنی حدود ۱۰M × ۱۵۰ B ≈ ۱.۵ GB برای هر operator، قبل از replica و فایل‌های checkpoint. این قابل‌قبول است. همان job با کلید `(user, session_id)` و نگه‌داشتن لیست رویدادها برای هر session، بدون TTL، با هر session که تا حالا دیده شده رشد می‌کند: ۱۰M × ۲۰ × ۲ KB ≈ ۴۰۰ GB، و هرگز کوچک نمی‌شود. این نوع رشد معمولاً **باگ طراحی** است (یک کلید یا بافر join بی‌حد)، نه مسئله‌ی tuning.

منابع رایج state بی‌حد:

- joinهایی که هر دو طرف را بدون محدودیت زمانی بافر می‌کنند (ببین [[stream-joins]]).
- کلیدهایی که هرگز بسته نمی‌شوند، مثل session ID بدون فاصله‌ی بی‌فعالیتی یا فضای user ID که مدام بزرگ‌تر می‌شود.
- مجموعه‌های deduplication بدون پنجره‌ی نگهداری (ببین [[deduplication]]).
- پنجره‌هایی با `allowedLateness` خیلی بزرگ‌تر از نیاز کسب‌وکار.

**مدت checkpoint یک شاخص پیش‌بینی‌کننده است.** وقتی زمان checkpoint به فاصله‌ی checkpoint نزدیک می‌شود، checkpointها روی هم می‌افتند یا شکست می‌خورند، نقاط بازیابی قدیمی می‌شوند و replay در حالت at-least-once بزرگ‌تر می‌شود. دو علت را از هم جدا کن:

- **زمان alignment**: با barrierهای همگام، کانال‌های سریع منتظر کندترین کانال می‌مانند. زیر [[backpressure]] این انتظار بیشتر می‌شود. unaligned checkpoint اجازه می‌دهد barrierها از داده‌های بافرشده جلو بزنند؛ این alignment را کوتاه می‌کند ولی checkpoint را بزرگ‌تر و سربار را کمی بیشتر می‌کند.
- **زمان آپلود**: checkpoint کامل state بزرگ، بیشتر وقتش صرف شبکه و آپلود به object store می‌شود. checkpointهای افزایشی RocksDB کمک می‌کنند، ولی RocksDB compaction ممکن است خیلی از فایل‌های SST را هم‌زمان بازنویسی کند، پس افزایش همیشه کوچک نیست.

**max parallelism و rescale.** savepoint با جابه‌جایی بازه‌های key group به parallelism جدید بازیابی می‌شود. تعداد key groupها (max parallelism) وقتی state برای اولین بار نوشته می‌شود ثابت می‌شود. اگر آن را خیلی کوچک انتخاب کنی، مقیاس‌دهی آینده محدود می‌شود؛ تغییرش بعداً یک مسئله‌ی مهاجرت state است. پس از اول با دقت و با حاشیه‌ی کافی تنظیمش کن.

**تکامل schema.** state با یک `TypeSerializer` سریال می‌شود. Flink بعضی تغییرات سازگار را پشتیبانی می‌کند (مثلاً افزودن فیلد در بعضی ساختارهای Avro یا POJO، و در نسخه‌های جدیدتر فعال‌کردن TTL روی state موجود). تغییر نام، تغییر نوع و serializerهای ناسازگار معمولاً یک job مهاجرت state می‌خواهند که state قدیمی را بخواند و state جدید بنویسد. مسیر restore از savepoint را قبل از release تست کن، نه وسط incident.

**کی از keyed state استفاده نکنیم.** اگر فقط آخرین مقدار هر کلید را برای سرو کردن می‌خواهی، یک key-value store یا جدول ساده‌تر برای پرس‌وجو است. اگر state خیلی بزرگ و کم‌تغییر است، محاسبه‌ی دوره‌ای batch ممکن است ارزان‌تر باشد. state قدرتمند است، ولی هر بایتی که نگه می‌داری باید checkpoint، restore و مانیتور شود.

**چه چیزهایی را مانیتور کنیم:** اندازه‌ی کل state به‌ازای هر operator، مدت checkpoint و زمان alignment، تعداد checkpoint‌های ناموفق، مصرف RocksDB یا heap، latency دسترسی به state و سن آخرین checkpoint موفق.

## 🧪 امتحان کن

روی کاغذ یا در یک notebook حل کن، بعد جواب‌ها را بررسی کن.

1. **تخمین اندازه.** یک job به‌ازای هر `card_id` یک `ValueState<Long>` نگه می‌دارد. ۵ میلیون کارت فعال داری. هر کلید حدود ۴۰ بایت مقدار و overhead دارد و checkpointها ۳ نسخه‌ی کامل نگه می‌دارند. اندازه‌ی زنده‌ی state و فضای دیسک با ۳ checkpoint نگه‌داشته‌شده را تخمین بزن (اشتراک فایل‌های افزایشی را نادیده بگیر).
2. **انتخاب TTL.** یک job تقلب «آخرین merchant دیده‌شده» را برای هر کارت نگه می‌دارد تا تکرار شارژ را تشخیص دهد. قانون کسب‌وکار می‌گوید شارژ تکراری در ۲ ساعت، تکراری است. چه TTL‌ای تنظیم می‌کنی و چرا ۲ ساعت برای «مشتریانی که ظرف یک ماه برمی‌گردند» کافی نیست؟
3. **تشخیص checkpoint.** یک job فاصله‌ی checkpoint آن ۶۰ ثانیه است. checkpointها به‌طور میانگین ۴۵ ثانیه طول می‌کشند، و بعد از دو برابر شدن state، با timeout شکست می‌خورند. دو علت را به ترتیب بررسی‌کردنت فهرست کن.

<details><summary>جواب‌ها را ببین</summary>

**۱.** state زنده: ۵,۰۰۰,۰۰۰ × ۴۰ B = ۲۰۰,۰۰۰,۰۰۰ B ≈ ۲۰۰ MB. با ۳ checkpoint کامل نگه‌داشته‌شده: ۳ × ۲۰۰ MB ≈ ۶۰۰ MB روی checkpoint store، به‌علاوه‌ی نسخه‌ی کاری روی task managerها. با checkpointهای افزایشی مجموع ذخیره‌شده کمتر است، ولی برای برنامه‌ریزی ظرفیت، ۲۰۰ MB مرتبه‌ی درست بزرگی است.

**۲.** TTL دو ساعته (با `OnCreateAndWrite`، پس تایمر با هر شارژ جدید تازه می‌شود) همان اطلاعاتی را نگه می‌دارد که قانون تکرار لازم دارد. «برگشت ظرف یک ماه» به ۳۰ روز state برای هر کارت نیاز دارد، یعنی ۳۶۰ برابر زمان بیشتر و برای کلیدهای فعال تا ۳۶۰ برابر مدخل بیشتر. چنین قانونی به job دسته‌ای یا یک store جدا تعلق دارد، نه به تایمر بلندمدت استریمینگ.

**۳.** اول بررسی کن alignment کند است یا نه (backpressure، skew در key groupها، یک کلید داغ). دوم زمان آپلود را بررسی کن (رشد اندازه‌ی state، churn ناشی از RocksDB compaction، توان object store). بعد بین کاهش state هر کلید، تنظیم parallelism، یا unaligned checkpoint (اگر alignment غالب است) تصمیم بگیر.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «بعد از خطا، Flink چطور سازگاری state را حفظ می‌کند؟»** جواب متوسط: «checkpointهای دوره‌ای از state می‌گیرد و از آخرین checkpoint ری‌استارت می‌کند.» جواب سنیور: «checkpointها snapshotهای barrier-aligned هستند، پس state هر operator با یک موقعیت یکسان در ورودی مطابقت دارد. هنگام ری‌استارت، sourceها به offsetهای ذخیره‌شده در checkpoint برمی‌گردند و state بازیابی می‌شود. همراه با sinkهای تراکنشی، این exactly-once برای نتیجه را می‌دهد، نه فقط برای state.»

**سؤال: «checkpointهای یک job stateful مرتب timeout می‌شوند. چه می‌کنی؟»** جواب متوسط: «timeout و parallelism را زیاد می‌کنم.» جواب سنیور: «اول زمان alignment را از زمان آپلود در متریک‌های checkpoint جدا می‌کنم. alignment به backpressure یا skew اشاره دارد؛ آپلود به اندازه‌ی state یا churn مربوط به compaction. بعد بررسی می‌کنم کدام کلیدها state بی‌حد دارند و آیا TTL یا بازطراحی راه‌حل واقعی است.»

> **🎤 این را در مصاحبه بگو:** «من state را با سه سؤال فکر می‌کنم: کدام کلید مالک آن است، کجا نگه داشته می‌شود و چه زمانی فراموش می‌شود. قبل از production، TTL تنظیم می‌کنم و اندازه‌ی state را مانیتور می‌کنم، چون رشد state همان چیزی است که ماه‌ها بعد باعث شکست checkpoint می‌شود.»

## ⚠️ اشتباهات رایج

> **⚠️ مراقب باش:** فرض کنی TTL فوراً فضای دیسک را آزاد می‌کند. مقادیر منقضی‌شده فوراً پنهان می‌شوند ولی بعداً حذف می‌شوند، پس مصرف دیسک ممکن است مدتی بالا بماند.

> **⚠️ مراقب باش:** کلید یک operator دارای state را بدون مهاجرت عوض کنی. state بر اساس کلید قبلی پارتیشن شده، پس job جدید آن را پیدا نمی‌کند.

- استفاده از join بدون محدودیت زمانی در هر دو طرف، و بعد تعجب از رشد مداوم state.
- تنظیم فاصله‌ی checkpoint کوتاه‌تر از مدت checkpoint، که باعث صف شدن checkpointها می‌شود.
- نادیده گرفتن max parallelism در اولین deploy، و بعد ناتوانی در مقیاس‌دهی تمیز.
- نگه‌داشتن payload کامل رویدادها در state وقتی فقط یک خلاصه‌ی کوچک لازم است.
- در نظر گرفتن savepoint به‌عنوان بک‌آپی که هرگز restore از آن تست نشده.

## ✅ خودت را بسنج (quiz)

1. keyed state کجا پارتیشن می‌شود؟
   - A) روی job manager
   - B) بر اساس کلید، به‌طوری که هر task موازی بازه‌ای از key groupها را دارد
   - C) یک‌بار برای هر operator و مشترک بین همه‌ی taskها
   - D) فقط در topic کافکا

<details><summary>جواب</summary>

**B** — keyed state بر اساس کلید به key groupها پارتیشن می‌شود و هر task موازی بازه‌ی پیوسته‌ای از آن‌ها را دارد. به همین دلیل وقتی parallelism تغییر می‌کند state قابل توزیع مجدد است.

</details>

2. ۲ TB state داری و روی هر task manager یک SSD سریع. کدام backend مناسب‌تر است؟
   - A) heap backend، چون همیشه سریع‌ترین است
   - B) RocksDB backend، چون state می‌تواند بیشتر از حافظه باشد و checkpoint افزایشی را پشتیبانی می‌کند
   - C) operator state، چون همه‌چیز را در یک لیست نگه می‌دارد
   - D) هیچ backendی؛ state باید در Kafka بماند

<details><summary>جواب</summary>

**B** — RocksDB state را روی دیسک محلی نگه می‌دارد، پس فراتر از RAM مقیاس می‌گیرد و checkpoint افزایشی دارد. heap backend به‌ازای هر دسترسی سریع‌تر است، ولی به اندازه‌ی heap و توقف‌های GC محدود است.

</details>

3. TTL روی ۲۴ ساعت با `OnCreateAndWrite` تنظیم شده. یک کلید هر ساعت خوانده می‌شود ولی هرگز نوشته نمی‌شود. state آن کی منقضی می‌شود و چرا؟

   - A) هرگز، چون خواندن مجاز است
   - B) حدود ۲۴ ساعت پس از آخرین نوشتن، چون در `OnCreateAndWrite` خواندن تایمر را تازه نمی‌کند
   - C) بعد از ۱ ساعت، چون آخرین خواندن حساب می‌شود
   - D) فوراً، چون TTL برای خواندن نادیده گرفته می‌شود

<details><summary>جواب</summary>

**B** — در `OnCreateAndWrite` فقط ساخت و نوشتن TTL را تازه می‌کنند. اگر خواندن هم باید فعالیت حساب شود، از `OnReadAndWrite` استفاده کن.

</details>

4. **(سنیور)** یک job با کلید `(user_id, session_id)` همه‌ی رویدادهای هر session را بدون TTL نگه می‌دارد. طی یک ماه، مدت checkpoint از ۲۰ ثانیه به ۴ دقیقه می‌رسد. ریشه‌ی مسئله چیست و اول چه چیزی را عوض می‌کنی؟

   - A) فاصله‌ی checkpoint خیلی طولانی است
   - B) رشد بی‌حد state از کلیدهایی که هرگز بسته نمی‌شوند؛ timeout برای session یا TTL اضافه کن و نگهداری را بازبینی کن
   - C) RocksDB نمی‌تواند لیست را نگه دارد
   - D) راه‌حل ماشین‌های task manager بیشتر با همان state است

<details><summary>جواب</summary>

**B** — state با هر session که تا حالا دیده شده رشد می‌کند، پس اندازه‌ی checkpoint و زمان آپلودش هم رشد می‌کند. بستن session‌ها بر اساس فاصله‌ی بی‌فعالیتی و محدود کردن نگهداری، علت را برطرف می‌کند؛ اضافه کردن ماشین فقط مشکل را جابه‌جا می‌کند.

</details>

5. **(سنیور)** چرا `max parallelism` برای state مهم است، حتی اگر قصد نداری بیشتر از parallelism فعلی مقیاس بگیری؟

   - A) تعداد پارتیشن‌های Kafka را تعیین می‌کند
   - B) تعداد key groupهای state را ثابت می‌کند و این محدود می‌کند job بعداً تا کجا می‌تواند مقیاس بگیرد
   - C) فاصله‌ی checkpoint را کنترل می‌کند
   - D) فقط روی jobهای batch اثر دارد

<details><summary>جواب</summary>

**B** — تعداد key groupها هنگام نوشتن state تعیین می‌شود. اگر خیلی کم باشد، مقیاس‌دهی آینده محدود می‌شود و تغییرش بعداً یعنی مهاجرت state؛ پس با حاشیه و عمداً تنظیمش کن.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| state در streaming چیست؟ | اطلاعاتی که job بین رویدادها نگه می‌دارد، مثل شمارنده، پنجره یا بافر join |
| keyed در برابر operator state | keyed: به‌ازای هر کلید، در منطق کسب‌وکار. operator: به‌ازای هر نمونه‌ی موازی، مثل offset منبع |
| backend heap در برابر RocksDB | heap سریع است ولی به حافظه محدود؛ RocksDB روی دیسک، بزرگ‌تر از RAM و هر دسترسی کندتر |
| checkpoint در برابر savepoint | checkpoint خودکار برای بازیابی است؛ savepoint عمدی و قابل‌حمل برای ارتقا و rescale |
| checkpoint افزایشی | فقط فایل‌های SST تغییرکرده را آپلود می‌کند (RocksDB)؛ هزینه با میزان تغییر است، نه کل حجم |
| TTL با `OnCreateAndWrite` | تایمر با ساخت و نوشتن تازه می‌شود، نه با خواندن |
| آیا TTL فوراً دیسک را آزاد می‌کند؟ | خیر؛ مدخل‌های منقضی‌شده فوراً پنهان و بعداً حذف می‌شوند (best effort) |
| علت اصلی رشد state | کلیدهای بی‌حد یا joinهای بدون محدودیت زمانی، و نگهداری طولانی‌تر از نیاز کسب‌وکار |

## 🔗 ادامه

- **stream-concepts** و **time-and-ordering** توضیح می‌دهند event time و پنجره‌ها چه state‌ای لازم دارند.
- **stream-joins** نشان می‌دهد کدام نوع join بافر محدود لازم دارد.
- **kafka-exactly-once** توضیح می‌دهد checkpoint و sinkهای تراکنشی چطور به تضمین end-to-end تبدیل می‌شوند.
- **spark-structured-streaming** مدل state store در Spark را پوشش می‌دهد تا با Flink مقایسه کنی.
- قبل از tune کردن چیزی در production، بخش state و checkpointing مستندات Flink را برای نسخه‌ی دقیق خودت بخوان.
