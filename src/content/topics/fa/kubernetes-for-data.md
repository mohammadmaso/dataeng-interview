---
order: 137
tid: "kubernetes-for-data"
track: "13"
section: null
num: "13.2"
title: "Kubernetes for Data Workloads"
type: "technology"
level: "advanced"
priority: "P1"
est_hours: 5
summary: "StatefulSet و Persistent Volume برای سیستم‌های stateful، Operatorها (Strimzi برای Kafka، Spark Operator، Flink Operator، ClickHouse Operator)، مدیریت منابع (requests/limits)، و چالش‌های اجرای دیتابیس روی K8s."
covers: ["local PV vs Ceph RBD for Kafka/ClickHouse","pod anti-affinity","YuniKorn/Volcano for batch scheduling"]
interview_qs: []
extras: []
prerequisites: ["docker"]
related: ["spark-on-kubernetes", "yarn-vs-k8s", "kafka-architecture", "clickhouse", "dataops-cicd", "onprem-iran"]
---
## 🎯 خلاصه

> Kubernetes کانتینرها را روی چندین ماشین اجرا می‌کند و آن‌ها را در **حالت مطلوب (desired state)** نگه می‌دارد که تو اعلام کرده‌ای. برای سرویس‌های **بدون state** این کار آسان است. برای سیستم‌های داده‌ی **stateful** (Kafka، ClickHouse، Postgres، ذخیره‌سازی به سبک HDFS) سخت‌ترین بخش‌ها این‌هاست: **storage که همراه pod می‌ماند**، **هویت مرتب**، **جداسازی منابع** و **ارتقای امن**.

- **Pod** کوچک‌ترین واحد است: یک یا چند کانتینر که شبکه و volume مشترک دارند. podها دورریختنی‌اند؛ IPشان عوض می‌شود.
- **StatefulSet** به podها نام ثابت (`kafka-0`، `kafka-1`) و storage ثابت از طریق **PersistentVolumeClaim** می‌دهد.
- **Requestها** به scheduler می‌گویند pod چه می‌خواهد؛ **limitها** سقف مصرف‌اند. هر دو را درست تنظیم کن، وگرنه jobها گرسنه می‌مانند یا kill می‌شوند.
- **Operator** یک controller است که عملیات یک سیستم مشخص را کدنویسی می‌کند (rolling restart، rebalance، scale). Strimzi، Spark Operator، Flink Kubernetes Operator و operatorهای ClickHouse نمونه‌اند.
- اجرای دیتابیس روی Kubernetes ممکن است، ولی لایه‌ی storage و operator تعیین می‌کنند امن است یا نه. فرض نکن که امن است.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** مدیر یک هتل. تو به مدیر می‌گویی «همیشه سه اتاق با منظره می‌خواهم». اگر اتاقی خراب شود، مدیر تو را جابه‌جا می‌کند. اما برای مهمانی که هر شب همان اتاق را می‌خواهد (یک سرویس stateful)، مدیر باید شماره‌ی اتاق و وسایلت (داده) را با تو نگه دارد، نه اینکه هر اتاق خالی را بدهد.

اشیای Kubernetes که در پلتفرم داده می‌بینی:

| شیء | چیست | مثال داده |
|---|---|---|
| Pod | یک یا چند کانتینر که با هم اجرا می‌شوند | یک executor اسپارک یا یک سرور ClickHouse |
| Deployment | N pod یکسان و بدون state را در حال اجرا نگه می‌دارد | یک REST API که داشبورد را سرویس می‌دهد |
| StatefulSet | podهایی با نام ثابت و storage ثابت | brokerهای Kafka، controllerهای KRaft |
| Service | یک نام شبکه‌ی ثابت برای مجموعه‌ای از podها | `clickhouse.analytics.svc` |
| PersistentVolumeClaim (PVC) | درخواست storage که از عمر pod بیشتر می‌ماند | ۵۰۰ GB برای یک broker Kafka |
| ConfigMap / Secret | تنظیمات و مقادیر حساس | اطلاعات اتصال، رمز |

یک Deployment ساده برای سرویس بدون state:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: schema-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: schema-api
  template:
    metadata:
      labels:
        app: schema-api
    spec:
      containers:
        - name: api
          image: registry.internal/schema-api:1.4.2
          ports:
            - containerPort: 8080
          resources:
            requests:
              cpu: "250m"
              memory: "256Mi"
            limits:
              memory: "512Mi"
```

اگر یک pod بمیرد، Deployment یکی جدید با نام و IP جدید می‌سازد. برای یک API این مشکلی نیست. برای دیتابیسی که فایل‌هایش را روی دیسک محلی نگه می‌دارد مشکل است.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### حالت مطلوب و controllerها

تو حالت مطلوب را می‌نویسی (`replicas: 3`). یک controller آن را با وضعیت واقعی مقایسه می‌کند و عمل می‌کند: pod می‌سازد، حذف می‌کند یا جایگزین می‌کند تا با هم یکی شوند. هیچ چیزی «یک‌بار» اجرا نمی‌شود، مگر اینکه Job بخواهی. به همین دلیل pod کرش‌کرده برمی‌گردد، و به همین دلیل خطا در spec ممکن است بی‌صدا برای همیشه تلاش کند.

### StatefulSet: هویت و storage

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: clickhouse
spec:
  serviceName: clickhouse-headless
  replicas: 3
  selector:
    matchLabels:
      app: clickhouse
  template:
    metadata:
      labels:
        app: clickhouse
    spec:
      containers:
        - name: clickhouse
          image: clickhouse/clickhouse-server:24.8   # نسخه‌ای را پین کن که تست کرده‌ای
          resources:
            requests:
              cpu: "4"
              memory: "16Gi"
            limits:
              memory: "16Gi"
          volumeMounts:
            - name: data
              mountPath: /var/lib/clickhouse
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: local-fast
        resources:
          requests:
            storage: 2Ti
```

این چیزها را به تو می‌دهد:

- podها `clickhouse-0`، `clickhouse-1`، `clickhouse-2` نام می‌گیرند و به ترتیب ساخته می‌شوند.
- هر pod PVC **اختصاصی** خودش را دارد، با نام `data-clickhouse-0`، و وقتی pod دوباره زمان‌بندی می‌شود، این claim با همان شماره‌ی ترتیبی می‌ماند.
- یک Service بدون IP (`clickhouse-headless`) به هر pod یک نام DNS ثابت می‌دهد، مثل `clickhouse-0.clickhouse-headless`، که تنظیمات replication می‌تواند از آن استفاده کند.

حذف یک StatefulSet به صورت پیش‌فرض PVCهایش را **حذف نمی‌کند**. این از داده محافظت می‌کند، ولی یعنی دیسک‌های قدیمی تا وقتی خودت حذف‌شان نکنی هزینه می‌سازند.

### Storage: چرا نوع volume همه چیز را تعیین می‌کند

PersistentVolume (PV) خود storage است. `storageClassName` مشخص می‌کند چطور تأمین شود.

| نوع storage | مزایا | معایب برای سیستم داده |
|---|---|---|
| دیسک محلی (local PV) | کمترین latency، throughput بالا، ارزان | pod به node گره می‌خورد؛ از دست رفتن node یعنی داده باید در سطح اپلیکیشن replicate شده باشد |
| storage بلاکی شبکه‌ای (مثلاً Ceph RBD) | pod می‌تواند به node دیگر برود و دوباره وصل شود | latency بیشتر، domain خرابی مشترک، نیاز به tuning کارایی |
| object storage از طریق CSI یا gateway سازگار با S3 | ظرفیت زیاد، ارزان | فایل‌سیستم POSIX نیست؛ بیشتر دیتابیس‌ها نباید آن را data directory خود کنند |

برای یک broker Kafka یا یک replica ClickHouse، این trade-off واقعی است. local PV سریع‌ترین دیسک را می‌دهد ولی pod را به node وصل می‌کند، پس **replication در سطح اپلیکیشن** (replication factor برابر ۳ در Kafka، replicaهای ClickHouse از طریق Keeper) باید از خرابی node جان سالم به در ببرد. storage شبکه‌ای جابه‌جایی را آسان می‌کند ولی latency و domain خرابی مشترک اضافه می‌کند. بر اساس هر سیستم تصمیم بگیر و خرابی‌ای را که واقعاً انتظارش را داری تست کن.

### زمان‌بندی: requestها، limitها و جای‌گذاری

- **Requestها** چیزی هستند که scheduler رزرو می‌کند. pod فقط روی node‌ای قرار می‌گیرد که CPU و حافظه‌ی درخواستی آزاد کافی داشته باشد.
- **Limitها** سقف‌اند. CPU بالاتر از limit کنترل (throttle) می‌شود؛ حافظه‌ی بالاتر از limit باعث **OOMKilled** شدن container می‌شود.
- برای دیتابیس‌ها و سرویس‌های JVM، **request حافظه را برابر limit** بگذار تا kernel زیر فشار node آن‌ها را نکشد. برای حافظه‌ی off-heap جا بگذار.
- از **pod anti-affinity** استفاده کن تا replicaهای یک سیستم روی یک node نیفتند:

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app: clickhouse
        topologyKey: kubernetes.io/hostname
```

برای پخش در سطح zone، به جای آن `topology.kubernetes.io/zone` را به عنوان `topologyKey` استفاده کن.

### Operatorها

یک StatefulSet ساده نمی‌داند چطور یک broker Kafka اضافه کند، partitionها را rebalance کند یا یک replica ClickHouse را به ترتیب درست restart کند. یک **Operator** یک custom resource (مثلاً یک شیء `Kafka` یا `ClickHouseInstallation`) را می‌پاید و روش خود سیستم را اجرا می‌کند:

- **Strimzi** کلاسترهای Kafka، topicها و کاربران را مدیریت می‌کند.
- **Spark Operator** (پیاده‌سازی Kubeflow) برنامه‌های Spark را از یک منبع `SparkApplication` ارسال و پیگیری می‌کند.
- **Flink Kubernetes Operator** استقرار و ارتقای Flink را با savepoint مدیریت می‌کند.
- **operatorهای ClickHouse** (نسخه‌های جامعه و نسخه‌های نگهداری‌شده توسط فروشنده) توپولوژی و تنظیمات کلاستر را مدیریت می‌کنند.

برای نسخه‌ی API و قابلیت‌های پشتیبانی‌شده‌ی release خودت، مستندات هر operator را بررسی کن؛ این‌ها زیاد تغییر می‌کنند.

### زمان‌بندی دسته‌ای (Batch)

scheduler پیش‌فرض podها را یکی‌یکی جای می‌دهد. یک job اسپارک به ۵۰ executor هم‌زمان نیاز دارد یا هیچ‌کدام؛ در غیر این صورت ممکن است نیمی از کلاستر را بگیرد و برای همیشه منتظر بماند (deadlock). سیستم‌های gang scheduling مثل **Volcano** و **YuniKorn** یک گروه کامل را با هم جای می‌دهند، و صف‌ها سهم هر تیم را مشخص می‌کنند. این پاسخ Kubernetes به کاری است که صف‌های YARN انجام می‌دادند؛ به [[yarn-vs-k8s]] نگاه کن.

## 🧠 عمق و trade-offها (سنیور)

- **کار stateful تصمیم اپلیکیشن است.** Kubernetes هویت ثابت و volume می‌دهد. replication، انتخاب leader، rebalance و backup هنوز کار خود سیستم است. یک StatefulSet با replication factor برابر ۱ یک نقطه‌ی خرابی است با چند مرحله‌ی اضافه.
- **local در برابر storage شبکه‌ای برای Kafka.** Kafka در سطح اپلیکیشن replicate می‌کند، پس دیسک‌های محلی با replication factor ۳ بین zoneها معمولاً بهترین throughput به ازای هزینه را می‌دهند، و از دست رفتن یک broker یعنی rebalance، نه قطعی. storage شبکه‌ای replication را دوباره انجام می‌دهد و به هر fsync مدت اضافه می‌کند. انتخاب به بودجه‌ی دیسک و مدل خرابی بستگی دارد.
- **ClickHouse و merge.** merge پس‌زمینه I/O سنگین دارد. برای podها node pool اختصاصی بگذار و requestهای CPU را آن‌قدر بالا نگه دار که merge کوئری‌ها را گرسنه نکند. تعداد part در هر partition را مانیتور کن.
- **ارتقا ریسک واقعی است.** StatefulSetها به طور پیش‌فرض podها را یکی‌یکی و به ترتیب شماره‌ی ترتیبی به‌روز می‌کنند، ولی «یکی‌یکی» برای یک سیستم quorum کافی نیست. operatorها این را می‌فهمند؛ YAML خام نمی‌فهمد. rolling restart را با بار واقعی تست کن و lag و latency کوئری را در حین آن اندازه بگیر.
- **جداسازی منابع بین jobها.** بدون limit، یک job اسپارک سرکش یک broker Kafka را بیرون می‌کند. از namespace با ResourceQuota، LimitRange، PriorityClass و node poolهای جدا برای کارهای stateful و batch استفاده کن.
- **مشاهده‌پذیری.** restart های pod، رویدادهای OOMKilled، مصرف PVC، latency دیسک، CPU throttle شده و خطاهای reconcile operator را رصد کن. podی که بی‌صدا هر ساعت restart می‌شود یک ریسک از دست رفتن داده است.
- **backup.** snapshot از PVC تا وقتی restore را تست نکرده‌ای backup نیست. از ابزار backup خود سیستم استفاده کن (مثلاً snapshot سازگار هر replica، یا export مدیریت‌شده) و restore را طبق زمان‌بندی تأیید کن.
- **چه زمانی روی Kubernetes اجرا نکنی.** یک دیتابیس کوچک، پایدار و تک‌نودی با یک VM اختصاصی و یک برنامه‌ی backup خوب ممکن است ساده‌تر باشد. وقتی به زمان‌بندی، self-healing و ابزار استاندارد نیاز داری و کسی را داری که لایه‌ی storage را می‌فهمد، روی Kubernetes اجرا کن.

> **⚠️ مراقب باش:** حذف یک StatefulSet، یا PVC پشت آن، می‌تواند تنها نسخه‌ی یک replica را پاک کند. قبل از حذف، `persistentVolumeReclaimPolicy` کلاس storage را چک کن و هرگز بدون backup تأییدشده `kubectl delete pvc` را روی دیتابیس اجرا نکن.

## 🧪 امتحان کن

به یک کلاستر محلی (مثلاً kind یا minikube) و `kubectl` نیاز داری.

1. Deployment بخش مبتدی را اعمال کن. آن را به ۵ scale کن، یک pod را حذف کن و توضیح بده چه چیزی در نام و IP podها تغییر کرد. بعد توضیح بده چرا همین رفتار برای یک دیتابیس با دیسک محلی مشکل است.
2. یک StatefulSet با ۳ replica و یک volume claim به اندازه‌ی ۱ GiB بنویس. `pod-1` را حذف کن و بگو کدام PVC دوباره وصل می‌شود. بعد توضیح بده اگر StatefulSet را با `--cascade=orphan` حذف کنی و با همان نام دوباره بسازی، برای داده چه می‌شود.
3. anti-affinity را به StatefulSet اضافه کن و توضیح بده وقتی کلاستر یک node دارد در برابر سه node چه چیزی را جلوگیری می‌کند.

<details><summary>جواب‌ها را نشان بده</summary>

**۱.** بعد از scale به ۵، پنج pod با پسوند تصادفی داریم. حذف یک pod، یک جایگزین با نام و IP جدید می‌سازد. هیچ‌چیز در اپلیکیشن نباید به IP وابسته باشد. برای دیتابیسی با دیسک محلی، pod جایگزین داده ندارد، پس باید دوباره بپیوندد و همگام شود، وگرنه داده‌ی node قدیمی بی‌صاحب می‌ماند.

**۲.** `pod-1` دوباره PVC خودش یعنی `data-<name>-1` را می‌گیرد، چون نام claim از StatefulSet و شماره‌ی ترتیبی مشتق می‌شود. با حذف orphan، podها پاک می‌شوند ولی PVCها می‌مانند، پس ساختن دوباره‌ی StatefulSet با همان نام claimهای موجود را دوباره وصل می‌کند و داده برمی‌گردد. این مکانیزم پشت «ساختن امن دوباره» است، ولی فقط وقتی نام و اندازه‌ها یکی باشند.

**۳.** روی کلاستر یک‌نودی، anti-affinity اجباری باعث می‌شود replicaهای اضافی قابل زمان‌بندی نباشند (در وضعیت `Pending` می‌مانند)، که درست است چون در غیر این صورت یک domain خرابی مشترک می‌داشتند. روی سه node، scheduler هر replica را روی یک node می‌گذارد. در production، از دست رفتن یک node فقط یک replica را می‌برد، نه همه را.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «آیا Kafka را روی Kubernetes اجرا می‌کنی؟»**

- **جواب متوسط:** «بله، با Strimzi، با StatefulSet و volume پایدار، و با pod anti-affinity تا brokerها بین nodeها پخش شوند.»
- **جواب سنیور:** «Strimzi را برای چرخه‌ی عمر انتخاب می‌کنم، ولی تصمیم storage اول می‌آید. local PV throughput می‌دهد و نیاز به replication factor ۳ بین domainهای خرابی دارد، پس از دست رفتن یک node یعنی rebalance. storage شبکه‌ای جابه‌جایی را آسان می‌کند ولی latency و domain خرابی مشترک اضافه می‌کند. همچنین requestها را برابر limitها می‌گذارم، node poolهای اختصاصی استفاده می‌کنم، و قبل از production یک rolling restart را زیر بار تست می‌کنم.»

**سؤال: «تفاوت Deployment و StatefulSet چیست؟»** Deployment podهای قابل جایگزین با نام تصادفی و بدون storage پایدار می‌دهد. StatefulSet نام‌های مرتب و ثابت و volumeهای پایدار اختصاصی هر pod می‌دهد که همراه هویت pod می‌مانند.

> **🎤 این را در مصاحبه بگو:** «Kubernetes حالت مطلوب، هویت و ابزارهای storage می‌دهد. امن بودن یک سیستم داده روی آن به replication، operator و کلاس storage بستگی دارد. local یا شبکه‌ای بودن storage را بر اساس مدل خرابی انتخاب می‌کنم و مسیر restore را همیشه تست می‌کنم.»

## ⚠️ اشتباهات رایج

> **⚠️ مراقب باش:** تنظیم فقط limit حافظه و بدون request. در این صورت scheduler podها را روی nodeهایی می‌گذارد که توان نگه داشتنشان را ندارند و زیر بار OOMKilled می‌شوند.

- اجرای یک replica از یک سیستم stateful و نامیدن آن «high availability».
- فراموش کردن اینکه PVC از pod و StatefulSet خودش بیشتر عمر می‌کند، پس storage قدیمی هزینه می‌سازد.
- استفاده از Deployment برای یک دیتابیس، و از دست دادن داده وقتی pod جابه‌جا می‌شود.
- نگذاشتن pod anti-affinity، پس همه‌ی replicaها روی یک node می‌افتند.
- تعیین limit پایین CPU برای merge یا compaction، که باعث throttle و lag می‌شود.
- اجازه دادن به اشتراک node بین jobهای batch و brokerها بدون quota یا priority class.
- ارتقای یک operator یا image دیتابیس بدون تست روش rolling.

## ✅ خودت را بسنج

1. کدام شیء Kubernetes به هر pod یک نام ثابت و volume پایدار اختصاصی می‌دهد؟
   - A) Deployment
   - B) Service
   - C) StatefulSet
   - D) ConfigMap

<details><summary>جواب</summary>

**C** — StatefulSetها نام مرتب و ثابت و PVCهای اختصاصی را همراه هویت pod نگه می‌دارند. Deployment podهای قابل جایگزین با نام تصادفی می‌دهد.

</details>

2. یک pod executor اسپارک مدام OOMKilled می‌شود. limit حافظه تنظیم شده ولی request آن خیلی پایین‌تر است. محتمل‌ترین مشکل چیست؟
   - A) request باید صفر باشد
   - B) container از limit حافظه‌اش بیشتر مصرف می‌کند؛ heap و سربار off-heap JVM را با limit بسنج
   - C) requestهای CPU باعث OOM می‌شوند
   - D) Kubernetes از limit حافظه برای Spark پشتیبانی نمی‌کند

<details><summary>جواب</summary>

**B** — OOMKilled یعنی container بیشتر از limit حافظه مصرف کرده. heap JVM و سربار off-heap را با limit مقایسه کن و هر دو را با هم اندازه بگیر.

</details>

3. چرا pod anti-affinity برای یک دیتابیس با سه replica مهم است؟
   - A) باعث می‌شود podها سریع‌تر بالا بیایند
   - B) replicaها را روی nodeهای متفاوت نگه می‌دارد، پس خرابی یک node همه‌ی نسخه‌ها را نمی‌برد
   - C) نیاز به replication را حذف می‌کند
   - D) اندازه‌ی storage را زیاد می‌کند

<details><summary>جواب</summary>

**B** — جای‌گذاری یک تصمیم domain خرابی است. replication فقط وقتی کمک می‌کند که replicaها در یک خرابی مشترک نباشند.

</details>

4. **(سنیور)** برای کلاستر ClickHouse باید storage انتخاب کنی. local NVMe PV یا storage بلاکی شبکه‌ای؟ معیار تصمیم چیست؟
   - A) همیشه storage شبکه‌ای، چون مدرن‌تر است
   - B) local NVMe بهترین throughput کوئری و merge را می‌دهد و اپلیکیشن داده را replicate می‌کند؛ storage شبکه‌ای جابه‌جایی را آسان می‌کند ولی latency و domain خرابی مشترک اضافه می‌کند. بر اساس مدل خرابی و اینکه روش replication و recovery تست شده باشد انتخاب کن
   - C) همیشه local، چون storage شبکه‌ای نمی‌تواند replicate شود
   - D) برای analytics انتخاب اهمیتی ندارد

<details><summary>جواب</summary>

**B** — storage محلی سریع است ولی داده را به node گره می‌زند، پس replication باید از خرابی node جان سالم به در ببرد. storage شبکه‌ای latency و domain خرابی مشترک را در برابر تحرک می‌دهد. پاسخ درست به طراحی replication و مسیر recovery تست‌شده بستگی دارد.

</details>

5. **(سنیور)** تیمی پیشنهاد می‌دهد برای «ریست» کردن یک کلاستر خراب Kafka، StatefulSet را حذف و دوباره بسازند. اول چه چیزی را چک می‌کنی؟
   - A) هیچ؛ ساختن دوباره همیشه امن است
   - B) اینکه PVCها و reclaim policy داده را نگه می‌دارند یا پاک می‌کنند، اینکه operator و شناسه‌ی کلاستر دوباره هم‌خوان می‌شوند یا نه، و اینکه قبل از هر مرحله‌ی مخرب backup تأییدشده وجود دارد
   - C) اینکه نام topicها حروف بزرگ دارد یا نه
   - D) اینکه کلاستر بیشتر از ۱۰۰ node دارد یا نه

<details><summary>جواب</summary>

**B** — ساختن دوباره‌ی یک workload stateful می‌تواند داده را دوباره وصل کند، پاک کند، یا یک کلاستر دوم با هویت متناقض بسازد. قبل از اقدام، رفتار storage و reclaim، metadata و backup را تأیید کن.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| Pod | کوچک‌ترین واحد قابل استقرار: یک یا چند کانتینر با شبکه و volume مشترک؛ دورریختنی |
| Deployment در برابر StatefulSet | Deployment: podهای قابل جایگزین با نام تصادفی؛ StatefulSet: نام ثابت و مرتب و storage اختصاصی هر pod |
| PersistentVolumeClaim | درخواست storage که می‌تواند از pod استفاده‌کننده‌اش بیشتر عمر کند |
| Request در برابر limit | request: برای زمان‌بندی رزرو می‌شود؛ limit: سقف است؛ حافظه‌ی بالاتر از limit باعث OOMKilled می‌شود |
| چرا request حافظه برابر limit برای دیتابیس‌های JVM | از eviction زیر فشار node جلوگیری و ظرفیت را قابل پیش‌بینی می‌کند |
| Pod anti-affinity | قانون جای‌گذاری که replicaها را روی nodeها یا zoneهای متفاوت نگه می‌دارد |
| Operator | یک controller که روش‌های عملیاتی یک سیستم مشخص را از طریق یک custom resource کدنویسی می‌کند |
| Local PV در برابر storage شبکه‌ای | local: سریع‌ترین، وابسته به node، نیازمند replication در سطح اپلیکیشن؛ شبکه‌ای: متحرک، latency بیشتر، domain خرابی مشترک |

## 🔗 ادامه

- **[[docker]]** image هایی که این podها اجرا می‌کنند را توضیح می‌دهد.
- **[[spark-on-kubernetes]]** executor و driver اسپارک را به صورت pod نشان می‌دهد، با operatorها و تنظیمات منابعی که لازم دارند.
- **[[yarn-vs-k8s]]** زمان‌بندی Kubernetes را با صف‌های YARN و data locality مقایسه می‌کند.
- **[[kafka-architecture]]** مدل replication و خرابی را توضیح می‌دهد که انتخاب storage برای brokerها به آن وابسته است.
- **[[clickhouse]]** replica، merge و TTL را پوشش می‌دهد که برای اندازه‌گیری StatefulSet بالا اهمیت دارند.
- **[[onprem-iran]]** اجرای چنین کلاستری روی سخت‌افزار خودت را بررسی می‌کند، جایی که برنامه‌ریزی storage و خرابی node محدودیت اصلی است.
- مستندات Kubernetes درباره‌ی StatefulSet، کلاس‌های storage و زمان‌بندی را بخوان؛ سپس مستندات operator را برای نسخه‌های دقیق منابعی که کلاسترت استفاده می‌کند بخوان.

