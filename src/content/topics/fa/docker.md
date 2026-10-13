---
order: 136
tid: "docker"
track: "13"
section: null
num: "13.1"
title: "Containers & Docker"
type: "technology"
level: "beginner"
priority: "P1"
est_hours: 2
summary: "ایمیج، کانتینر، لایه‌ها، Docker Compose برای محیط توسعه‌ی محلی پایپ‌لاین (Kafka + Postgres + Spark)."
covers: []
interview_qs: []
extras: []
prerequisites: ["linux-shell"]
related: ["kubernetes-for-data", "dataops-cicd", "spark-on-kubernetes", "postgresql"]
---
## 🎯 خلاصه

> یک **container** یک process ایزوله است که از یک **image** اجرا می‌شود: یک پشته‌ی فقط‌خواندنی از **لایه‌ها** که کد، runtime و کتابخانه‌هایت را نگه می‌دارد. **Docker** image می‌سازد و container اجرا می‌کند، و با **Docker Compose** چند container را با هم بالا می‌آورد، مثلاً Kafka، Postgres و job اسپارکت را روی یک لپ‌تاپ.

- **image** یک قالب است؛ **container** نمونه‌ی در حال اجرای آن است.
- هر دستور Dockerfile یک **لایه** می‌سازد. لایه‌ها cache و به اشتراک گذاشته می‌شوند، پس ترتیب دستورها در سرعت build مهم است.
- container فضای فایل، شبکه و درخت process خودش را دارد، ولی **kernel میزبان را به اشتراک می‌گذارد**. ماشین مجازی نیست.
- **Volume**ها داده را بیرون از لایه‌ی قابل نوشتن container نگه می‌دارند. بدون آن‌ها، داده با container از بین می‌رود.
- از Compose برای توسعه‌ی محلی و تست استفاده کن. orchestration تولید ابزار دیگری است؛ به [[kubernetes-for-data]] نگاه کن.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** یک کانتینر حمل‌ونقل. جعبه همان است چه موز داخلش باشد چه لوازم الکترونیکی، و بدون باز کردن و بسته‌بندی دوباره از کشتی به کامیون منتقل می‌شود. image جعبه‌ی مهروموم‌شده با همه‌ی محتویات است؛ container همان جعبه وقتی روی یک کامیون مشخص در حال استفاده است.

چرا مهندس داده به این اهمیت می‌دهد؟ یک job اسپارک روی لپ‌تاپ کار می‌کند ولی روی کلاستر خراب می‌شود، چون نسخه‌ی پایتون فرق دارد. container دقیقاً همان پایتون، جاوا و کتابخانه‌ها را بسته‌بندی می‌کند، پس «روی سیستم من کار می‌کند» تبدیل می‌شود به «داخل image کار می‌کند».

سه دستور اصلی:

```bash
docker pull postgres:16            # دانلود image از registry
docker run --rm -it postgres:16 psql --version   # اجرای یک container از آن، و حذف بعد از پایان
docker ps                          # فهرست containerهای در حال اجرا
```

بخش `:16` یک **tag** است. tagها برچسب‌هایی هستند که ممکن است جابه‌جا شوند؛ برای build قابل تکرار، یک نسخه‌ی مشخص یا digest (`image@sha256:...`) را پین کن.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### Imageها، لایه‌ها و cache کردن build

یک Dockerfile می‌گوید image چطور ساخته شود:

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

# ۱) اول وابستگی‌ها: این لایه به ندرت عوض می‌شود، پس cache می‌ماند.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ۲) کد در آخر: این لایه با هر commit عوض می‌شود.
COPY pipeline/ ./pipeline/

# به عنوان root اجرا نکن.
RUN useradd --create-home appuser
USER appuser

ENTRYPOINT ["python", "-m", "pipeline.load"]
```

هر دستوری که فایل‌سیستم را تغییر می‌دهد (`COPY`، `RUN`، `ADD`) یک لایه می‌سازد. وقتی `pipeline/` را عوض می‌کنی، Docker لایه‌های cache شده تا `COPY requirements.txt` را دوباره استفاده می‌کند و هیچ پکیجی را دوباره نصب نمی‌کند. اگر قبل از `pip install` کل پروژه را کپی کرده بودی، هر تغییر کد همه‌ی وابستگی‌ها را دوباره نصب می‌کرد.

لایه‌ها بر اساس محتوا شناسایی و به اشتراک گذاشته می‌شوند. ده image که روی `python:3.12-slim` ساخته شده‌اند، آن لایه‌ی پایه را فقط یک بار روی دیسک نگه می‌دارند.

### Containerها، processها و namespaceها

وقتی یک container اجرا می‌شود، لینوکس این‌ها را به آن می‌دهد:

- یک **namespace برای process** (PID 1 داخل container همان entrypoint توست)،
- یک **namespace شبکه** (یک IP و پورت روی یک شبکه‌ی مجازی)،
- یک **namespace برای mount** (فایل‌سیستم image به‌علاوه‌ی یک لایه‌ی نازک قابل نوشتن روی آن)،
- محدودیت منابع از طریق **cgroups** (CPU و حافظه).

container مستقیم از kernel میزبان استفاده می‌کند. به همین دلیل container در میلی‌ثانیه بالا می‌آید، و به همین دلیل نمی‌تواند kernel سیستم‌عامل دیگری را اجرا کند. container لینوکس روی میزبان ویندوز از طریق یک ماشین مجازی لینوکسی در پس‌زمینه اجرا می‌شود.

### ذخیره‌سازی: لایه‌ی قابل نوشتن، volume و bind mount

| ذخیره‌سازی | محل داده | با `docker rm` باقی می‌ماند؟ | کاربرد |
|---|---|---|---|
| لایه‌ی قابل نوشتن | داخل container | خیر | فقط فایل موقت |
| Named volume | مدیریت‌شده توسط Docker | بله | فایل‌های دیتابیس (پوشه‌ی داده‌ی Postgres) |
| Bind mount | یک مسیر دلخواه روی میزبان | بله (روی میزبان است) | کد منبع در توسعه |

نوشتن حجم زیاد داده در لایه‌ی قابل نوشتن کند است و container را سنگین می‌کند. دیتابیس و خروجی را روی volume بگذار.

### شبکه در Compose

Compose برای پروژه یک شبکه‌ی خصوصی می‌سازد. containerها با **نام سرویس** همدیگر را پیدا می‌کنند: یک job به `postgres:5432` وصل می‌شود، نه به `localhost`. فقط پورت‌هایی که با `ports:` منتشر کرده‌ای از میزبان قابل دسترسی‌اند.

### یک پشته‌ی محلی واقع‌گرایانه

```yaml
# compose.yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: dev
      POSTGRES_PASSWORD: dev_only_password
      POSTGRES_DB: warehouse
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dev -d warehouse"]
      interval: 5s
      timeout: 3s
      retries: 10

  loader:
    build: .
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DATABASE_URL: postgresql://dev:dev_only_password@postgres:5432/warehouse

volumes:
  pgdata:
```

با `docker compose up --build` اجرایش کن. loader فقط بعد از اینکه Postgres سالم گزارش شد شروع می‌شود. بدون healthcheck، `depends_on` فقط منتظر **شروع** container می‌ماند، نه منتظر اینکه دیتابیس اتصال بپذیرد، و loader در اولین تلاش کرش می‌کند.

رمزهایی مثل `dev_only_password` برای یک پشته‌ی محلی اشکالی ندارند و هرگز نباید مقدار production باشند. از یک فایل env که commit نمی‌شود، یا یک secret store استفاده کن.

## 🧠 عمق و trade-offها (سنیور)

- **اندازه‌ی image و سطح حمله.** از multi-stage build استفاده کن: در یک image builder با ابزارهای build کامپایل کن و فقط artifactها را در یک image runtime سبک کپی کن. image کوچک‌تر در کلاستر سریع‌تر pull می‌شود و بسته‌های آسیب‌پذیر کمتری دارد.
- **پین و اسکن.** image پایه را در production با digest پین کن، برای وصله‌های امنیتی منظم دوباره build کن، و image ها را در CI اسکن کن. «ماه پیش کار می‌کرد» قابل تکرار نیست اگر tag جابه‌جا شده باشد.
- **حداقل دسترسی در runtime.** به عنوان کاربر غیر root اجرا کن، قابلیت‌های غیرضروری را حذف کن، و secretها را mount کن نه اینکه در لایه‌ها بگذاری. هر چیزی که در یک دستور `RUN` نوشته شود در تاریخچه‌ی image می‌ماند، حتی اگر مرحله‌ی بعدی آن را پاک کند.
- **جاذبه‌ی داده در container.** سیستم‌های stateful (دیتابیس، brokerهای Kafka) در container به volume پایدار و برنامه‌ای برای از دست رفتن node نیاز دارند. برای لپ‌تاپ یک named volume کافی است. برای کلاستر به تضمین‌های storage و scheduling که در [[kubernetes-for-data]] آمده نیاز داری.
- **Java و حافظه.** پروسه‌های JVM باید محدودیت حافظه‌ی container را رعایت کنند. JVMهای جدیدتر محدودیت cgroup را می‌خوانند، ولی باز هم اندازه‌ی heap را صریح تعیین کن و جا برای حافظه‌ی off-heap بگذار. مستندات نسخه‌ی JVM خودت را بررسی کن.
- **قابل تکرار بودن build.** `apt-get install` یا `pip install` بدون نسخه، در روزهای مختلف image های متفاوت می‌دهد. با lock file نسخه‌ی وابستگی‌ها را پین کن.
- **محلی در برابر production.** Compose برای تست یکپارچگی با Kafka، Postgres و Spark واقعی عالی است. ولی scheduler نیست، failover چندنودی ندارد، و جای نگهداری state تولید نیست.
- **چه زمانی container لازم نیست.** یک اسکریپت کوچک که ماهی یک بار اجرا می‌شود ممکن است نیاز به image نداشته باشد. کار وابسته به GPU یا سخت‌افزار خاص ممکن است روی bare metal راحت‌تر باشد، بسته به درایورها و کلاستر.

> **⚠️ مراقب باش:** `localhost` داخل container خود container است. job در یک container که به `localhost:5432` وصل می‌شود شکست می‌خورد، حتی اگر Postgres روی میزبان اجرا شده باشد. در Compose از نام سرویس استفاده کن، یا در Docker Desktop برای رسیدن به میزبان از `host.docker.internal`.

## 🧪 امتحان کن

به Docker همراه با plugin مربوط به Compose نیاز داری.

1. یک Dockerfile برای یک اسکریپت کوچک پایتون بنویس که تعداد ردیف‌های یک جدول در Postgres را چاپ کند. فایل Compose بالا را استفاده کن، اسکریپت را به صورت bind mount وصل کن و یک بار اجرایش کن. بعد ترتیب Dockerfile را طوری عوض کن که `pip install` قبل از کپی کد اجرا شود، و توضیح بده وقتی اسکریپت را ویرایش می‌کنی cache build چه تغییری می‌کند.
2. `docker compose up -d postgres` را اجرا کن، یک جدول بساز، بعد `docker compose down` و دوباره `docker compose up -d postgres`. توضیح بده آیا جدول هنوز وجود دارد و چرا. بعد `docker compose down -v` را اجرا کن و دوباره بررسی کن.
3. اندازه‌ی image های `python:3.12` و `python:3.12-slim` را با `docker images` پیدا کن. توضیح بده تفاوتشان چیست و برای image تولید چرا مهم است.

<details><summary>جواب‌ها را نشان بده</summary>

**۱.** ویرایش `main.py` فقط لایه‌ی آخرین `COPY` را عوض می‌کند؛ لایه‌ی `pip install` از cache استفاده می‌شود، پس build در چند ثانیه تمام می‌شود. در ترتیب اول، کپی کل پروژه قبل از `pip install` لایه‌ی نصب را در هر ویرایش نامعتبر می‌کند.

**۲.** جدول بعد از `down` و `up` باقی است چون named volume به نام `pgdata` حفظ شده. `down -v` named volumeها را هم حذف می‌کند، پس جدول از بین می‌رود. درس: `down` داده را نگه می‌دارد، `down -v` آن را نابود می‌کند.

**۳.** image کامل شامل کامپایلرها، header ها و بسته‌های سیستمی زیادی است که برای ساخت کتابخانه‌های پایتون لازم‌اند. image slim فقط runtime را دارد. image کوچک‌تر سریع‌تر pull می‌شود و بسته‌های کمتری در معرض اسکنر آسیب‌پذیری قرار می‌دهد. اندازه‌ها بین نسخه‌ها عوض می‌شوند، پس مقادیر را از خروجی `docker images` خودت بخوان.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «تفاوت image و container چیست؟»**

- **جواب متوسط:** «image یک قالب فقط‌خواندنی از لایه‌هاست. container نمونه‌ی در حال اجرای یک image است، با یک لایه‌ی قابل نوشتن، شبکه و process خودش.»
- **جواب سنیور:** «image تغییرناپذیر و شناسایی‌شده با محتواست، پس یک digest همیشه یک فایل‌سیستم را نشان می‌دهد. container یک لایه‌ی نازک قابل نوشتن، namespaceها و محدودیت cgroup را روی kernel میزبان اضافه می‌کند. برای داده، نکته‌ی مهم این است که لایه‌ی قابل نوشتن دور انداختنی است، پس state باید در volume یا سرویس بیرونی باشد.»

**سؤال: «چرا برای یک pipeline داده از container استفاده می‌کنی؟»**

با قابل تکرار بودن (runtime و وابستگی‌های پین‌شده)، ایزولاسیون (تداخل نسخه‌ی کتابخانه‌ها) و قابل حمل بودن (همان image در CI، staging و کلاستر) جواب بده. بگو image تمام ماجرا نیست: تنظیمات، secretها و state باید جدا مدیریت شوند.

> **🎤 این را در مصاحبه بگو:** «job را containerize می‌کنم تا runtime و وابستگی‌هایش همراه کد نسخه‌بندی شوند. image ها در CI با پایه‌ی پین‌شده build می‌شوند، با کاربر غیر root اجرا می‌شوند، و تنظیمات و secretها را از بیرون image می‌گیرند. state در volume یا سرویس مدیریت‌شده است، نه در لایه‌ی container.»

## ⚠️ اشتباهات رایج

> **⚠️ مراقب باش:** `docker compose down -v` named volumeها را حذف می‌کند، یعنی دیتابیس از بین می‌رود. آن را یک تصمیم آگاهانه بدان، نه یک عادت.

- کپی کردن کل سورس قبل از نصب وابستگی‌ها، که باعث می‌شود هر تغییر کد همه چیز را دوباره build کند.
- استفاده از `:latest` در production، که باعث می‌شود دو استقرار از «همان» image متفاوت باشند.
- نوشتن فایل‌های دیتابیس در لایه‌ی قابل نوشتن container به جای volume.
- گذاشتن رمز عبور در خط `ENV` داخل Dockerfile یا فایل Compose که commit شده؛ آن‌ها در تاریخچه‌ی image می‌مانند.
- اجرای همه‌چیز به عنوان root.
- فرض اینکه `depends_on` منتظر آماده شدن دیتابیس می‌ماند؛ فقط منتظر شروع container می‌ماند، مگر اینکه healthcheck اضافه کنی.
- نگاه کردن به container مثل یک VM و SSH زدن به آن برای رفع مشکل. به جای آن image را دوباره build کن.

## ✅ خودت را بسنج

1. رابطه‌ی image و container چیست؟
   - A) همان چیز با دو اسم
   - B) image یک قالب فقط‌خواندنی است؛ container نمونه‌ی در حال اجرای آن است
   - C) container قالب است؛ image یک process در حال اجرا
   - D) image همیشه یک kernel کامل سیستم‌عامل دارد

<details><summary>جواب</summary>

**B** — imageها قالب‌های تغییرناپذیر از لایه‌اند؛ containerها نمونه‌های در حال اجرا با یک لایه‌ی قابل نوشتن روی آن.

</details>

2. یک خط کد را عوض می‌کنی و دوباره build می‌کنی. مرحله‌ی `pip install` دوباره اجرا می‌شود و ۳ دقیقه طول می‌کشد. محتمل‌ترین مشکل Dockerfile چیست؟
   - A) image پایه خیلی بزرگ است
   - B) کپی کد (`COPY . .`) قبل از `pip install` آمده، پس cache لایه‌ی وابستگی با هر تغییر کد نامعتبر می‌شود
   - C) `pip` نمی‌تواند cache شود
   - D) container به حافظه‌ی بیشتری نیاز دارد

<details><summary>جواب</summary>

**B** — اول فایل وابستگی را کپی کن، نصب کن، بعد کد را کپی کن. لایه‌های بعد از یک لایه‌ی تغییر کرده دوباره ساخته می‌شوند، پس ترتیب دستورها cache را کنترل می‌کند.

</details>

3. یک container پایپ‌لاین به `localhost:5432` وصل نمی‌شود. Postgres در container دیگری از همان پروژه‌ی Compose اجرا می‌شود. connection string باید چه چیزی استفاده کند؟
   - A) `localhost:5432`
   - B) نام سرویس در Compose، برای مثال `postgres:5432`
   - C) شناسه‌ی تصادفی container
   - D) `127.0.0.1` با پورت متفاوت

<details><summary>جواب</summary>

**B** — داخل container، `localhost` به همان container اشاره می‌کند. Compose به سرویس‌ها نام DNS هم‌نام سرویس‌شان را روی شبکه‌ی پروژه می‌دهد.

</details>

4. کدام انتخاب ذخیره‌سازی باعث می‌شود دیتابیس Postgres بعد از `docker compose down` باقی بماند؟
   - A) لایه‌ی قابل نوشتن container
   - B) یک named volume
   - C) یک متغیر محیطی
   - D) یک دستور `RUN` در Dockerfile

<details><summary>جواب</summary>

**B** — named volumeها با حذف container باقی می‌مانند. `down -v` استثنای آن است که آن‌ها را هم حذف می‌کند.

</details>

5. **(سنیور)** image تولید تیم از `python:3` ساخته می‌شود و build قابل تکرار نیست. یک مجموعه تغییر مشخص پیشنهاد بده.

   - A) از `:latest` استفاده کن تا همیشه جدیدترین وصله‌ها را بگیری
   - B) پایه را به نسخه یا digest مشخص پین کن، وابستگی‌ها را با lock file پین کن، multi-stage build استفاده کن، با کاربر غیر root اجرا کن، و در CI به‌صورت زمان‌بندی‌شده دوباره build و اسکن کن
   - C) محیط مجازی را داخل image commit کن
   - D) job را روی لپ‌تاپ اجرا کن

<details><summary>جواب</summary>

**B** — قابل تکرار بودن به ورودی‌های پین‌شده نیاز دارد. build زمان‌بندی‌شده هنوز وصله‌های امنیتی می‌گیرد، ولی عمدی است و تغییر نسخه یا digest در code review دیده می‌شود.

</details>

## 🗂️ فلش‌کارت

| Front | Back |
|---|---|
| image در برابر container | image: قالب لایه‌ای تغییرناپذیر؛ container: نمونه‌ی در حال اجرا با لایه‌ی قابل نوشتن |
| چرا ترتیب دستورهای Dockerfile مهم است | تغییر یک لایه همه‌ی لایه‌های بعدی cache را نامعتبر می‌کند |
| container در برابر VM | container kernel میزبان را به اشتراک می‌گذارد و با namespace و cgroup ایزوله می‌شود؛ VM kernel خودش را دارد |
| `localhost` داخل container | به خود container اشاره می‌کند، نه میزبان یا containerهای دیگر |
| named volume در برابر لایه‌ی قابل نوشتن | volume با حذف container باقی می‌ماند؛ لایه‌ی قابل نوشتن همراه container از بین می‌رود |
| `depends_on` به‌تنهایی | منتظر شروع container می‌ماند، نه آماده شدن سرویس؛ با `condition: service_healthy` و healthcheck کامل کن |
| چرا tag یا digest را پین کنیم | tagی مثل `latest` جابه‌جا می‌شود، پس یک نام در طول زمان می‌تواند image های مختلف داشته باشد |
| `docker compose down -v` | containerها و named volumeها را حذف می‌کند، پس داده پاک می‌شود |

## 🔗 ادامه

- **[[kubernetes-for-data]]** اجرای سیستم‌های داده‌ی stateful در کلاستر را توضیح می‌دهد، جایی که containerها به storage پایدار و scheduling نیاز دارند.
- **[[dataops-cicd]]** نشان می‌دهد image ها چطور در CI/CD ساخته، تست و مستقر می‌شوند.
- **[[linux-shell]]** و **[[os-jvm-essentials]]** process، namespace و رفتار حافظه‌ی زیر container را توضیح می‌دهند.
- **[[spark-on-kubernetes]]** نشان می‌دهد job های اسپارک چطور به صورت container در کلاستر اجرا می‌شوند.
- **[[postgresql]]** دیتابیس استفاده‌شده در مثال Compose را توضیح می‌دهد.
- مستندات رسمی Docker درباره‌ی بهترین روش‌های Dockerfile و healthcheck در Compose را بخوان؛ نحو و پیش‌فرض‌ها با زمان تغییر می‌کنند.

