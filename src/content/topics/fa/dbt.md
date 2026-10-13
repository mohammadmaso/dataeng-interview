---
order: 125
tid: "dbt"
track: "11"
section: null
num: "11.4"
title: "dbt Deep Dive"
type: "technology"
level: "advanced"
priority: "P0"
est_hours: 5
summary: "تبدیل داده با SQL و مهندسی نرم‌افزار: model، ref و source، materializationها (view، table، incremental، ephemeral)، تست‌ها، snapshot برای SCD2، macro و Jinja، مستندسازی و lineage. adapterها برای ClickHouse، StarRocks، Spark و Trino."
covers: ["incremental strategies (append, merge, delete+insert, insert_overwrite)","model layering (staging → intermediate → marts)","CI with slim builds"]
interview_qs: []
extras: []
prerequisites: ["sql-fundamentals", "sql-ddl-merge"]
related: ["dimensional-modeling", "scd", "data-quality", "dataops-cicd", "dedup-idempotent-loads", "medallion-architecture", "airflow", "semantic-layer"]
---

## 🎯 خلاصه

> **dbt** (data build tool) به تو اجازه می‌دهد «T» در ELT را به صورت دستورهای `SELECT` در فایل بنویسی. dbt هر فایل را به یک جدول یا view تبدیل می‌کند، ترتیب اجرا را از روی ارجاع‌ها بین آن‌ها تشخیص می‌دهد، تست‌ها را اجرا می‌کند و مستندات و lineage تولید می‌کند. dbt خودش داده را جابه‌جا نمی‌کند؛ کار را warehouse انجام می‌دهد.

- یک **model** یک فایل `.sql` است که یک `SELECT` دارد. dbt آن را بسته به **materialization** در `CREATE VIEW`، `CREATE TABLE` یا یک بارگذاری incremental قرار می‌دهد.
- `{{ ref('model') }}` و `{{ source('system', 'table') }}` گراف وابستگی را می‌سازند. هرگز نام schema یا ترتیب اجرا را دستی نمی‌نویسی.
- **تست‌ها** (unique، not null، accepted values، relationships) و **snapshotها** (تاریخچه‌ی ردیف‌های در حال تغییر، SCD Type 2) کنار مدل‌هایی قرار می‌گیرند که بررسی می‌کنند.
- موضوعات سنیور: **استراتژی‌های incremental** (چطور یک مدل فقط داده‌ی جدید را امن پردازش می‌کند)، **لایه‌بندی** (staging، intermediate، marts)، و **CI مبتنی بر state** (فقط چیزی را بساز و تست کن که تغییر کرده).

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** یک کتاب دستور پخت که هر دستور می‌گوید به کدام دستور دیگر نیاز دارد («اول فوند را بساز»). یک ابزار کتاب را می‌خواند، ترتیب را تشخیص می‌دهد، هر غذا را به ترتیب درست می‌پزد، و قبل از سرو هر غذا آن را با یک لیست قانون چک می‌کند.

بدون dbt، یک warehouse اغلب پوشه‌ای از اسکریپت‌های SQL دارد، یک زمان‌بندی که آن‌ها را به ترتیب اجرا می‌کند، و کسی مطمئن نیست کدام اسکریپت کدام گزارش را تغذیه می‌کند. با dbt وابستگی داخل خود SQL نوشته می‌شود:

```sql
-- models/staging/stg_orders.sql
select
    order_id,
    customer_id,
    cast(order_ts as timestamp) as order_ts,
    lower(status) as status,
    amount_cents / 100.0 as amount
from {{ source('shop', 'raw_orders') }}
```

```sql
-- models/marts/customer_revenue.sql
select
    customer_id,
    count(*) as orders,
    sum(amount) as revenue
from {{ ref('stg_orders') }}
where status = 'shipped'
group by customer_id
```

دو چیز جدید است:

- `{{ source(...) }}` به یک جدول خام اشاره می‌کند که dbt آن را نمی‌سازد. آن را یک بار در یک فایل YAML اعلام می‌کنی.
- `{{ ref(...) }}` به model دیگری اشاره می‌کند. dbt از آن می‌فهمد که `customer_revenue` باید بعد از `stg_orders` اجرا شود، و نام را با schema و جدول درست در warehouse جایگزین می‌کند.

`dbt build` را اجرا کن؛ dbt هر دو model را به ترتیب می‌سازد و تست‌هایی را که به آن‌ها وصل کرده‌ای اجرا می‌کند.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

**کامپایل.** وقتی دستوری اجرا می‌کنی، dbt پروژه را می‌خواند، Jinja (`{{ ... }}` و `{% ... %}`) را رندر می‌کند، هر `ref` و `source` را به نام واقعی رابطه تبدیل می‌کند، و یک DAG می‌سازد. بعد SQL را به ترتیب وابستگی به warehouse می‌فرستد. نتیجه را در `target/compiled/` می‌توانی ببینی؛ وقتی یک model رفتار عجیبی دارد، این اولین جایی است که باید نگاه کنی.

**Materializationها.** تنظیم `materialized` مشخص می‌کند چه ساخته شود:

| Materialization | چه چیزی در warehouse ساخته می‌شود | کی استفاده شود |
|---|---|---|
| `view` | یک view؛ کوئری موقع خواندن اجرا می‌شود | مدل‌های کوچک، ارزان، یا کم‌خوانده‌شده |
| `table` | بازسازی کامل در هر اجرا | جدول‌های کوچک تا متوسط، یا وقتی بازسازی ارزان است |
| `incremental` | یک بار ساخته می‌شود، بعد در هر اجرا ردیف‌های جدید اضافه یا merge می‌شوند | جدول‌های fact بزرگ |
| `ephemeral` | ساخته نمی‌شود؛ به صورت CTE داخل مدل‌هایی که از آن استفاده می‌کنند تزریق می‌شود | منطق کمکی که لازم نیست جداگانه کوئری شود |

**مدل‌های incremental.** در اجرای اول، مدل جدول کامل را می‌سازد. در اجراهای بعدی، `is_incremental()` درست است و کوئری مدل فقط ردیف‌های جدید یا تغییر‌یافته را انتخاب می‌کند. `{{ this }}` به جدول موجود اشاره دارد:

```sql
-- models/marts/fct_orders.sql (dialect سازگار با PostgreSQL)
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id'
) }}

select order_id, customer_id, order_ts, status, amount
from {{ ref('stg_orders') }}
{% if is_incremental() %}
  -- look back 3 days so late-arriving updates are picked up
  where order_ts >= (select max(order_ts) from {{ this }}) - interval '3 days'
{% endif %}
```

**استراتژی‌های incremental.** استراتژی تعیین می‌کند ردیف‌های جدید چطور نوشته شوند. دسترسی به هر استراتژی به adapter بستگی دارد، پس مستندات adapter خودت را ببین:

- **append**: ردیف‌های جدید را insert می‌کند. ساده و سریع است، ولی اگر یک بازه دو بار پردازش شود ردیف تکراری می‌سازد. فقط برای رویدادهای کاملاً تغییرناپذیر استفاده کن که مرحله‌ی dedup جای دیگری دارند.
- **merge**: روی `unique_key` upsert می‌کند. ردیف‌های منطبق به‌روز و بقیه insert می‌شوند. انتخاب معمول وقتی است که ردیف‌ها ممکن است تغییر کنند.
- **delete+insert**: ردیف‌هایی را که کلیدشان در دسته‌ی جدید است حذف می‌کند، بعد دسته را insert می‌کند. وقتی `MERGE` کند است یا در دسترس نیست مفید است.
- **insert_overwrite**: کل partitionهایی را که داده‌ی جدید لمس کرده جایگزین می‌کند. روی storage پارتیشن‌بندی‌شده (BigQuery، موتورهای مبتنی بر Spark) بسیار کارآمد است و وقتی partitionها کامل باشند ذاتاً idempotent است.

**تست‌ها.** تست‌های عمومی در YAML اعلام و به SQL کامپایل می‌شوند. یک تست وقتی قبول است که کوئری‌اش هیچ ردیفی برنگرداند:

```yaml
# models/marts/schema.yml
version: 2
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'cancelled']
```

تست تکی یک فایل `.sql` در پوشه‌ی `tests/` است که ردیف‌هایی را برمی‌گرداند که یک قانون را می‌شکنند. بسته‌هایی مثل `dbt_utils` تست‌های عمومی بیشتری اضافه می‌کنند.

**Snapshotها.** snapshot تغییر یک جدول منبعِ قابل‌تغییر را در طول زمان ثبت می‌کند و تاریخچه‌ی SCD Type 2 می‌سازد (یک ردیف برای هر نسخه، با تاریخ‌های اعتبار). dbt تغییر را یا از یک ستون timestamp تشخیص می‌دهد یا با مقایسه‌ی ستون‌های انتخابی:

```sql
{% snapshot customers_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}
select * from {{ source('shop', 'customers') }}
{% endsnapshot %}
```

snapshot یک دستور جدا دارد (`dbt snapshot`) و اغلب زمان‌بندی جدا دارد. ببین [[scd]].

**Macro و Jinja.** macro یک تابع قابل استفاده‌ی مجدد با Jinja است که متن SQL برمی‌گرداند:

```sql
-- macros/cents.sql
{% macro cents_to_dollars(column_name) %}
    ({{ column_name }} / 100.0)
{% endmacro %}

-- used in a model
select {{ cents_to_dollars('amount_cents') }} as amount from {{ source('shop', 'raw_orders') }}
```

از macro برای جلوگیری از کپی‌پیست منطق استفاده کن، نه برای پنهان کردن قانون کسب‌وکار. بازبین باید هنوز بتواند SQLی را که اجرا می‌شود بخواند.

**مستندات و lineage.** `dbt docs generate` یک سایت با توضیحات، ستون‌ها و یک گراف lineage می‌سازد که از `ref` و `source` ساخته می‌شود. این همان گرافی است که dbt برای اجرای پروژه استفاده کرده، پس تا وقتی همه از `ref` استفاده کنند دقیق می‌ماند.

**Adapterها.** dbt از طریق یک adapter، SQL را به dialect warehouse کامپایل می‌کند. adapterها برای موتورهای زیادی وجود دارند، از جمله ClickHouse، StarRocks، Spark و Trino (برای برخی، نگه‌داری توسط جامعه انجام می‌شود). SQL مدل فقط تا جایی قابل حمل است که dialectها با هم توافق داشته باشند؛ materializationها، استراتژی‌های incremental و توابع بین adapterها فرق می‌کنند.

## 🧠 عمق و trade-offها (سنیور)

- **درستی incremental سخت‌ترین بخش است.** فیلتر `where ts > (select max(ts) from {{ this }})` ردیف‌هایی را که دیر با timestamp قدیمی‌تر می‌رسند از دست می‌دهد. یک پنجره‌ی lookback این را حل می‌کند، ولی ردیف‌ها را دوباره پردازش می‌کند، پس استراتژی باید idempotent باشد (`merge` یا `insert_overwrite`). trade-off بین هزینه و کامل بودن است. قبل از انتخاب پنجره، واقعاً ببین داده چقدر دیر می‌رسد. ببین [[late-arriving-data]].
- **full refresh یک جایگزین معتبر است.** وقتی منطق عوض می‌شود، برای آن model دستور `dbt run --full-refresh` را اجرا کن (یا partitionهایش را بازسازی کن). modelی که فقط به صورت incremental ساخته می‌شود سخت تعمیر می‌شود؛ برای بازسازی طراحی کن.
- **لایه‌بندی گراف را منظم نگه می‌دارد.** ساختار رایج:
  - **staging**: یک model برای هر جدول منبع، فقط تغییر نام و cast، بدون join؛
  - **intermediate**: joinها و منطق کسب‌وکار قابل استفاده‌ی مجدد، که مستقیم به کاربر نشان داده نمی‌شوند؛
  - **marts**: جدول‌هایی که تحلیلگرها و ابزارهای BI کوئری می‌زنند، شکل‌یافته برای سؤال‌ها (ببین [[dimensional-modeling]]).

  لایه‌بندی جلوی این را می‌گیرد که marts مستقیم جدول‌های خام را بخوانند، پس تغییر در منبع فقط یک model staging را لمس می‌کند نه بیست گزارش را.
- **grain و کلیدهای model.** هر mart باید grain خودش را اعلام کند (یک ردیف به ازای هر سفارش، یا هر مشتری در هر روز) و کلیدش را تست کند. بیشتر حادثه‌های کیفیت داده در پروژه‌های dbt از joinی می‌آیند که ردیف‌ها را ضرب می‌کند؛ تست `unique` روی کلید پیش از داشبورد آن را می‌گیرد. ببین [[data-quality]].
- **CI با state (slim build).** اجرای کل پروژه روی هر pull request کند و پرهزینه است. یک نسخه از manifest تولید را نگه دار، و فقط چیزی را بساز که تغییر کرده و چیزهایی که به آن وابسته‌اند:

  ```bash
  # in CI: compare with the manifest saved from the last production run
  dbt build --select state:modified+ --defer --state ./prod-artifacts
  ```

  `state:modified+` مدل‌های تغییرکرده و همه‌ی پایین‌دستی‌هایشان را انتخاب می‌کند. `--defer` باعث می‌شود ارجاع‌های بالادستیِ تغییرنکرده به جدول‌های تولید اشاره کنند، پس PR نیازی به ساخت کل گراف ندارد. توجه کن که تغییر macro یا config می‌تواند روی مدل‌های زیادی اثر بگذارد، و اجرای CI هم برای هر PR به یک schema جدا نیاز دارد تا برخورد نکنند. ببین [[dataops-cicd]].
- **هزینه و رفتار warehouse.** یک `view` روی join بزرگ برای هر خواننده دوباره محاسبه می‌شود. یک `table` که هر روز روی یک منبع میلیاردی بازسازی شود پول را هدر می‌دهد. materialization را بر اساس الگوی خواندن و هزینه‌ی بازسازی انتخاب کن، و قبل از سرزنش dbt، query profile warehouse را ببین.
- **snapshot در مقیاس.** snapshot در هر اجرا هر ردیف را مقایسه می‌کند. روی منبع بزرگ و پهن، استراتژی timestamp را ترجیح بده و مطمئن شو ستون منبع قابل اتکاست. snapshot فقط به اندازه‌ی `updated_at` منبع خوب است.
- **کی dbt مناسب نیست.** dbt برای تبدیل‌های با شکل SQL داخل warehouse یا lakehouse است. پردازش ردیف‌به‌ردیف استریم، مهندسی ویژگی سنگین با Python، یا pipelineهایی که منطقشان بیشتر فراخوانی API است جای دیگری‌اند (ببین [[flink]] یا [[airflow]]). dbt هنوز می‌تواند یک تسک داخل orchestrator باشد.
- **چه چیزی را مانیتور کنیم.** روند شکست تست‌ها، مدت اجرای هر model، تعداد ردیف‌هایی که یک اجرای incremental می‌نویسد نسبت به اجرای قبلی (جهش ناگهانی یا صفر علامت هشدار است)، و تازگی منابع.

## 🧪 امتحان کن

از یک warehouse سازگار با PostgreSQL استفاده کن، یا هر موتوری که SQL زیر بدون تغییر در آن اجرا شود.

1. **یک مدل staging بنویس.** با منبع `raw_orders` که ستون‌های `id`، `cust`، `ts_text` (timestamp به صورت رشته) و `status_code` (۱ = placed، ۲ = shipped، ۳ = cancelled) را دارد، مدل `stg_orders` را با نام‌های درست، نوع timestamp و ستون `status` رمزگشایی‌شده بنویس.

2. **استراتژی انتخاب کن.** جدول `fct_payments` به‌روزرسانی وضعیت پرداخت را دریافت می‌کند. یک پرداخت ممکن است روزها بعد از `pending` به `captured` تغییر کند. استراتژی incremental را انتخاب کن، بلاک `config()` را بنویس، و توضیح بده چرا `append` اشتباه است.

3. **شکاف تست را پیدا کن.** یک mart دارای `order_id` است و به `customers` join می‌شود. کدام دو تست عمومی بیشترین باگ‌های رایج join را می‌گیرند، و تست رابطه را چطور بنویسی؟

<details><summary>پاسخ‌ها را ببین</summary>

**۱.**
```sql
select
    id as order_id,
    cust as customer_id,
    cast(ts_text as timestamp) as order_ts,
    case status_code
        when 1 then 'placed'
        when 2 then 'shipped'
        when 3 then 'cancelled'
    end as status
from {{ source('shop', 'raw_orders') }}
```
عبارت `case` کد را فقط در یک جا رمزگشایی می‌کند، پس همه‌ی martها معنای یکسانی می‌خوانند.

**۲.**
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id'
) }}
```
`append` اشتباه است چون همان پرداخت با وضعیت جدید دوباره می‌آید؛ دو ردیف برای یک پرداخت می‌سازد. `merge` روی `payment_id` ردیف موجود را به‌روز می‌کند. آن را با پنجره‌ی lookback ترکیب کن، چون یک پرداخت ممکن است روزها بعد از ایجادش تغییر کند.

**۳.** `unique` و `not_null` روی `order_id` ردیف‌های تکراری ناشی از join انتشاری (fan-out) را می‌گیرند. برای رابطه، تست `relationships` را بگذار تا هر `customer_id` در mart در `customers` وجود داشته باشد:
```yaml
- name: customer_id
  tests:
    - relationships:
        to: ref('customers')
        field: customer_id
```
تست `relationships` کلیدهای یتیم را می‌گیرد، که در غیر این صورت در inner join بی‌صدا حذف می‌شوند.

</details>

## 💬 گوشه‌ی مصاحبه

**«تفاوت view، table و مدل incremental چیست؟»**
- **جواب متوسط:** «view کوئری را موقع خواندن اجرا می‌کند، table در هر اجرا بازسازی می‌شود، و مدل incremental فقط ردیف‌های جدید یا تغییر‌یافته را اضافه می‌کند.»
- **جواب سنیور:** «این یک trade-off بین هزینه و درستی است. view ساختنش هزینه ندارد، ولی هر خواننده هزینه‌ی محاسبه را می‌پردازد. table ساده و درست است، ولی در هر اجرا بازسازی کامل دارد. مدل incremental روی داده‌ی بزرگ ارزان‌ترین است، ولی به کلید، استراتژی idempotent و lookback برای داده‌ی دیررسیده نیاز دارد. من بر اساس حجم، الگوی خواندن و دفعات تغییر منطق انتخاب می‌کنم، و مسیر full refresh را قبل از نیاز به آن برنامه‌ریزی می‌کنم.»

**«dbt را در CI چطور اجرا می‌کنی؟»** فقط آنچه تغییر کرده و هر چیزی که پایین‌دستی آن است را بساز، با مقایسه‌ی state نسبت به آخرین manifest تولید. مدل‌های بالادستیِ تغییرنکرده را به تولید ارجاع بده. تست‌ها را روی مدل‌های ساخته‌شده اجرا کن. به هر pull request یک schema جدا بده تا اجراها با هم برخورد نکنند.

**«چرا staging، intermediate و marts؟»** هر لایه یک وظیفه دارد. staging منبع را از بقیه‌ی پروژه جدا می‌کند. intermediate منطق قابل استفاده‌ی مجدد را نگه می‌دارد. marts جدول‌های پایدار را به کاربران نشان می‌دهند. با این کار تغییر schema منبع فقط یک جا را تحت تأثیر قرار می‌دهد.

> **🎤 این را در مصاحبه بگو:** «dbt به من مدل‌های SQL با وابستگی صریح، تست و lineage می‌دهد. materialization را بر اساس حجم و الگوی خواندن انتخاب می‌کنم، مدل‌های incremental را با کلید و پنجره‌ی lookback idempotent می‌کنم، و با لایه‌بندی و slim CI کاری می‌کنم که هر تغییر فقط چیزی را که به آن مربوط است دوباره بسازد.»

## ⚠️ اشتباهات رایج

> **⚠️ مراقب باش:** نوشتن نام schema یا جدول به صورت دستی به جای `ref` و `source`. گراف خراب می‌شود، lineage غلط است و CI نمی‌تواند تغییرات را درست انتخاب کند.

> **⚠️ مراقب باش:** استفاده از فیلتر `is_incremental()` روی timestamp بدون lookback. ردیف‌های دیررسیده بی‌صدا گم می‌شوند و کسی متوجه نمی‌شود تا وقتی جمع‌ها با منبع نمی‌خوانند.

- انتخاب `append` برای داده‌ی قابل‌تغییر، که ردیف تکراری تولید می‌کند.
- ساختن هر model به صورت `table` روی منبع بزرگ، و بعد تعجب از این‌که اجرای شبانه ساعت‌ها طول می‌کشد.
- گذاشتن منطق کسب‌وکار در staging، پس هر model پایین‌دستی آن را تکرار می‌کند.
- نگذاشتن تست `unique` روی کلید mart، پس join انتشاری بی‌صدا رد می‌شود.
- فراموش کردن این‌که نتیجه‌ی `dbt test` فقط به اندازه‌ی تست‌هایی است که اعلام کرده‌ای؛ بدون تست، تور ایمنی‌ای وجود ندارد.
- رفتار با macro به عنوان جعبه‌ی سیاه. macroای که یک فیلتر را پنهان کند، دیباگ حادثه را سخت می‌کند.

## ✅ خودت را بسنج

۱. `{{ ref('stg_orders') }}` چه کاری می‌کند که یک نام جدول ساده نمی‌کند؟
   - الف) کوئری را سریع‌تر اجرا می‌کند
   - ب) یک یال وابستگی در DAG می‌سازد و به رابطه‌ی درست برای محیط هدف تبدیل می‌شود
   - ج) نتیجه را کش می‌کند
   - د) داده را داخل model کپی می‌کند

<details><summary>پاسخ</summary>

**ب** — `ref` ترتیب ساخت و lineage را به dbt می‌گوید، و به schema و نام درست تبدیل می‌شود، پس همان پروژه در dev و prod اجرا می‌شود. نام جدول ثابت هیچ‌کدام را نمی‌دهد.

</details>

۲. یک مدل incremental با `merge` روی `order_id` است. یک ردیف دو روز بعد از اولین بارگذاری وضعیتش عوض می‌شود. اگر فیلتر `where order_ts > (select max(order_ts) from {{ this }})` باشد، محتمل‌ترین مشکل چیست؟
   - الف) `merge` نمی‌تواند ردیف‌ها را به‌روز کند
   - ب) فیلتر فقط ردیف‌های جدیدتر از آخرین timestamp بارگذاری‌شده را انتخاب می‌کند، پس ردیف به‌روزشده هرگز دوباره پردازش نمی‌شود
   - ج) `unique_key` باید timestamp باشد
   - د) model باید view باشد

<details><summary>پاسخ</summary>

**ب** — ردیف تغییرکرده `order_ts` قدیمی دارد، پس فیلتر از آن رد می‌شود. راه‌حل پنجره‌ی lookback است، یا فیلتر روی ستون `updated_at` که منبع نگه می‌دارد.

</details>

۳. یک pull request macro‌ای را تغییر می‌دهد که توسط ۴۰ مدل استفاده می‌شود. کدام روش CI هم کارآمد است و هم امن؟
   - الف) اجرای کل پروژه روی هر commit
   - ب) ساخت فقط مدل تغییرکرده و هیچ چیز دیگر
   - ج) استفاده از `state:modified+` با `--defer` در برابر manifest تولید، تا وابسته‌های macro تغییرکرده بازسازی و تست شوند
   - د) رد کردن CI برای تغییرات macro

<details><summary>پاسخ</summary>

**ج** — slim CI گره‌های تغییرکرده و وابسته‌هایشان را می‌سازد و `--defer` بقیه را به جدول‌های تولید وصل می‌کند. ساختن فقط مدل تغییرکرده، ۴۰ وابسته‌ای را که ممکن است تحت تأثیر باشند نادیده می‌گیرد.

</details>

۴. **(سنیور)** یک مدل incremental به نام `fct_orders` به ۲ میلیارد ردیف رسیده و هر اجرا چهار ساعت طول می‌کشد. کدام تغییر بهترین قدم بعدی است و چه چیزی را باید تضمین کند؟
   - الف) تبدیل به `view` تا چیزی ذخیره نشود
   - ب) استفاده از یک استراتژی آگاه از partition مثل `insert_overwrite` برای partitionهای اخیر (اگر adapter پشتیبانی کند)، با پنجره‌ی lookback که داده‌ی دیررسیده را پوشش دهد، و بررسی این‌که اجراهای مجدد idempotent باشند
   - ج) حذف `unique_key`
   - د) اجرای مدل به صورت `ephemeral`

<details><summary>پاسخ</summary>

**ب** — بازسازی فقط partitionهای اخیر کار را کم می‌کند و lookback داده‌ی دیررسیده را پوشش می‌دهد. چیزی که باید بررسی کنی idempotent بودن است: اجرای مجدد یک بازه باید همان جدول را بسازد. view باعث می‌شود هر خواننده چهار ساعت منطق را دوباره محاسبه کند، و `ephemeral` چیزی ذخیره نمی‌کند.

</details>

۵. **(سنیور)** به تاریخچه‌ی کامل SCD Type 2 برای یک جدول مشتری نیاز داری که به صورت درجا (in place) به‌روز می‌شود. چرا مدل incremental ابزار اشتباه است و ابزار درست کدام است؟
   - الف) مدل‌های incremental نمی‌توانند منبع را بخوانند
   - ب) مدل incremental فقط آخرین وضعیت هر کلید را نگه می‌دارد، پس نسخه‌های قدیمی بازنویسی می‌شوند؛ snapshot هر نسخه را با تاریخ‌های اعتبار ثبت می‌کند
   - ج) snapshotها نمی‌توانند از timestamp استفاده کنند
   - د) مدل‌های incremental همیشه تاریخچه را append می‌کنند

<details><summary>پاسخ</summary>

**ب** — merge یک ردیف فعلی برای هر کلید نگه می‌دارد. snapshot برای تاریخچه ساخته شده است: هر تغییر یک ردیف جدید با شروع و پایان می‌سازد، که همان SCD Type 2 است. مفاهیم [[scd]] را برای انتخاب نوع مناسب ببین.

</details>

## 🗂️ فلش‌کارت

| جلو | پشت |
|---|---|
| `ref()` | به model دیگری اشاره می‌کند؛ یک یال DAG می‌سازد و برای هر محیط به رابطه‌ی درست تبدیل می‌شود |
| `source()` | به جدول خامی اشاره می‌کند که در YAML اعلام شده؛ dbt آن را نمی‌سازد |
| `is_incremental()` | در اجراهای بعد از اولی درست است؛ فیلتر داخلش فقط ردیف‌های جدید یا تغییرکرده را انتخاب می‌کند |
| `unique_key` | کلیدی که merge یا delete+insert با آن ردیف موجود را به‌روز می‌کنند، نه این‌که تکرار کنند |
| `ephemeral` | در warehouse ساخته نمی‌شود؛ به صورت CTE داخل مدل‌هایی که از آن استفاده می‌کنند تزریق می‌شود |
| Snapshot | یک منبع در حال تغییر را در طول زمان ثبت می‌کند (SCD Type 2) با استراتژی timestamp یا check |
| `state:modified+` | مدل‌های تغییرکرده و همه‌ی پایین‌دستی‌هایشان را انتخاب می‌کند؛ برای slim CI همراه با `--defer` |
| پنجره‌ی lookback | یک دوره‌ی اخیر را در مدل incremental دوباره پردازش می‌کند تا ردیف‌های دیررسیده گرفته شوند |

## 🔗 ادامه

- [[dimensional-modeling]] و [[scd]] شکل martها و الگوهای تاریخچه‌ای را توضیح می‌دهند که dbt معمولاً می‌سازد.
- [[dedup-idempotent-loads]] توضیح می‌دهد چرا استراتژی‌های incremental باید قابل تکرار باشند.
- [[data-quality]] و [[dataops-cicd]] درباره‌ی تست‌ها و pipelineهای تحویل اطراف dbt عمیق‌تر می‌روند.
- [[airflow]] یک روش رایج برای زمان‌بندی اجرای dbt به عنوان تسک داخل یک pipeline بزرگ‌تر است.
- [[semantic-layer]] لایه‌ی متریکی را پوشش می‌دهد که اغلب روی martهای dbt قرار می‌گیرد.
- مستندات adapter خودت را ببین. نام materializationها، استراتژی‌های incremental و توابع SQL بین ClickHouse، StarRocks، Spark، Trino و warehouseهای ابری فرق دارد.
