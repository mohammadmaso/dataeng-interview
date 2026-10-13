---
term: "گراف جهت‌دار بدون دور (DAG)"
term_en: "DAG (directed acyclic graph)"
aliases: ["dag", "گراف جهت‌دار بدون دور"]
category: orchestration
short: "گرافی از taskها با وابستگی‌های جهت‌دار و بدون دور که ترتیب اجرای معتبر pipeline را تعیین می‌کند."
related: ["backfill", "idempotency", "etl-elt"]
---
## به زبان ساده (مبتدی)
یک دستور پخت مرحله‌هایی دارد که به هم وابسته‌اند: سبزی‌ها را باید قبل از سرخ‌کردن خرد کنید، ولی می‌توانید همزمان آب را بجوشانید. DAG چنین دستور پختی به‌شکل نمودار است: جعبه‌ها (taskها) با پیکان (وابستگی) که هرگز به عقب برنمی‌گردند. این به scheduler اجازه می‌دهد مراحل مستقل را موازی و مراحل وابسته را به ترتیب درست اجرا کند.

## نحوه‌ی کار (متوسط)
گره‌ها taskها هستند و یال‌ها می‌گویند «B بعد از A اجرا می‌شود». بدون دور بودن مهم است چون دور یعنی taskها تا ابد منتظر هم می‌مانند. scheduler ترتیب توپولوژیک را حساب می‌کند و taskهایی را که بالادستی‌هایشان موفق شده‌اند، تا حد ممکن موازی اجرا می‌کند. orchestratorها (Airflow، Dagster، Prefect) pipeline را با DAG تعریف می‌کنند و زمان‌بندی، retry، SLA، backfill و observability اضافه می‌کنند.

```
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def sales_pipeline():
    @task
    def extract(): ...
    @task
    def transform(raw): ...
    @task
    def load(clean): ...
    load(transform(extract()))      # extract -> transform -> load

sales_pipeline()
```

همین ایده در Spark (logical plan و DAG مرحله‌های job)، dbt (گراف وابستگی مدل‌ها از `ref()`) و Flink (job graph) هم هست. در Airflow هر اجرای DAG به یک logical date/data interval وابسته است و هر task instance وضعیتی مثل queued، running، success، failed و up_for_retry دارد.

## نگاه سنیور و نکات مصاحبه
راهنمای طراحی: taskها کوچک، خودتوان و پارامتری روی تاریخ منطقی باشند؛ منطق سنگین در کد سطح بالای DAG که scheduler parse می‌کند نگذارید؛ به‌جای polling پرمصرف از sensor/deferrable operator یا زمان‌بندی مبتنی بر asset استفاده کنید؛ همزمانی را محدود کنید و برای منابع مشترک pool بگذارید؛ داده‌ی بزرگ را با XCom رد نکنید (ارجاع بدهید). DAG به‌طور ذاتی حلقه یا fan-out پویا را بیان نمی‌کند پس dynamic task mapping لازم است. زمان‌بندی event-driven یا data-aware (dataset/asset) از اتصال شکننده‌ی زمانی بین DAGها جلوگیری می‌کند.

سؤال مصاحبه: «Airflow DAG را چطور قابل‌اعتماد می‌کنی؟» پاسخ: taskهای کوچک و خودتوان بر اساس data interval؛ retry با backoff و timeout؛ هشدار و SLA؛ بررسی کیفیت داده به‌صورت task؛ طراحی امن برای backfill بدون سورپرایز depends_on_past؛ و poolهای منبع صریح.
