---
order: 35
tid: "python-production"
track: "3"
section: null
num: "3.6"
title: "Production-Grade Python for Pipelines"
type: "practice"
level: "intermediate"
priority: "P1"
est_hours: 3
summary: "کد پایپ‌لاین قابل اعتماد: retry با backoff، idempotency، لاگ ساخت‌یافته، مدیریت پیکربندی و secret، تست با pytest و Testcontainers، بسته‌بندی با uv یا poetry، lint و type check."
covers: ["project structure","unit vs integration tests for data","mocking sources","pydantic for schema validation"]
interview_qs: []
extras: []
prerequisites: ["python-core", "python-concurrency"]
related: ["python-dataframes", "api-ingestion", "dedup-idempotent-loads", "dlq-bad-records", "data-quality", "dataops-cicd", "docker"]
---

## 🎯 خلاصه

> یک اسکریپت پایتون که یک بار روی لپ‌تاپ کار می‌کند، پایپ‌لاین نیست. یک پایپ‌لاین **production** کدی است که **می‌تواند امن دوباره اجرا شود**، **با زمینه‌ی کافی و بلند شکست می‌خورد**، **تنظیماتش را بیرون از کد می‌خواند**، و **قبل از رسیدن به داده‌ی واقعی تست شده** است.

- فقط خطاهای **گذرا** را دوباره امتحان کن، با exponential backoff و jitter.
- **Idempotency**: اجرای دوباره‌ی یک batch باید همان نتیجه را بدهد. از کلید و upsert استفاده کن، نه `INSERT` کورکورانه.
- **لاگ‌های ساخت‌یافته** (JSON با فیلد) تا بتوانی خطاها را کوئری بزنی، نه جمله‌ها را grep کنی.
- **تنظیمات** از متغیرهای محیطی؛ **secret** هرگز داخل مخزن کد.
- **تست**: تست واحد سریع برای تبدیل‌ها و اعتبارسنجی، و چند تست یکپارچه روی دیتابیس واقعی داخل کانتینر.
- **ابزارها**: پروژه با lockfile (`uv` یا `poetry`)، `ruff` برای lint، `mypy` برای تایپ، و `pytest` برای تست.

## 🌱 از ساده شروع کنیم (مبتدی)

> **💡 تشبیه:** یک آشپزخانه را تصور کن. دستور پختی که یک بار جواب داده، یادداشتی روی دستمال کاغذی است. دستور پخت production فهرست دقیق مواد (lockfile)، تنظیم فر از روی دکمه‌ی دیوار و نه از حافظه (config)، تایمر و برنامه برای وقتی فر خراب می‌شود (retry)، و چشیدن قبل از سرو (تست) را دارد.

این کوچک‌ترین نسخه‌ی یک مرحله‌ی پایپ‌لاین است که همین قواعد را دنبال می‌کند. ورودی را اعتبارسنجی می‌کند، ردیف‌های بد را کنار می‌گذارد، و خلاصه را لاگ می‌کند:

```python
import json
import logging

from pydantic import BaseModel, Field, ValidationError

log = logging.getLogger("pipeline")
logging.basicConfig(level=logging.INFO)

class Order(BaseModel):
    order_id: int = Field(gt=0)
    customer: str = Field(min_length=1)
    amount: float = Field(ge=0)

raw = [
    {"order_id": 1, "customer": "ali", "amount": 10.0},
    {"order_id": -2, "customer": "sara", "amount": 5.0},   # bad: id must be > 0
    {"order_id": 3, "customer": "", "amount": 7.0},        # bad: empty customer
]

valid, rejected = [], 0
for row in raw:
    try:
        valid.append(Order.model_validate(row))
    except ValidationError:
        rejected += 1

log.info("batch done: %d valid, %d rejected", len(valid), rejected)
# INFO:pipeline:batch done: 1 valid, 2 rejected
```

دو ایده را نگه دار: ردیف‌های بد را **صریحاً رد کن** به‌جای اینکه اجرا را بشکنند یا بی‌صدا رد شوند، و **تعداد را گزارش کن** تا کسی بتواند ببیند چه اتفاقی افتاده.

## 🔧 واقعاً چطور کار می‌کند (متوسط)

### ساختار پروژه

پایپ‌لاینی که دیگران (Airflow، cron، کانتینر) اجرایش می‌کنند باید یک بسته‌ی قابل نصب باشد، نه یک اسکریپت ولو:

```text
orders-pipeline/
  pyproject.toml          # وابستگی‌ها، تنظیمات ابزارها، نقطه‌ی ورود
  uv.lock                 # نسخه‌های دقیق (commit کن)
  src/pipeline/
    __init__.py
    models.py             # اسکیماها (pydantic)
    retry.py              # کمکی retry
    load.py               # نوشتن idempotent
    config.py             # تنظیمات از محیط
  tests/
    test_models.py
    test_retry.py
    test_load.py
```

ساختار `src/` تست‌ها را وادار می‌کند بسته‌ی نصب‌شده را import کنند، که خطاهای بسته‌بندی را زود می‌گیرد. پروژه را با `uv init --package orders-pipeline` بساز، وابستگی‌ها را با `uv add pydantic` اضافه کن، و ابزارهای توسعه را با `uv add --dev pytest ruff mypy`. همه را با `uv run pytest`، `uv run ruff check .` و `uv run mypy src` اجرا کن.

### اسکیما با pydantic

اعتبارسنجی را در مرز، یعنی جایی که داده وارد کدت می‌شود، انجام بده. pydantic نسخه‌ی ۲ تبدیل نوع می‌کند و محدودیت‌ها را اعمال می‌کند:

```python
# src/pipeline/models.py
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

class Order(BaseModel):
    order_id: int = Field(gt=0)
    customer: str = Field(min_length=1)
    amount: Decimal = Field(ge=0)
    created_at: datetime
```

`Decimal` از خطای گرد کردن float برای پول جلوگیری می‌کند. `model_validate` رشته‌ها را تبدیل می‌کند: `"12.50"` می‌شود `Decimal("12.50")` و `"2026-01-05T10:00:00"` می‌شود `datetime`. بررسی ناموفق یک `ValidationError` با فهرست همه‌ی مشکلات می‌دهد، که دقیقاً همان چیزی است که در لاگ می‌خواهی.

### retry با backoff و jitter

فقط خطاهای **گذرا** را دوباره امتحان کن (timeout، قطع اتصال، HTTP 429 و 503). امتحان مجدد یک `ValidationError` یا HTTP 400 فقط وقت هدر می‌دهد. exponential backoff فاصله‌ی تلاش‌ها را زیاد می‌کند؛ jitter (تأخیر تصادفی) جلوی retry هم‌زمان ده‌ها worker را می‌گیرد:

```python
# src/pipeline/retry.py
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

def retry(
    attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,  # injectable for tests
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except retry_on:
                    if attempt == attempts:
                        raise
                    delay = min(max_delay, base_delay * 2 ** (attempt - 1))
                    sleep(random.uniform(0, delay))  # "full jitter"
            raise AssertionError("unreachable")
        return wrapper
    return decorator
```

استفاده: `@retry(attempts=5, retry_on=(ConnectionError, TimeoutError))`. به انتخاب‌های طراحی دقت کن: فراخواننده خطاهای قابل retry را نام می‌برد، آخرین خطا دوباره raise می‌شود (هرگز قورت داده نمی‌شود)، و `sleep` پارامتر است تا تست‌ها فوری اجرا شوند.

### لودهای idempotent

idempotent یعنی: اجرای دوباره‌ی یک batch همان وضعیت نهایی را بدهد. retry، اجرای مجدد بعد از crash و backfill همه به آن وابسته‌اند. مکانیزمش یک **کلید کسب‌وکاری** و یک **upsert** (درج یا به‌روزرسانی بر اساس کلید) است، نه یک `INSERT` ساده:

```python
# src/pipeline/load.py
from collections.abc import Iterable
from typing import Protocol

from pydantic import ValidationError

from pipeline.models import Order

class OrderStore(Protocol):
    def upsert(self, orders: list[Order]) -> None: ...

def load_orders(raw_rows: Iterable[dict], store: OrderStore) -> int:
    valid: list[Order] = []
    rejected = 0
    for row in raw_rows:
        try:
            valid.append(Order.model_validate(row))
        except ValidationError:
            rejected += 1
    store.upsert(valid)
    return rejected

class MemoryStore:
    """In-memory store for tests. A real one would run INSERT ... ON CONFLICT (order_id) DO UPDATE."""

    def __init__(self) -> None:
        self.rows: dict[int, Order] = {}

    def upsert(self, orders: list[Order]) -> None:
        for o in orders:
            self.rows[o.order_id] = o
```

با `Protocol`، کد production به رفتاری که لازم داری (`upsert`) وابسته است، نه به کتابخانه‌ی دیتابیس، و تست کردنش آسان می‌شود.

### لاگ ساخت‌یافته

لاگ داده است. حقایق را داخل فیلد بگذار تا در سیستم لاگ بتوانی با `rows_rejected > 0` فیلتر کنی:

```python
# src/pipeline/logs.py
import json
import logging

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"level": record.levelname, "logger": record.name, "msg": record.getMessage()}
        payload.update(getattr(record, "ctx", {}))
        return json.dumps(payload)

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
log = logging.getLogger("pipeline")
log.addHandler(handler)
log.setLevel(logging.INFO)

log.info("batch loaded", extra={"ctx": {"source": "orders_api", "rows": 3, "rejected": 1}})
# {"level": "INFO", "logger": "pipeline", "msg": "batch loaded", "source": "orders_api", "rows": 3, "rejected": 1}
```

شناسه‌ی batch، منبع، تعداد ورودی و خروجی، مدت اجرا و تعداد retry را لاگ کن. هرگز secret، داده‌ی شخصی کامل یا payload کامل را لاگ نکن.

### پیکربندی و secret

پیکربندی (اندازه‌ی batch، endpointها) از متغیرهای محیطی می‌آید که در هر محیط فرق دارند. secret (رمز، توکن) از محیط یا یک secret manager و در زمان اجرا تزریق می‌شود، و هرگز از فایلی داخل git. وقتی مقدار لازم نیست، زود شکست بخور:

```python
# src/pipeline/config.py
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    db_url: str
    batch_size: int

def load_settings() -> Settings:
    return Settings(
        db_url=os.environ["PIPELINE_DB_URL"],          # KeyError at startup if missing: good
        batch_size=int(os.getenv("PIPELINE_BATCH_SIZE", "500")),
    )
```

`os.environ[...]` (نه `.get`) باعث می‌شود secret گم‌شده در شروع برنامه خطا بدهد، نه سه ساعت بعد وسط اجرا.

### تست

در لایه‌ها تست کن. تست‌های واحد سریع‌اند و به سرویس نیاز ندارند. تست‌های یکپارچه از یک دیتابیس واقعی داخل کانتینر استفاده می‌کنند:

```python
# tests/test_models.py
import pytest
from pydantic import ValidationError

from pipeline.models import Order

def test_valid_order_converts_types():
    o = Order.model_validate({"order_id": "7", "customer": "ali",
                              "amount": "12.50", "created_at": "2026-01-05T10:00:00"})
    assert o.order_id == 7
    assert str(o.amount) == "12.50"

def test_negative_amount_is_rejected():
    with pytest.raises(ValidationError):
        Order.model_validate({"order_id": 1, "customer": "ali",
                              "amount": "-1", "created_at": "2026-01-05T10:00:00"})
```

```python
# tests/test_retry.py
import pytest

from pipeline.retry import retry

def test_retries_then_succeeds():
    calls = {"n": 0}
    sleeps: list[float] = []

    @retry(attempts=3, base_delay=1.0, sleep=sleeps.append)
    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("boom")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3
    assert len(sleeps) == 2          # slept between attempts, not after the last

def test_gives_up_and_reraises():
    @retry(attempts=2, sleep=lambda _: None, retry_on=(ConnectionError,))
    def always_down() -> None:
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        always_down()
```

```python
# tests/test_load.py
from pipeline.load import MemoryStore, load_orders

ROWS = [
    {"order_id": 1, "customer": "ali", "amount": "10", "created_at": "2026-01-05T10:00:00"},
    {"order_id": 2, "customer": "sara", "amount": "5", "created_at": "2026-01-05T11:00:00"},
    {"order_id": 3, "customer": "", "amount": "7", "created_at": "2026-01-05T12:00:00"},
]

def test_rerun_is_idempotent():
    store = MemoryStore()
    load_orders(ROWS, store)
    rejected = load_orders(ROWS, store)     # the same batch again
    assert len(store.rows) == 2             # no duplicates
    assert rejected == 1                    # the empty customer
```

**Mock کردن منبع.** شبکه را در مرز تابع با یک fake جایگزین کن، با `unittest.mock.patch` یا `monkeypatch` در pytest، مثلاً `monkeypatch.setattr("pipeline.source.fetch_page", lambda url: [...])`. مرزی را mock کن که مال خودت است (wrapper خودت یعنی `fetch_page`)، نه داخلیات کتابخانه.

**تست یکپارچه با Testcontainers.** وقتی خود SQL مهم است (`ON CONFLICT`، نوع‌ها، constraintها)، روی یک موتور واقعی تست کن. بسته‌ی `testcontainers` برای مدت اجرای تست‌ها یک کانتینر داکر موقت بالا می‌آورد:

```python
# tests/integration/conftest.py   (requires Docker and: uv add --dev testcontainers[postgres])
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def pg_url():
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url()
```

این تست‌ها را با `@pytest.mark.integration` علامت بزن و در CI جدا اجرا کن، چون کندترند.

### lint، تایپ و بسته‌بندی

- `ruff` باگ‌ها، importهای بی‌استفاده و مشکلات سبک را در یک ابزار سریع پیدا می‌کند (`uv run ruff check .`، `uv run ruff format .`).
- `mypy` annotationها را چک می‌کند (`uv run mypy src`). وقتی کد تایپ‌دار شد، `strict = true` را زیر `[tool.mypy]` در `pyproject.toml` فعال کن.
- lockfile (`uv.lock` یا `poetry.lock`) هر وابستگی غیرمستقیم را ثابت می‌کند، پس build دوشنبه و جمعه همان بسته‌ها را نصب می‌کند.

## 🧠 عمق و trade-offها (سنیور)

- **idempotency ویژگی کل مسیر است، نه یک تابع.** retry فقط وقتی امن است که نوشتن کلیددار باشد. upsert روی `order_id` idempotent است؛ `INSERT` با UUID تولیدشده نیست. delete-then-insert روی هر partition وقتی idempotent است که atomic باشد. الگوهای سمت warehouse را در [[dedup-idempotent-loads]] بخوان.
- **retry بار را ضرب می‌کند.** پنج تلاش با حداکثر تأخیر ۳۰ ثانیه روی ۲۰۰ worker، درست بعد از بازیابی یک دیتابیس، یک طوفان retry است. کل زمان retry را محدود کن، برای قطع‌های طولانی circuit breaker بگذار، و retry را در کوچک‌ترین واحد کار انجام بده.
- **dead-letter کن، حذف نکن.** ردیف‌های ردشده باید جایی قابل کوئری باشند (جدول یا فایل با خطا و خط منبع). فقط شمارش برای حسابرسی کافی نیست. به [[dlq-bad-records]] نگاه کن.
- **اعتبارسنجی هزینه دارد.** pydantic برای هر سطر کندتر از بررسی اسکیمای برداری روی DataFrame یا constraintهای خود دیتابیس است. برای ده‌ها میلیون سطر، به‌صورت دسته‌ای اعتبارسنجی کن (Polars یا DuckDB با اسکیمای اعلام‌شده) و مدل سطری را برای جریان‌های کوچک یا حساس نگه دار. اندازه بگیر.
- **قرارداد فراتر از کد.** یک مدل pydantic یک قرارداد محلی است. یک schema registry یا data contract مشترک باعث می‌شود تولیدکننده و مصرف‌کننده روی فیلدها و قواعد تکامل توافق کنند ([[data-quality]]).
- **بهداشت secret.** آن‌ها را بچرخان، دسترسی‌شان را به کمترین حد محدود کن، هرگز در exception چاپ نکن (یک connection string در traceback رمز را لو می‌دهد)، و چک کن لاگ‌ها و گزارش خطاها آن‌ها را پنهان کنند.
- **build تکرارپذیر.** lockfile را commit کن، نسخه‌ی پایتون را pin کن (`.python-version`)، و image کانتینر را از روی lock بساز. انحراف وابستگی بین محیط‌ها یکی از رایج‌ترین علت‌های «روی سیستم من کار می‌کند» است.
- **trade-off تست.** mock سریع است اما می‌تواند اشتباهات SQL را پنهان کند. تست کانتینری آن‌ها را می‌گیرد اما Docker در CI لازم دارد. یک تقسیم سالم: تست واحد زیاد، چند تست یکپارچه برای هر کوئری حساس، و یک اجرای end-to-end روی fixture کوچک در staging. به [[dataops-cicd]] نگاه کن.
- **چه چیزی را رصد کنی.** ردیف ورودی، خروجی، ردشده، تعداد retry هر مرحله، مدت batch، و تازگی داده (زمان از آخرین بارگذاری موفق). روی نسبت ردشده هشدار بده، نه روی هر خطای منفرد.

## 🧪 امتحان کن

پروژه‌ی بالا (یا قطعه‌کدهای درس) را با `uv` در یک پوشه‌ی تازه بساز. بعد این سه تمرین را انجام بده.

1. **تست سقف.** تستی بنویس که ثابت کند تأخیر retry هرگز از `max_delay` بیشتر نمی‌شود، حتی برای تلاش دهم. با تزریق `sleep` تأخیرها را ثبت کن.
2. **آستانه‌ی ردشده.** `load_orders` را طوری تغییر بده که وقتی بیش از ۵ درصد ردیف‌ها رد می‌شوند `RuntimeError` بدهد. تست بنویس با ۱ ردیف بد از ۱۰ (باید رد شود) و ۲ ردیف بد از ۱۰ (باید خطا بدهد).
3. **تکراری در یک batch.** upsert فعلی آخرین ردیف هر کلید را نگه می‌دارد. `MemoryStore.upsert` را طوری تغییر بده که برای کلیدهای تکراری در همان batch، ردیفی با جدیدترین `created_at` را نگه دارد. تستی اضافه کن که ردیف جدیدتر اول در لیست باشد.

<details><summary>راه‌حل‌ها را نشان بده</summary>

**۱.**
```python
def test_delay_is_capped():
    sleeps: list[float] = []

    @retry(attempts=10, base_delay=1.0, max_delay=4.0, sleep=sleeps.append)
    def always_fails() -> None:
        raise ConnectionError

    with pytest.raises(ConnectionError):
        always_fails()
    assert len(sleeps) == 9
    assert all(0 <= s <= 4.0 for s in sleeps)
```
حد بالای هر تأخیر با jitter برابر `min(4.0, 2**(n-1))` است، پس هیچ sleep نمی‌تواند از ۴ ثانیه بیشتر شود.

**۲.**
```python
def load_orders(raw_rows, store):   # same as before, plus a guard before upsert
    rows = list(raw_rows)
    valid, rejected = [], 0
    for row in rows:
        try:
            valid.append(Order.model_validate(row))
        except ValidationError:
            rejected += 1
    if rows and rejected / len(rows) > 0.05:
        raise RuntimeError(f"too many rejected rows: {rejected}/{len(rows)}")
    store.upsert(valid)
    return rejected

def test_threshold():
    good = {"order_id": 1, "customer": "a", "amount": "1", "created_at": "2026-01-01T00:00:00"}
    bad = {"order_id": 0, "customer": "a", "amount": "1", "created_at": "2026-01-01T00:00:00"}
    assert load_orders([good] * 19 + [bad], MemoryStore()) == 1   # 1/20 = 5 %, not above 5 %
    with pytest.raises(RuntimeError):
        load_orders([good] * 8 + [bad, bad], MemoryStore())       # 2/10 = 20 %
```
محافظ باید قبل از upsert اجرا شود، تا یک batch بد هرگز به store نرسد. توجه کن که ۱ ردیف بد از ۱۰ (۱۰ درصد) با این آستانه درست خطا می‌دهد.

**۳.**
```python
class MemoryStore:
    def __init__(self) -> None:
        self.rows: dict[int, Order] = {}

    def upsert(self, orders: list[Order]) -> None:
        for o in orders:
            current = self.rows.get(o.order_id)
            if current is None or o.created_at >= current.created_at:
                self.rows[o.order_id] = o

def test_latest_wins_regardless_of_order():
    store = MemoryStore()
    old = Order.model_validate({"order_id": 1, "customer": "a", "amount": "1", "created_at": "2026-01-01T00:00:00"})
    new = Order.model_validate({"order_id": 1, "customer": "a", "amount": "2", "created_at": "2026-02-01T00:00:00"})
    store.upsert([new, old])
    assert store.rows[1].amount == 2
```
مقایسه بر اساس زمان رویداد است، پس نتیجه به ترتیب رسیدن بستگی ندارد. همین قاعده با `ROW_NUMBER` یا `MERGE` در SQL، نسخه‌ی warehouse این تمرین است.

</details>

## 💬 گوشه‌ی مصاحبه

**سؤال: «چطور یک پایپ‌لاین را امن برای اجرای دوباره می‌کنی؟»**
جواب متوسط: «نوشتن‌ها را با upsert روی یک کلید کسب‌وکاری idempotent می‌کنم، و retry را فقط برای خطاهای گذرا استفاده می‌کنم.»
جواب سنیور: «هر نوشتن کلیددار است، پس اجرای دوباره یا retry به همان وضعیت می‌رسد. هر مرحله در سطح partition atomic است، پس یک batch نیمه‌نوشته هرگز دیده نمی‌شود. خطاهای گذرا را با backoff نمایی محدود و jitter دوباره امتحان می‌کنم، و ردیف‌های ردشده را با دلیلشان به یک dead-letter store می‌فرستم. این را با یک تست ثابت می‌کنم: همان batch را دو بار اجرا می‌کند و تعداد سطرها و checksum را مقایسه می‌کند.»

**سؤال: «در یک پایپ‌لاین داده چه چیزی را تست می‌کنی؟»**
متوسط: «تست واحد برای تبدیل‌ها و اسکیماها، و تست یکپارچه برای کوئری دیتابیس.»
سنیور: «تست‌های واحد سریع برای تبدیل و اعتبارسنجی با مرزهای fake، تست‌های یکپارچه‌ی کانتینری برای SQLی که معنا را حمل می‌کند (upsertها، constraintها، نوع‌ها)، و یک اجرای کوچک end-to-end روی داده‌ی ثابت در CI. idempotency و آستانه‌ی ردشده را هم تست می‌کنم، چون آن‌ها چیزهایی هستند که ساعت ۳ صبح خراب می‌شوند.»

> **🎤 این را در مصاحبه بگو:** «پایپ‌لاین را مثل نرم‌افزار می‌بینم: ورودی اعتبارسنجی‌شده، نوشتن کلیددار idempotent، retry فقط برای خطاهای گذرا، لاگ ساخت‌یافته با شمارش، تنظیمات از محیط، و تست در سه سطح. هدف این است که اجرای دوباره کسل‌کننده باشد.»

## ⚠️ اشتباهات رایج

> **⚠️ هشدار:** همه‌چیز را retry کردن. یک `ValidationError` یا 400 برای همیشه شکست می‌خورد؛ retry فقط هشدار را دیر می‌کند و بار را ضرب می‌کند.

> **⚠️ هشدار:** `except Exception: pass` دور یک مرحله‌ی load. اجرا «موفق» می‌شود با داده‌ی گم‌شده، و تا وقتی گزارش غلط شود کسی نمی‌فهمد.

- استفاده از `INSERT` ساده در مرحله‌ای که retry می‌شود، که هر retry تکراری می‌سازد.
- نوشتن رمز یا connection string داخل کد یا یک فایل `.env` که commit شده.
- استفاده از `os.getenv("DB_PASSWORD")` بدون بررسی مقدار پیش‌فرض، پس job با `None` شروع می‌شود و بعداً با خطای گیج‌کننده شکست می‌خورد.
- لاگ کردن کل سطر، از جمله داده‌ی شخصی، در پیام خطا.
- اجرای تست‌ها فقط روی لپ‌تاپ با دیتابیس محلی، که یک migration جاافتاده را پنهان می‌کند.
- نداشتن lockfile، پس انتشار یک کتابخانه‌ی جدید رفتار را یک‌شبه عوض می‌کند.

## ✅ خودت را بسنج

۱. کدام خطا را باید با backoff نمایی دوباره امتحان کرد؟
   - A) `pydantic.ValidationError` روی یک ردیف خراب
   - B) HTTP 400 از یک کوئری بد
   - C) timeout یا HTTP 503 از یک API که موقتاً شلوغ است
   - D) یک `KeyError` برای متغیر محیطی گم‌شده

<details><summary>جواب</summary>

**C** — timeout و 503 گذرا هستند؛ retry می‌تواند موفق شود. خطای اعتبارسنجی، 400 و خطای پیکربندی دائمی‌اند و هر بار شکست می‌خورند، پس retry فقط وقت تلف می‌کند.

</details>

۲. یک مرحله ردیف‌ها را با UUID تولیدشده درج می‌کند و بعد از timeout دوباره اجرا می‌شود. مشکل چیست و راه‌حل کدام است؟
   - A) UUID کند است؛ از عدد صحیح استفاده کن
   - B) retry بعد از timeout که درج قبلی را واقعاً commit کرده، تکراری می‌سازد؛ از یک کلید کسب‌وکاری و upsert (`ON CONFLICT ... DO UPDATE`) استفاده کن تا اجرای دوباره به یک نتیجه برسد
   - C) برای همه‌ی درج‌ها باید retry غیرفعال شود
   - D) مشکل سطح لاگ است

<details><summary>جواب</summary>

**B** — ممکن است تلاش اول قبل از رسیدن timeout به کلاینت، commit شده باشد. UUID جدید باعث می‌شود نوشتن دوم یک ردیف متفاوت باشد. کلید کسب‌وکاری قطعی با upsert عملیات را idempotent می‌کند.

</details>

۳. چرا `os.environ["PIPELINE_DB_PASSWORD"]` برای یک پایپ‌لاین production بهتر از `os.getenv("PIPELINE_DB_PASSWORD")` است؟
   - A) سریع‌تر است
   - B) وقتی secret گم شده، در شروع برنامه `KeyError` می‌دهد، به‌جای اینکه بعداً با `None` شکست بخورد
   - C) رمز را رمزنگاری می‌کند
   - D) `getenv` نمی‌تواند متغیرهای محیطی را بخواند

<details><summary>جواب</summary>

**B** — هدف شکست سریع در شروع است. یک رمز `None` می‌تواند از چند مرحله رد شود و خطای گیج‌کننده ساعت‌ها بعد بدهد.

</details>

۴. **(سنیور)** job شما یک API را با `attempts=5` و بدون jitter دوباره امتحان می‌کند، روی ۲۰۰ worker موازی. API بعد از یک قطعی کوتاه برمی‌گردد و بلافاصله دوباره می‌افتد. چه شده و چه چیزی را عوض می‌کنی؟

   - A) API به worker بیشتری نیاز دارد
   - B) همه‌ی workerها در موج‌های همگام retry می‌کنند و یک طوفان retry ایجاد می‌شود؛ jitter تصادفی اضافه کن، سقف تأخیر و بودجه‌ی کل retry را محدود کن، و یک circuit breaker در نظر بگیر
   - C) retry هرگز نباید با workerهای موازی استفاده شود
   - D) راه‌حل غیرفعال کردن لاگ است

<details><summary>جواب</summary>

**B** — بدون jitter، retryهای همه‌ی workerها هم‌راستا می‌شوند و در یک لحظه به سرویس در حال بازیابی می‌خورند. full jitter بار را پخش می‌کند، سقف آسیب را محدود می‌کند، و circuit breaker وقتی سرویس واضحاً خاموش است فراخوانی‌ها را متوقف می‌کند.

</details>

۵. **(سنیور)** یک تیم می‌گوید تست‌های واحد با دیتابیس mock‌شده کافی‌اند. قوی‌ترین استدلال مخالف چیست، و مجموعه‌ی تست را چطور ساختار می‌دهی؟

   - A) mockها همیشه از دیتابیس واقعی کندترند
   - B) mockها نمی‌توانند معنای SQL مثل `ON CONFLICT`، تبدیل نوع و constraintها را تأیید کنند، پس یک تست mock‌شده می‌تواند پاس شود در حالی که کوئری واقعی شکست می‌خورد؛ تست واحد سریع زیاد نگه دار، به‌علاوه تست یکپارچه‌ی کانتینری برای کوئری‌هایی که منطق را حمل می‌کنند، و هر دو را در CI اجرا کن
   - C) mock در pytest مجاز نیست
   - D) تست‌های یکپارچه باید در production اجرا شوند

<details><summary>جواب</summary>

**B** — معنای SQL در موتور دیتابیس است که mock اجرایش نمی‌کند. از mock برای منطق خالص و مرزهایی که مال خودت است استفاده کن، و از موتور واقعی برای SQL مهم، با یک استراتژی تست لایه‌ای.

</details>

## 🗂️ فلش‌کارت

| جلوی کارت | پشت کارت |
|---|---|
| کدام خطاها را retry کنیم؟ | خطاهای گذرا: timeout، قطع اتصال، 429، 503. نه خطای اعتبارسنجی یا 400 |
| چرا jitter؟ | تأخیر تصادفی جلوی retry هم‌زمان ده‌ها worker را می‌گیرد |
| load idempotent یعنی چه؟ | اجرای دوباره‌ی یک batch همان وضعیت نهایی را باقی می‌گذارد |
| upsert روی کلید کسب‌وکاری | درج یا به‌روزرسانی بر اساس کلید (`ON CONFLICT ... DO UPDATE`)، تا retryها به یک نتیجه برسند |
| چرا برای secretهای لازم `os.environ[...]`؟ | در شروع برنامه وقتی مقدار نیست شکست می‌خورد |
| lockfile برای چیست؟ | همه‌ی وابستگی‌های غیرمستقیم را ثابت می‌کند تا build تکرارپذیر باشد |
| Testcontainers چه می‌دهد؟ | یک دیتابیس واقعی در کانتینر داکر موقت برای تست‌های یکپارچه |
| چرا ساختار `src/`؟ | تست‌ها بسته‌ی نصب‌شده را import می‌کنند، که خطاهای بسته‌بندی را زود می‌گیرد |

## 🔗 ادامه

- **python-concurrency** انتخاب‌های thread و async را توضیح می‌دهد که retry و load موازی روی آن‌ها سوار هستند.
- **python-dataframes** تبدیل‌های برداری را پوشش می‌دهد که معمولاً بین اعتبارسنجی و load قرار می‌گیرند.
- **dedup-idempotent-loads** الگوهای سمت warehouse برای نوشتن idempotent در مقیاس بزرگ را نشان می‌دهد.
- **dlq-bad-records** توضیح می‌دهد چطور ردیف‌های ردشده را ذخیره کنی تا بشود اصلاح و دوباره اجرا کرد.
- **dataops-cicd** و **docker** نشان می‌دهند این تست‌ها و image ها چطور در continuous integration و delivery اجرا می‌شوند.
- مستندات `pydantic`، `uv` و `ruff` را برای نسخه‌های نصب‌شده‌ات چک کن؛ پرچم‌ها و کلیدهای تنظیمات بین نسخه‌ها عوض می‌شوند.
