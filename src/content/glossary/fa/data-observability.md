---
term: "مشاهده‌پذیری داده (Data observability)"
term_en: "Data observability"
aliases: ["data observability", "مشاهده‌پذیری داده", "پایش داده", "monte carlo"]
category: quality
short: "پایش مستمر سلامت داده و pipelineها (تازگی، حجم، schema، توزیع، lineage) برای تشخیص و عیب‌یابی زودهنگام مشکلات."
related: ["data-quality", "lineage", "sla-slo"]
---
## به زبان ساده (مبتدی)
داشبورد خودرو سرعت، بنزین و هشدار موتور را نشان می‌دهد تا پیش از خراب‌شدن متوجه مشکل شوید. data observability داشبورد داده است: همان لحظه‌ای که جدول دیر شده، حجمش ناگهان نصف شده یا ستونی تغییر کرده خبر می‌دهد، قبل از اینکه ذی‌نفعی عدد غلط را ببیند.

## نحوه‌ی کار (متوسط)
تست‌ها (کیفیت داده) قواعد شناخته‌شده را چک می‌کنند؛ observability رفتار سیستم را می‌پاید تا مشکلات ناشناخته را بگیرد و عیب‌یابی را کوتاه کند. سیگنال‌ها: وضعیت و مدت اجرای pipeline، تازگی جدول، تعداد سطر، تغییر schema، نرخ null/تکراری، جابه‌جایی توزیع مقادیر، متریک کوئری و هزینه و lineage برای تحلیل اثر. با جمع‌آوری متادیتا/متریک (information schema انبار، لاگ کوئری، رویدادهای OpenLineage)، baseline سری زمانی با تشخیص ناهنجاری و هشدار متصل به ابزار on-call پیاده می‌شود.

```
Five pillars (commonly cited):
  freshness     - did the table update when expected?        (max(updated_at) vs now)
  volume        - is the row count within the usual range?   (z-score vs last 4 weeks)
  schema        - did columns/types change?
  distribution  - null rate, uniqueness, value ranges drift?
  lineage       - what upstream caused it, what downstream is affected?

-- freshness check
SELECT now() - max(loaded_at) AS staleness FROM mart.orders;   -- alert if > SLO
```

ابزارها شامل stackهای متن‌باز (artifactهای dbt، Great Expectations/Soda، Elementary، OpenLineage با Marquez/DataHub، Prometheus/Grafana برای متریک pipeline) و پلتفرم‌های تجاری است. هدف کاهش mean-time-to-detect و mean-time-to-resolve است.

## نگاه سنیور و نکات مصاحبه
کیفیت هشدار مهم است: هشدار ناهنجاری پرسروصدا نادیده گرفته می‌شود؛ پس فصلی بودن (الگوی روزهای هفته) را تنظیم کنید، جدول‌ها را بر اساس اهمیت لایه‌بندی کنید، هشدار را به مالک بفرستید و SLO به‌ازای هر data product تعریف کنید (تازگی ۳۰ دقیقه، کامل‌بودن ۹۹٪). observability را با lineage برای رفتن سریع از نشانه به ریشه و با runbook برای خرابی‌های رایج ترکیب کنید. سلامت pipeline (job اجرا شد) را از سلامت داده (خروجی درست است) جدا کنید: DAG سبز می‌تواند داده‌ی غلط را پنهان کند. observability هزینه (کوئری‌های گران، رشد ذخیره‌سازی) هم بیشتر جزو آن می‌شود.

سؤال مصاحبه: «داشبوردی یک هفته غلط بوده و کسی نفهمیده؛ چطور جلوگیری می‌کنی؟» پاسخ: مانیتور تازگی، حجم و توزیع با مالک و شدت روی جدول‌های حیاتی، تست در joinهای کلیدی، lineage برای یافتن سریع ریشه و SLO تا تشخیص قبل از دیدن مصرف‌کننده رخ دهد.
