---
term: "Orchestration (هماهنگ‌سازی workflow)"
term_en: "Workflow orchestration"
aliases: ["orchestration","orchestrator","airflow","scheduler","ارکستراسیون"]
category: orchestration
short: "زمان‌بندی و هماهنگی taskها و وابستگی‌هایشان همراه retry و پایش."
related: ["dag","data-pipeline","backfill","retry"]
---
## به زبان ساده (مبتدی)
رهبر ارکستری که به هر نوازنده می‌گوید کی شروع کند تا کل قطعه به ترتیب اجرا شود و اگر کسی از قلم افتاد متوجه شود.

## نحوه‌ی کار (متوسط)
orchestratorها (Airflow، Dagster، Prefect) taskها را به‌صورت DAG تعریف می‌کنند، طبق زمان‌بندی یا trigger اجرا می‌کنند، وابستگی‌ها را اعمال می‌کنند، شکست‌ها را retry می‌کنند، پارامتر می‌دهند، وضعیت و لاگ نشان می‌دهند و backfill را پشتیبانی می‌کنند. کار هماهنگی را می‌کنند؛ محاسبه‌ی سنگین باید در Spark، warehouse یا کانتینر اجرا شود نه داخل orchestrator.

## نگاه سنیور و نکات مصاحبه
taskها را idempotent و پارامتری بر اساس logical date نگه دارید، کار سنگین را در پروسه‌ی scheduler نبرید، برای triggerهای رویدادمحور از sensor یا dataset استفاده کنید، SLA و هشدار بگذارید و concurrency/pool را محدود کنید. orchestration مبتنی بر asset (Dagster) به‌جای فقط task، خروجی‌های داده را مدل می‌کند.

سؤال مصاحبه: «orchestration مبتنی بر task در برابر asset؟» پاسخ: task-محور گام‌ها را زمان‌بندی می‌کند؛ asset-محور محصولات داده را اعلام می‌کند و آنچه باید اجرا شود را استخراج می‌کند و lineage بهتری می‌دهد.
