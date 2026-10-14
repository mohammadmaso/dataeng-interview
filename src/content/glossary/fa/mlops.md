---
term: "MLOps"
term_en: "MLOps"
aliases: ["mlops","عملیات یادگیری ماشین"]
category: ml
short: "روش‌هایی برای ساخت، استقرار، پایش و بازآموزی قابل‌اعتماد مدل‌های ML."
related: ["feature-store","training-serving-skew","data-drift"]
---
## به زبان ساده (مبتدی)
DevOps برای یادگیری ماشین: مطمئن شدن که مدلی که در notebook کار می‌کند در production هم کار کند.

## نحوه‌ی کار (متوسط)
شامل pipelineهای داده و feature، ردیابی آزمایش، model registry و نسخه‌بندی، CI/CD برای آموزش و serving، الگوهای استقرار (batch، online، shadow، canary) و پایش drift و کارایی.

## نگاه سنیور و نکات مصاحبه
بیشتر شکست‌های ML مشکل داده‌اند: skew، featureهای کهنه، نشت label. تکرارپذیری به داده، کد و config نسخه‌بندی‌شده نیاز دارد. مهندسان داده بخش بزرگی را در اختیار دارند: pipelineهای feature، مجموعه‌های آموزش point-in-time-correct و SLA.

سؤال مصاحبه: «point-in-time correctness چیست؟» پاسخ: featureهای آموزش باید فقط از داده‌ای محاسبه شوند که در زمان هر label در دسترس بود تا نشت از آینده نشود.
