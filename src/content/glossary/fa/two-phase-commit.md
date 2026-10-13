---
term: "کامیت دوفازی (Two-phase commit)"
term_en: "Two-phase commit (2PC)"
aliases: ["2pc", "two-phase commit", "کامیت دوفازی", "تراکنش توزیع‌شده"]
category: distributed
short: "پروتکلی که در آن coordinator اول از همه‌ی شرکت‌کنندگان آمادگی می‌خواهد و سپس دستور commit یا abort می‌دهد تا تراکنش توزیع‌شده اتمیک باشد."
related: ["saga", "exactly-once", "outbox-pattern", "quorum"]
---
## به زبان ساده (مبتدی)
چند دوست باید توافق کنند یک میز رستوران برای همه رزرو شود. اول مسئول می‌پرسد: «همه می‌توانید بیایید؟» همه قول می‌دهند آن روز را آزاد نگه دارند. فقط وقتی همه بله گفتند مسئول می‌گوید «رزرو شد!»؛ وگرنه برنامه برای همه لغو می‌شود. two-phase commit همین را برای دیتابیس‌ها انجام می‌دهد تا یا همه تغییر را اعمال کنند یا هیچ‌کدام.

## نحوه‌ی کار (متوسط)
فاز ۱ (prepare): هر شرکت‌کننده سهم خود را پایدار و آماده‌ی commit می‌کند، قفل نگه می‌دارد و رأی می‌دهد. فاز ۲: اگر همه بله دادند coordinator تصمیم را پایدار ثبت و به همه دستور commit می‌دهد؛ هر «نه» یا timeout به abort می‌انجامد. پس از رأی بله، شرکت‌کننده نمی‌تواند یک‌جانبه نظرش را عوض کند و باید منتظر تصمیم بماند.

```
Phase 1 (prepare / vote):
  coordinator -> all participants: "PREPARE"
  each participant: durably log the changes, lock resources, reply YES or NO

Phase 2 (decide):
  if all YES: coordinator logs COMMIT, sends COMMIT to all
  else:       coordinator logs ABORT,  sends ABORT  to all

Danger: coordinator crashes after YES votes -> participants are blocked, holding locks
```

اتمیک بودن را بین سیستم‌ها می‌دهد (تراکنش XA بین دیتابیس‌ها، تراکنش Kafka بین پارتیشن‌ها و offsetها، sinkهای two-phase-commit در Flink گره‌خورده به checkpoint، Spanner). در sinkهای exactly-once در Flink، pre-commit روی barrier checkpoint و commit هنگام تکمیل checkpoint انجام می‌شود.

## نگاه سنیور و نکات مصاحبه
ضعف آن blocking است: اگر coordinator بین دو فاز خراب شود شرکت‌کننده‌هایی که قفل دارند نمی‌توانند خودشان تصمیم بگیرند و دسترس‌پذیری آسیب می‌بیند؛ بازیابی به لاگ پایدار coordinator نیاز دارد. 2PC همچنین latency (رفت‌وبرگشت اضافه)، رقابت قفل و اتصال سفت اضافه می‌کند و بین سرویس‌ها مقیاس بدی دارد. جایگزین‌ها: saga با جبران (سازگاری نهایی)، الگوی outbox، consumer خودتوان، یا coordinator replicate‌شده با consensus (Spanner از گروه‌های Paxos به‌عنوان شرکت‌کننده برای پرهیز از نقطه‌ی شکست blocking استفاده می‌کند). 3PC هست ولی به‌ندرت استفاده می‌شود.

سؤال مصاحبه: «2PC در برابر saga؟» پاسخ: 2PC commit اتمیک و ایزوله می‌دهد ولی با خرابی coordinator بلاک می‌شود و قفل نگه می‌دارد و برای چند دیتابیس جفت‌شده مناسب است؛ saga تراکنش‌های محلی با اقدام جبرانی اجرا می‌کند، در دسترس و مقیاس‌پذیر است ولی فقط eventually consistent و جبران‌های خودتوان لازم دارد.
