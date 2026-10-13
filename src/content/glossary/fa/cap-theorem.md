---
term: "قضیه‌ی CAP"
term_en: "CAP theorem"
aliases: ["cap", "قضیه cap", "pacelc"]
category: distributed
short: "در زمان network partition سیستم replicate‌شده باید بین سازگاری (linearizability) و دسترس‌پذیری یکی را انتخاب کند؛ هر دو ممکن نیست."
related: ["quorum", "replication", "sharding"]
---
## به زبان ساده (مبتدی)
دو شعبه‌ی یک بانک خط تلفن بین خودشان را از دست می‌دهند. مشتری از یک شعبه موجودی می‌پرسد. شعبه می‌تواند تا تأیید از شعبه‌ی دیگر جواب ندهد (سازگار ولی در دسترس نیست) یا با آنچه می‌داند جواب بدهد که شاید کهنه باشد (در دسترس ولی احتمالاً ناسازگار). CAP می‌گوید در چنین شکافی هر دو را با هم نمی‌توان داشت.

## نحوه‌ی کار (متوسط)
بیان رسمی: در شبکه‌ی ناهمگام با partition هیچ سیستمی نمی‌تواند هم سازگاری linearizable (هر خواندن آخرین نوشتن را ببیند، مثل یک نسخه) و هم دسترس‌پذیری (هر نود سالم به هر درخواست جواب دهد) بدهد. تحمل partition در سیستم‌های توزیع‌شده اختیاری نیست، پس «دو از سه» گمراه‌کننده است: انتخاب بین C و A فقط وقتی partition وجود دارد معنا دارد.

```
Partitions WILL happen, so the real choice is during a partition:

CP: refuse/lose availability on the minority side to stay consistent
    (etcd, ZooKeeper, HBase, Spanner-like, Kafka with min.insync)
AP: keep answering on both sides, reconcile later -> stale/conflicting reads
    (Cassandra at low consistency, Dynamo-style, DNS)

PACELC: if Partition -> A or C; Else (normal) -> Latency or Consistency
```

توجه کنید C اینجا linearizability است نه C در ACID. بسیاری از سیستم‌ها تنظیم‌پذیرند (سطوح consistency در Cassandra). PACELC، CAP را گسترش می‌دهد: حتی بدون partition بین latency و سازگاری مصالحه هست چون replication همگام رفت‌وبرگشت شبکه هزینه دارد.

## نگاه سنیور و نکات مصاحبه
اشتباه‌های رایج مصاحبه: نامیدن دیتابیس تک‌نود «CA» و دیدن CAP به‌عنوان برچسب ثابت محصول. سیستم‌های واقعی برای هر عملیات و تنظیم روی یک طیف‌اند. به این فکر کنید که سمت اقلیت partition چه می‌کند، چه میزان کهنگی قابل‌قبول است (eventual، read-your-writes، monotonic reads) و تعارض‌ها چطور حل می‌شوند (LWW، CRDT، vector clock، merge در برنامه). در مهندسی داده: ذخیره‌ی متادیتا، قفل orchestration یا انتخاب leader CP می‌خواهند؛ مسیر ingestion متریک یا cache ممکن است AP را ترجیح دهد.

سؤال مصاحبه: «MongoDB / Cassandra از نوع CP است یا AP؟» پاسخ: به تنظیمات بستگی دارد؛ با نوشتن و خواندن majority تقریباً CP است (سمت اقلیت رد می‌کند)، با سطح consistency پایین دسترس‌پذیری را ترجیح می‌دهد و ممکن است داده‌ی کهنه بدهد؛ پس رفتار در partition را توضیح می‌دهم نه برچسب.
