---
term: "حد نصاب (Quorum)"
term_en: "Quorum"
aliases: ["quorum", "کوروم", "حد نصاب"]
category: distributed
short: "حداقل تعداد replicaهایی که باید روی خواندن یا نوشتن موافق باشند؛ quorum اکثریت تضمین می‌کند هر دو quorum اشتراک دارند."
related: ["replication", "cap-theorem", "two-phase-commit"]
---
## به زبان ساده (مبتدی)
کمیته‌ای با رأی اکثریت تصمیم می‌گیرد. اگر ۳ نفر از ۵ نفر موافق باشند تصمیم نهایی است، حتی اگر دو نفر غایب باشند. هر دو اکثریت دست‌کم یک عضو مشترک دارند، پس دو گروه نمی‌توانند تصمیم‌های متناقض بگیرند. quorum همین قاعده برای داده‌ی replicate‌شده است.

## نحوه‌ی کار (متوسط)
با `N` replica، quorum معمولاً اکثریت یعنی `floor(N/2)+1` است. در سیستم‌های بدون leader (Dynamo، Cassandra) مقدار `W` و `R` را تنظیم می‌کنید؛ شرط `R + W > N` تضمین می‌کند مجموعه‌ی خواندن با مجموعه‌ی آخرین نوشتن اشتراک دارد، پس دست‌کم یک replica جدیدترین مقدار را دارد (با timestamp یا version vector تعیین می‌شود). در سیستم‌های consensus (Raft، Paxos، ZooKeeper/ZAB، KRaft در Kafka) leader پیش از commit به ack اکثریت نیاز دارد و انتخاب leader جدید هم رأی اکثریت می‌خواهد.

```
N = replicas, W = write acks required, R = replicas read
Rule:  R + W > N   -> read set overlaps the latest write set

N=3, W=2, R=2  -> tolerates 1 node down, reads see latest committed write
N=3, W=3, R=1  -> fast reads, writes fail if any node is down
N=3, W=1, R=1  -> fast, but can read stale data (eventual consistency)

Cassandra: CONSISTENCY QUORUM = floor(N/2)+1
```

quorum اکثریت با `2f+1` نود `f` خرابی را تحمل می‌کند: ۳ نود یک و ۵ نود دو خرابی. به همین دلیل اندازه‌ی کلاسترها فرد است؛ افزودن نود چهارم به سه نود هزینه می‌آورد ولی تحمل خطای بیشتر نه.

## نگاه سنیور و نکات مصاحبه
انتخاب quorum دسترس‌پذیری و latency را با سازگاری معاوضه می‌کند. فقط اشتراک مجموعه‌ها linearizability نیست: sloppy quorum با hinted handoff، نوشتن‌های هم‌زمان، اختلاف ساعت و نوشتن‌های ناقص شکست‌خورده هنوز می‌توانند داده‌ی کهنه یا متعارض نشان دهند؛ پس خواندن quorum به read repair و anti-entropy نیاز دارد. در network partition سمت اقلیت quorum ندارد و نوشتن را متوقف می‌کند (رفتار CP). `acks=all` با `min.insync.replicas` در Kafka گونه‌ی مبتنی بر ISR است: نوشتن به تعداد کافی replica همگام نیاز دارد نه اکثریت خالص.

سؤال مصاحبه: «چرا ZooKeeper یا etcd ۳ یا ۵ نود دارد؟» پاسخ: commit به quorum اکثریت نیاز دارد؛ ۳ نود یک خرابی و ۵ نود دو خرابی را تحمل می‌کند؛ اندازه‌ی زوج هزینه می‌آورد ولی تحمل بیشتر نه و کلاستر بزرگ‌تر نوشتن را کند می‌کند.
