---
term: "تکثیر داده (Replication)"
term_en: "Replication"
aliases: ["replication", "replica", "تکثیر", "ریپلیکا", "replicate"]
category: distributed
short: "نگه‌داشتن نسخه‌هایی از داده روی چند نود برای تحمل خطا و مقیاس خواندن، به‌صورت همگام یا ناهمگام."
related: ["quorum", "cap-theorem", "sharding", "wal"]
---
## به زبان ساده (مبتدی)
نگه‌داشتن نسخه‌ی یدک یک سند مهم در ساختمانی دیگر یعنی آتش‌سوزی در یک جا آن را نابود نمی‌کند و دو نفر می‌توانند هم‌زمان آن را بخوانند. replication داده را روی چند ماشین کپی می‌کند تا سیستم از خرابی جان سالم به در ببرد و خواننده‌های بیشتری سرویس بگیرند.

## نحوه‌ی کار (متوسط)
رایج‌ترین توپولوژی single-leader است: نوشتن‌ها به leader می‌روند و لاگ خود (WAL/binlog) را برای followerها استریم می‌کند و آن‌ها به ترتیب اعمال می‌کنند. followerها خواندن را سرو می‌کنند و در خرابی جانشین می‌شوند. replication همگام منتظر ack یک follower می‌ماند (دوام، latency بیشتر)؛ ناهمگام فوراً برمی‌گردد (سریع، ولی replication lag و احتمال از دست رفتن commitهای اخیر در failover)؛ semi-sync دست‌کم یکی را صبر می‌کند.

```
Single-leader:   clients write -> LEADER --(log stream)--> FOLLOWER 1, FOLLOWER 2
                 reads: leader (fresh) or followers (maybe stale)
Multi-leader:    each region has a leader; conflicts need resolution (LWW, CRDT)
Leaderless:      write to N nodes, succeed with W acks; read R nodes (quorum)

sync:  commit waits for replica ack  -> no loss on failover, higher latency
async: commit returns immediately    -> replication lag, may lose last writes
```

گزینه‌های دیگر: multi-leader (نوشتن چندمنطقه‌ای، نیازمند حل تعارض) و leaderless با quorum. Kafka هر پارتیشن را با leader و مجموعه‌ی replicaهای همگام (ISR) تکثیر می‌کند؛ HDFS بلوک‌ها را تکثیر می‌کند؛ ClickHouse از ReplicatedMergeTree با هماهنگی Keeper استفاده می‌کند.

## نگاه سنیور و نکات مصاحبه
مشکلاتی که باید بشناسید: replication lag خواندن کهنه می‌دهد (نقض read-your-writes؛ با خواندن از leader، sticky read یا مسیریابی آگاه از lag حل می‌شود)، split brain بعد از failover بدون fencing، از دست رفتن داده در failover ناهمگام و واگرایی replicaها. failover به تشخیص خرابی مطمئن و انتخاب leader (consensus) و fencing leader قبلی نیاز دارد. replication جایگزین backup نیست: یک `DELETE` اشتباه یا خرابی فوراً تکثیر می‌شود. replication دسترس‌پذیری و مقیاس خواندن می‌دهد، sharding مقیاس نوشتن و ذخیره.

سؤال مصاحبه: «بلافاصله پس از نوشتن، خواندن از replica داده‌ی قدیمی می‌دهد؛ چرا و چه می‌کنی؟» پاسخ: replication lag ناهمگام است؛ خواندن کاربر را برای مدتی کوتاه یا تا رسیدن موقعیت replica به موقعیت لاگ آن نوشتن به leader می‌فرستم، یا برای تازگی سخت از خواندن همگام/quorum استفاده می‌کنم.
