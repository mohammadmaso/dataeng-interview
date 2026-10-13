---
term: "اجماع (Consensus: Raft/Paxos)"
term_en: "Consensus (Raft / Paxos)"
aliases: ["raft", "paxos", "consensus", "اجماع", "انتخاب leader"]
category: distributed
short: "پروتکل‌هایی که کلاستر را با وجود خرابی نودها روی دنباله‌ی مرتب واحدی از تصمیم‌ها همنظر می‌کنند، با leader منتخب و quorum اکثریت."
related: ["quorum", "replication", "cap-theorem"]
---
## به زبان ساده (مبتدی)
گروهی از دوستان باید در چت یک رستوران برای امشب انتخاب کنند، حتی اگر بعضی‌ها قطع و وصل شوند. یک هماهنگ‌کننده انتخاب می‌کنند که گزینه‌ای پیشنهاد می‌دهد؛ وقتی اکثریت موافق باشند آن انتخاب نهایی است و هماهنگ‌کننده بعداً نباید خلافش عمل کند. الگوریتم‌های consensus این کار را برای کامپیوترها به‌شکل قابل‌اعتماد صورت‌بندی می‌کنند.

## نحوه‌ی کار (متوسط)
consensus به replicaها اجازه می‌دهد روی یک مقدار یا لاگ مرتب (replicated state machine) توافق کنند تا همه‌ی نودها فرمان‌ها را به همان ترتیب اعمال کنند. Raft آن را به انتخاب leader، تکثیر لاگ و safety تقسیم می‌کند. leader نوشتن را می‌پذیرد و ورودی‌های لاگ را تکثیر می‌کند؛ ورودی وقتی روی اکثریت ذخیره شد commit می‌شود؛ انتخاب جدید رأی اکثریت می‌خواهد و leader برگزیده تضمین دارد همه‌ی ورودی‌های commit‌شده را دارد.

```
Raft (simplified):
  roles: follower -> candidate -> leader     (term numbers increase on each election)
  election: follower times out, requests votes; majority wins, becomes leader
  replication: client -> leader appends to its log -> sends AppendEntries to followers
  commit: once a MAJORITY has the entry, leader marks it committed and applies it
  safety: a candidate with a less up-to-date log cannot win
Used by: etcd, Consul, Kafka KRaft, CockroachDB, TiKV | Paxos/ZAB: Spanner, ZooKeeper
```

consensus با `2f+1` نود `f` خرابی crash را تحمل می‌کند و نیاز دارد quorum اکثریت در دسترس باشد؛ پس اقلیتِ جداافتاده پیشرفت نمی‌کند (رفتار CP). پایه‌ی storeهای متادیتا و هماهنگی است: etcd، ZooKeeper، کنترلر KRaft در Kafka، گروه‌های Raft به‌ازای هر shard در CockroachDB یا TiKV.

## نگاه سنیور و نکات مصاحبه
consensus گران است و برای مسیر داده‌ی حجیم ساخته نشده: نوشتن از یک leader می‌گذرد و رفت‌وبرگشت اکثریت می‌خواهد؛ پس برای متادیتا، قفل، انتخاب leader و پیکربندی به کار می‌رود و برای داده از replication دیگر (ISR، quorum) استفاده می‌شود. حالت‌های خرابی: تغییر مکرر leader با شبکه‌ی ناپایدار یا GC pause (timeout انتخاب را تنظیم کنید)، دیسک کند (fsync) روی اکثریت که commit را متوقف می‌کند، و split brain اگر پروتکل را دور بزنید (برای منابع بیرونی fencing token لازم است). خطاهای Byzantine پوشش داده نمی‌شوند (پروتکل BFT می‌خواهد).

سؤال مصاحبه: «چرا ZooKeeper/etcd تعداد نود فرد می‌خواهد؟» پاسخ: پیشرفت به اکثریت نیاز دارد؛ با ۳ نود یک خرابی و با ۴ نود هم یک خرابی تحمل می‌شود پس اندازه‌ی زوج هزینه می‌آورد بدون تحمل بیشتر؛ و در تقسیم شبکه حداکثر یک طرف اکثریت دارد.
