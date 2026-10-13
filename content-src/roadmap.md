---
title: "Data Engineering Interview Roadmap — Senior Track"
subtitle: "نقشه‌ی راه مصاحبه‌ی مهندسی داده — مسیر سنیور"
version: 0.1
language: fa + en (bilingual)
target: Senior Data Engineer interviews in Iran
timeline: 8 weeks
---

# Data Engineering Interview Roadmap — Senior Track
# نقشه‌ی راه مصاحبه‌ی مهندسی داده — مسیر سنیور

> **FA:** این سند فهرست کامل همه‌ی مباحثی است که برای مصاحبه‌ی مهندسی داده در سطح سنیور لازم است؛ از پایه تا پیشرفته. عنوان مباحث انگلیسی است و هر مبحث توضیح فارسی و انگلیسی دارد. ساختار سند طوری طراحی شده که بعداً مستقیماً به یک سایت تعاملی (شبیه HelloInterview) تبدیل شود: هر مبحث یک شناسه‌ی یکتا، سطح، اولویت و زمان تقریبی دارد.
>
> **EN:** This document is the complete topic inventory for senior-level data engineering interviews, from fundamentals to advanced. Topic titles are in English; every topic has a Persian and an English description. It is structured to be converted directly into an interactive site (HelloInterview-style): every topic has a unique ID, level, priority and estimated study time.

---

## How to Read This Document | راهنمای خواندن سند

**Metadata on every topic | فراداده‌ی هر مبحث**

| Field | Values | FA |
|---|---|---|
| `id` | kebab-case slug | شناسه‌ی یکتا (بعداً آدرس صفحه در سایت) |
| Level | 🟢 Beginner · 🟡 Intermediate · 🟠 Advanced · 🔴 Senior | سطح دشواری |
| Priority | **P0** must-know · **P1** very likely asked · **P2** good to know / differentiator | اولویت برای مصاحبه |
| Est. | hours | زمان تقریبی مطالعه |
| Type | Concept · Technology · Pattern · Problem · Practice · Guide | نوع محتوا |

**Content types (borrowed from HelloInterview) | انواع محتوا (الهام از HelloInterview)**

- **Guide** — راهنمای مصاحبه و روش آماده‌شدن / How the interview works and how to prepare
- **Concept** — مفاهیم پایه‌ای که مستقل از ابزار هستند / Tool-agnostic fundamentals
- **Technology** — بررسی عمیق یک تکنولوژی مشخص / Deep dive on one technology
- **Pattern** — الگوهای تکرارشونده در طراحی سیستم داده / Recurring data system design patterns
- **Problem** — مسئله‌ی کامل مصاحبه با پاسخ تشریحی / Full interview problem with worked solution
- **Practice** — تمرین (SQL، کدنویسی، مدل‌سازی) / Hands-on exercises

**Track map | نقشه‌ی مسیرها**

| # | Track | FA | Weight for Senior |
|---|---|---|---|
| 0 | Interview Guide | راهنمای مصاحبه | ★★★ |
| 1 | CS & Engineering Foundations | پایه‌های علوم کامپیوتر و مهندسی | ★★ |
| 2 | SQL | اس‌کیو‌ال | ★★★★★ |
| 3 | Python & Coding | پایتون و کدنویسی | ★★★ |
| 4 | Database Internals & Storage | معماری داخلی پایگاه داده | ★★★★ |
| 5 | Distributed Systems Theory | تئوری سیستم‌های توزیع‌شده | ★★★★ |
| 6 | Data Modeling | مدل‌سازی داده | ★★★★★ |
| 7 | Batch Processing (Hadoop & Spark) | پردازش دسته‌ای | ★★★★★ |
| 8 | Messaging & Stream Processing | پیام‌رسانی و پردازش جریانی | ★★★★★ |
| 9 | Data Ingestion & Integration | دریافت و یکپارچه‌سازی داده | ★★★★ |
| 10 | Warehouses, Lakes & Lakehouse | انبار، دریاچه و لیک‌هاوس | ★★★★ |
| 11 | Orchestration & Transformation | ارکستراسیون و تبدیل داده | ★★★★ |
| 12 | Data Quality, Observability & Governance | کیفیت، مشاهده‌پذیری و حاکمیت داده | ★★★ |
| 13 | Infrastructure & Platform | زیرساخت و پلتفرم | ★★★ |
| 14 | Data System Design | طراحی سیستم داده | ★★★★★ |
| 15 | ML & AI Data Engineering | مهندسی داده برای ML و AI | ★★ |
| 16 | Behavioral & Leadership | رفتاری و رهبری | ★★★★ |
| A–D | Appendices (8-week plan, resources, glossary, content model) | پیوست‌ها | — |

---

# Track 0 — Interview Guide | راهنمای مصاحبه

> **FA:** قبل از هر چیز باید بدانی مصاحبه چه چیزی را می‌سنجد. این بخش معادل «In a Hurry» در HelloInterview است.
> **EN:** Before anything else, understand what the interview measures. This is the equivalent of HelloInterview's "In a Hurry" section.

### 0.1 The Data Engineering Role
`de-role-overview` · 🟢 Beginner · P0 · ~1h · Guide
- **FA:** مهندس داده چه کار می‌کند و چه فرقی با تحلیل‌گر داده، دانشمند داده، Analytics Engineer و مهندس پلتفرم داده دارد. چرخه‌ی عمر داده: تولید، دریافت، ذخیره، تبدیل، سرو.
- **EN:** What a data engineer does and how it differs from data analyst, data scientist, analytics engineer and data platform engineer. The data engineering lifecycle: generation, ingestion, storage, transformation, serving.
- **Covers:** DE lifecycle (Reis & Housley), undercurrents (security, DataOps, orchestration, architecture, software engineering), role variants (pipeline-focused, platform-focused, analytics-focused)

### 0.2 The Iranian Data Engineering Market
`iran-de-market` · 🟢 Beginner · P0 · ~2h · Guide
- **FA:** شرکت‌های ایرانی در سه دسته‌ی کلی‌اند: شرکت‌های محصولی بزرگ (فروشگاهی، تاکسی اینترنتی، آگهی، اپ‌استور، فین‌تک)، اپراتورها و بانک‌ها و شرکت‌های نرم‌افزار بانکی، و استارتاپ‌ها. همه چیز تقریباً on-prem است چون کلاد عمومی در دسترس نیست. استک‌های رایج: Kafka، Spark، ClickHouse، Airflow، PostgreSQL، و در بانک و اپراتور Hadoop/Hive و Oracle. باید آگهی‌های شغلی شرکت‌های هدف را تحلیل کنی.
- **EN:** Iranian companies fall into three broad groups: large product companies (e-commerce, ride-hailing, classifieds, app stores, fintech), telcos/banks/banking-software vendors, and startups. Almost everything is on-prem because public cloud is effectively unavailable. Common stacks: Kafka, Spark, ClickHouse, Airflow, PostgreSQL, and Hadoop/Hive plus Oracle in banks and telcos. Analyze job postings of your target companies.
- **Covers:** company segments, typical stacks, on-prem implications, job posting analysis method, where to find postings (Jobinja, Jobvision, Quera, LinkedIn), salary ranges research
- **Practice:** Collect 15 senior DE job postings → tag technologies → build a frequency table → adjust priorities in this roadmap

### 0.3 What "Senior" Means
`senior-expectations` · 🟡 Intermediate · P0 · ~2h · Guide
- **FA:** تفاوت انتظارات از Mid و Senior: سنیور باید مبهم‌بودن مسئله را مدیریت کند، trade-off توضیح دهد، از تجربه‌ی واقعی شکست و بهینه‌سازی حرف بزند، مالکیت سیستم داشته باشد و بقیه را رشد دهد. در طراحی سیستم، سنیور سریع از پایه‌ها عبور می‌کند و وقت را صرف Deep Dive می‌کند.
- **EN:** How senior expectations differ from mid-level: handling ambiguity, articulating trade-offs, speaking from real failures and optimizations, owning systems end to end, and growing others. In system design, seniors move fast through basics and spend the time on deep dives.
- **Covers:** leveling rubric (Problem Navigation, Solution Design, Technical Excellence, Communication), depth vs breadth, red flags, "it depends" done right

### 0.4 The Interview Loop
`interview-loop` · 🟢 Beginner · P0 · ~1h · Guide
- **FA:** مراحل رایج: غربالگری تلفنی/HR، تست فنی آنلاین یا تکلیف خانگی، SQL زنده، کدنویسی پایتون، مصاحبه‌ی فنی عمیق روی تکنولوژی‌ها، طراحی سیستم داده، مرور پروژه‌های گذشته، مصاحبه‌ی رفتاری/مدیر، و مذاکره‌ی حقوق. در ایران معمولاً مرحله‌ی «پرسش عمیق از تجربه‌ها» وزن زیادی دارد.
- **EN:** Common stages: HR/phone screen, online test or take-home, live SQL, Python coding, technical deep-dive on technologies, data system design, past-project deep-dive, behavioral/manager round, offer negotiation. In Iran the "grill me on your experience" round often carries heavy weight.
- **Covers:** each round's goal, duration, how it is scored, how to prepare, take-home best practices

### 0.5 Delivery Framework for Data System Design
`delivery-framework` · 🟡 Intermediate · P0 · ~3h · Guide
- **FA:** یک چارچوب ثابت برای پاسخ‌دادن به هر سؤال طراحی سیستم داده تا گم نشوی: ۱) نیازمندی‌های کارکردی (چه کسی، چه داده‌ای، چه خروجی‌ای) ۲) نیازمندی‌های غیرکارکردی (تأخیر، تازگی داده، حجم، دقت، هزینه) ۳) منابع و تخمین حجم ۴) مدل داده ۵) معماری سطح بالا ۶) Deep Dive (exactly-once، داده‌ی دیررس، skew، backfill) ۷) عملیات: کیفیت، مانیتورینگ، بازیابی.
- **EN:** A repeatable framework for any data system design question: 1) functional requirements (who, what data, what output) 2) non-functional requirements (latency, freshness, volume, correctness, cost) 3) sources and back-of-envelope sizing 4) data model 5) high-level architecture 6) deep dives (exactly-once, late data, skew, backfill) 7) operations: quality, monitoring, recovery.
- **Covers:** time budgeting for 45/60 min, whiteboard layout, how to drive the conversation, how to handle interviewer push-back
- **Practice:** Apply the framework to 3 problems from Track 14 with a timer

### 0.6 How to Prepare in 8 Weeks
`how-to-prepare` · 🟢 Beginner · P0 · ~1h · Guide
- **FA:** روش مطالعه: اول مرور سریع کل نقشه، بعد عمیق‌شدن روی P0ها، تمرین روزانه‌ی SQL، هر هفته حداقل دو مسئله‌ی طراحی سیستم با زمان‌سنج، و مصاحبه‌ی آزمایشی (Mock). جزئیات در پیوست A.
- **EN:** Study method: quick pass over the whole map, then deep work on P0s, daily SQL practice, at least two timed system design problems per week, and mock interviews. Full plan in Appendix A.
- **Covers:** spaced repetition, active recall, mock interviews, tracking weak spots

### 0.7 Numbers Every Data Engineer Should Know
`numbers-to-know` · 🟡 Intermediate · P0 · ~2h · Concept
- **FA:** اعداد مرجع برای تخمین: تأخیر حافظه، SSD، دیسک و شبکه؛ توان عملیاتی یک بروکر Kafka، یک executor اسپارک، یک نود ClickHouse؛ اندازه‌ی رکوردها و نرخ فشرده‌سازی Parquet؛ تعداد ثانیه‌های روز (~۸۶٬۴۰۰ ≈ ۱۰⁵).
- **EN:** Reference numbers for estimation: memory, SSD, disk and network latency; rough throughput of a Kafka broker, a Spark executor, a ClickHouse node; record sizes and Parquet compression ratios; seconds per day (~86,400 ≈ 10⁵).
- **Covers:** latency table, throughput per component, storage math, QPS ↔ daily volume conversion, compression ratios
- **Practice:** "3 TB/day of CDRs — how many Kafka partitions, how much storage for 1 year in Parquet, how many Spark executors?"

---

# Track 1 — CS & Engineering Foundations | پایه‌های علوم کامپیوتر و مهندسی

### 1.1 Linux & Shell for Data Engineers
`linux-shell` · 🟢 Beginner · P1 · ~4h · Technology
- **FA:** کار روزمره با داده روی سرور: پردازش سریع فایل‌ها با ابزارهای خط فرمان، مدیریت فرایند و دیسک، اسکریپت‌نویسی Bash. در مصاحبه گاهی می‌پرسند «یک فایل ۱۰۰ گیگی را بدون Spark چطور تحلیل می‌کنی؟»
- **EN:** Everyday server-side data work: fast file processing with CLI tools, process and disk management, Bash scripting. Interviewers sometimes ask "how would you analyze a 100 GB file without Spark?"
- **Covers:** grep/awk/sed/cut/sort/uniq/xargs/parallel, find, jq, head/tail/split, pipes & redirection, cron, systemd, top/htop/iostat/vmstat, df/du, lsof, ssh/scp/rsync, file permissions
- **Interview Qs:** Count unique IPs in a huge log file · Find the 10 largest files · Why is the disk full but `du` shows free space?

### 1.2 Networking Essentials
`networking-essentials` · 🟢 Beginner · P1 · ~3h · Concept
- **FA:** پایه‌های شبکه که در طراحی سیستم لازم می‌شود: TCP و UDP، HTTP و HTTPS، DNS، لود بالانسر لایه ۴ و ۷، gRPC، WebSocket.
- **EN:** Networking basics needed for system design: TCP vs UDP, HTTP/HTTPS, DNS, L4 vs L7 load balancing, gRPC, WebSocket.
- **Covers:** OSI basics, connection pooling, keep-alive, TLS cost, bandwidth vs latency, cross-datacenter replication cost

### 1.3 Git & Engineering Workflow
`git-workflow` · 🟢 Beginner · P2 · ~2h · Technology
- **FA:** گیت، شاخه‌بندی، Code Review و CI. برای سنیور: چطور تیم داده را به روش مهندسی نرم‌افزار (تست، Review، CI/CD) عادت می‌دهی.
- **EN:** Git, branching, code review and CI. For seniors: how you bring software engineering discipline (tests, reviews, CI/CD) to a data team.
- **Covers:** rebase vs merge, trunk-based development, monorepo for data, pre-commit hooks

### 1.4 Data Structures & Algorithms for DE
`dsa-for-de` · 🟡 Intermediate · P1 · ~10h · Concept
- **FA:** سطح الگوریتم مورد انتظار در مصاحبه‌ی داده معمولاً Easy تا Medium است. تمرکز روی hash map، heap، sorting، two pointers، sliding window، intervals، گراف و topological sort (که پایه‌ی DAGهاست).
- **EN:** Algorithm level in DE interviews is usually LeetCode Easy–Medium. Focus on hash maps, heaps, sorting, two pointers, sliding window, intervals, graphs and topological sort (the basis of DAGs).
- **Covers:** Big-O, arrays/strings, hashing, heaps & top-K, sorting & external merge sort, intervals, BFS/DFS, topological sort, union-find (basic), LRU cache
- **Interview Qs:** Top-K frequent items · Merge overlapping intervals · Detect cycle in a DAG of jobs · Implement an LRU cache

### 1.5 Probabilistic Data Structures for Big Data
`probabilistic-ds` · 🟠 Advanced · P1 · ~4h · Concept
- **FA:** ساختارهای تقریبی برای داده‌ی حجیم: Bloom Filter برای عضویت، HyperLogLog برای شمارش یکتا، Count-Min Sketch برای فرکانس، t-digest برای صدک، Reservoir Sampling. سؤال کلاسیک: «تعداد کاربران یکتای روزانه را روی میلیاردها رویداد حساب کن.»
- **EN:** Approximate structures for big data: Bloom filter (membership), HyperLogLog (distinct count), Count-Min Sketch (frequency), t-digest (percentiles), reservoir sampling. Classic question: "count daily unique users over billions of events."
- **Covers:** error bounds, mergeability, where engines use them (ClickHouse uniq, Spark approx_count_distinct, Redis HLL)

### 1.6 Data Formats & Serialization
`data-formats` · 🟡 Intermediate · P0 · ~4h · Concept
- **FA:** فرمت‌های ردیفی و ستونی و تفاوت‌شان: CSV، JSON، Avro، Protobuf، Parquet، ORC، Arrow. ساختار داخلی Parquet (row group، column chunk، page، آمار min/max، dictionary encoding) و دلیل سرعت آن.
- **EN:** Row vs columnar formats and their trade-offs: CSV, JSON, Avro, Protobuf, Parquet, ORC, Arrow. Parquet internals (row groups, column chunks, pages, min/max stats, dictionary encoding) and why it is fast.
- **Covers:** schema evolution support per format, splittability, encoding (RLE, dictionary, delta), predicate pushdown, nested data, ASN.1 (telco)
- **Interview Qs:** Why Parquet over CSV? · Avro vs Protobuf for Kafka? · What makes a file "splittable"?

### 1.7 Compression
`compression` · 🟡 Intermediate · P1 · ~1h · Concept
- **FA:** الگوریتم‌های فشرده‌سازی و انتخاب بین سرعت و نرخ فشرده‌سازی: gzip، snappy، lz4، zstd. اثر آن بر CPU، شبکه و قابلیت تقسیم فایل.
- **EN:** Compression codecs and the speed vs ratio trade-off: gzip, snappy, lz4, zstd. Impact on CPU, network and file splittability.
- **Covers:** codec choice for Kafka, Parquet, ClickHouse; splittable vs non-splittable

### 1.8 Text Encoding & Persian Data
`encoding-persian-data` · 🟡 Intermediate · P1 · ~2h · Concept
- **FA:** مشکلات واقعی داده‌ی فارسی در ایران: UTF-8، تفاوت «ی/ي» و «ک/ك»، اعداد فارسی و عربی، نیم‌فاصله (ZWNJ)، تاریخ جلالی و تبدیل آن، منطقه‌ی زمانی تهران و تغییرات ساعت رسمی. این مبحث در مصاحبه‌های ایرانی تمایز ایجاد می‌کند.
- **EN:** Real-world Persian data problems: UTF-8, Persian vs Arabic "ye/kaf" characters, Persian/Arabic digits, zero-width non-joiner, Jalali calendar conversion, Tehran timezone and DST policy changes. A differentiator in Iranian interviews.
- **Covers:** normalization pipelines, collation and sorting, Jalali date dimension table, full-text search on Persian text

### 1.9 OS & JVM Essentials for Data Systems
`os-jvm-essentials` · 🟠 Advanced · P1 · ~4h · Concept
- **FA:** چون Spark، Kafka، Flink و Hadoop روی JVM اجرا می‌شوند، باید heap، GC، off-heap و page cache را بفهمی. Kafka سرعتش را مدیون page cache و zero-copy است.
- **EN:** Spark, Kafka, Flink and Hadoop run on the JVM, so understand heap, GC, off-heap memory and the OS page cache. Kafka's speed comes from the page cache and zero-copy.
- **Covers:** processes vs threads, virtual memory, page cache, sequential vs random I/O, mmap, zero-copy (sendfile), JVM heap & GC (G1, ZGC), GC pauses in long-running jobs

---

# Track 2 — SQL | اس‌کیو‌ال

> **FA:** مهم‌ترین مهارت قابل‌آزمون. تقریباً همه‌ی شرکت‌ها یک مرحله‌ی SQL زنده دارند. هدف: حل مسائل Medium/Hard در کمتر از ۱۵ دقیقه، با توضیح بلند.
> **EN:** The single most tested skill. Almost every company has a live SQL round. Goal: solve Medium/Hard problems in under 15 minutes while thinking aloud.

### 2.1 SQL Fundamentals & Logical Query Order
`sql-fundamentals` · 🟢 Beginner · P0 · ~3h · Concept
- **FA:** SELECT، WHERE، GROUP BY، HAVING، ORDER BY، LIMIT و ترتیب منطقی اجرا (FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY). فهم این ترتیب جلوی بسیاری از اشتباهات را می‌گیرد.
- **EN:** SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT and the logical execution order (FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY). Understanding this prevents many mistakes.
- **Covers:** aliases scope, DISTINCT, CASE WHEN, COALESCE, aggregate functions, COUNT(*) vs COUNT(col)

### 2.2 Joins in Depth
`sql-joins` · 🟢 Beginner · P0 · ~3h · Concept
- **FA:** همه‌ی انواع join: inner، left/right/full outer، cross، self، semi و anti join. مشکل fan-out (تکثیر ردیف‌ها) و رفتار NULL در شرط join.
- **EN:** All join types: inner, left/right/full outer, cross, self, semi and anti join. The fan-out (row duplication) problem and NULL behavior in join conditions.
- **Covers:** join cardinality (1:1, 1:N, N:M), filtering in ON vs WHERE for outer joins, EXISTS vs IN vs JOIN, NOT IN with NULLs trap
- **Interview Qs:** Customers who never ordered · Why did my revenue double after the join? · Difference between filter in ON and WHERE in a LEFT JOIN

### 2.3 Subqueries & CTEs
`sql-subqueries-ctes` · 🟢 Beginner · P0 · ~2h · Concept
- **FA:** زیرپرس‌وجوهای scalar، correlated و derived table، و CTE برای خوانایی. کی CTE materialize می‌شود و کی نه.
- **EN:** Scalar, correlated and derived-table subqueries, and CTEs for readability. When CTEs are materialized and when they are inlined.
- **Covers:** correlated subquery performance, LATERAL / CROSS APPLY

### 2.4 Window Functions
`sql-window-functions` · 🟡 Intermediate · P0 · ~6h · Concept
- **FA:** قلب مصاحبه‌های SQL. توابع رتبه‌بندی (ROW_NUMBER، RANK، DENSE_RANK، NTILE)، توابع جابه‌جایی (LAG، LEAD، FIRST_VALUE، LAST_VALUE)، تجمیع روی پنجره، و تفاوت frame از نوع ROWS و RANGE.
- **EN:** The heart of SQL interviews. Ranking (ROW_NUMBER, RANK, DENSE_RANK, NTILE), offset functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE), windowed aggregates, and ROWS vs RANGE frames.
- **Covers:** PARTITION BY vs GROUP BY, default frame pitfalls with LAST_VALUE, QUALIFY (where supported), moving averages, cumulative sums
- **Interview Qs:** Second highest salary per department · 7-day rolling average · Percentage of total per group

### 2.5 Recursive CTEs
`sql-recursive-ctes` · 🟠 Advanced · P1 · ~2h · Concept
- **FA:** پرس‌وجوی بازگشتی برای سلسله‌مراتب (چارت سازمانی، دسته‌بندی کالا) و پیمایش گراف، و تولید سری تاریخ.
- **EN:** Recursive queries for hierarchies (org chart, product categories), graph traversal, and generating date series.
- **Covers:** anchor + recursive member, cycle detection, depth limits

### 2.6 Advanced Grouping & Pivoting
`sql-advanced-grouping` · 🟡 Intermediate · P1 · ~2h · Concept
- **FA:** GROUPING SETS، ROLLUP، CUBE برای گزارش‌های چندسطحی، FILTER در تجمیع، و تبدیل ردیف به ستون (pivot) و برعکس.
- **EN:** GROUPING SETS, ROLLUP, CUBE for multi-level reports, aggregate FILTER clauses, pivot and unpivot.
- **Covers:** conditional aggregation `SUM(CASE WHEN ...)`, GROUPING() function

### 2.7 NULLs & Three-Valued Logic
`sql-nulls` · 🟢 Beginner · P0 · ~1h · Concept
- **FA:** NULL برابر هیچ چیز نیست، حتی خودش. اثرش روی شرط‌ها، تجمیع، join و NOT IN. منبع رایج باگ و سؤال انحرافی در مصاحبه.
- **EN:** NULL equals nothing, not even itself. Its impact on predicates, aggregates, joins and NOT IN. A frequent source of bugs and trick questions.
- **Covers:** IS DISTINCT FROM, NULL ordering, NULLIF, COUNT behavior

### 2.8 Dates, Times & Time Zones in SQL
`sql-datetime` · 🟡 Intermediate · P0 · ~2h · Concept
- **FA:** کار با تاریخ و زمان: truncate، اختلاف تاریخ، منطقه‌ی زمانی، timestamp with/without time zone، تولید سری تاریخ برای پرکردن روزهای خالی، و جدول تاریخ جلالی.
- **EN:** Working with dates and times: truncation, date diffs, time zones, timestamp with/without time zone, generating date series to fill gaps, Jalali date dimension.
- **Covers:** date spine pattern, week definitions (Saturday-start weeks in Iran), UTC storage best practice

### 2.9 Classic SQL Interview Patterns
`sql-interview-patterns` · 🟠 Advanced · P0 · ~15h · Practice
- **FA:** الگوهایی که بارها در مصاحبه تکرار می‌شوند. هر الگو یک راه‌حل استاندارد دارد که باید از حفظ باشی.
- **EN:** Patterns that recur over and over in interviews. Each has a standard solution you should know by heart.
- **Patterns:**
  1. **Top-N per group** — ROW_NUMBER + filter
  2. **Gaps & Islands** — consecutive days / streaks (ROW_NUMBER difference trick)
  3. **Sessionization** — LAG + new-session flag + cumulative SUM
  4. **Running totals & moving averages**
  5. **Retention & cohort analysis** — first-activity cohort, day-N retention
  6. **Funnel analysis** — step conversion with ordered events
  7. **Deduplication** — keep latest record per key
  8. **Median & percentiles** — PERCENTILE_CONT, manual median
  9. **Period-over-period** — MoM / YoY growth with LAG
  10. **Point-in-time lookup** — join to SCD2 with valid_from/valid_to
  11. **Anti-join** — "never did X"
  12. **Pairs & combinations** — self-join with `a.id < b.id`
  13. **Histogram / bucketing** — WIDTH_BUCKET / FLOOR
  14. **Overlapping intervals** — concurrent sessions, max concurrency
  15. **Market basket** — products bought together
  16. **Fill forward** — last non-null value
  17. **Pivot reports** — conditional aggregation
- **Practice:** 60+ problems (DataLemur, LeetCode SQL 50 + Hard, StrataScratch) tagged by pattern

### 2.10 Query Execution & Optimization
`sql-query-optimization` · 🟠 Advanced · P0 · ~6h · Concept
- **FA:** خواندن EXPLAIN و EXPLAIN ANALYZE، الگوریتم‌های join (nested loop، hash join، merge join)، آمار جدول و تخمین cardinality، predicate pushdown و partition pruning. سؤال سنیور: «این کوئری کند است؛ قدم‌به‌قدم چطور عیب‌یابی می‌کنی؟»
- **EN:** Reading EXPLAIN / EXPLAIN ANALYZE, join algorithms (nested loop, hash, merge), table statistics and cardinality estimation, predicate pushdown and partition pruning. Senior question: "this query is slow — walk me through how you debug it."
- **Covers:** cost-based optimizer, sargable predicates, function-on-column anti-pattern, join order, spilling to disk, materialized views

### 2.11 Indexing
`sql-indexing` · 🟡 Intermediate · P0 · ~3h · Concept
- **FA:** ساختار B-tree، index ترکیبی و اهمیت ترتیب ستون‌ها، covering index، index در PostgreSQL (GIN، GiST، BRIN)، و هزینه‌ی index روی نوشتن.
- **EN:** B-tree structure, composite indexes and column order, covering indexes, PostgreSQL index types (GIN, GiST, BRIN), and the write cost of indexes.
- **Covers:** clustered vs non-clustered, index-only scan, partial indexes, when indexes don't help in OLAP (sparse indexes instead)

### 2.12 Transactions, Isolation & Locking
`sql-transactions` · 🟠 Advanced · P1 · ~3h · Concept
- **FA:** ACID، سطوح ایزولیشن (Read Uncommitted تا Serializable)، ناهنجاری‌ها (dirty read، non-repeatable read، phantom، write skew)، MVCC و قفل‌گذاری.
- **EN:** ACID, isolation levels (Read Uncommitted to Serializable), anomalies (dirty read, non-repeatable read, phantom, write skew), MVCC and locking.
- **Covers:** SELECT FOR UPDATE, deadlocks, optimistic vs pessimistic concurrency

### 2.13 DDL, Upserts & MERGE
`sql-ddl-merge` · 🟡 Intermediate · P0 · ~2h · Concept
- **FA:** طراحی جدول، constraintها، جدول partition‌شده، و الگوی upsert با MERGE یا INSERT ... ON CONFLICT. پایه‌ی بارگذاری افزایشی.
- **EN:** Table design, constraints, partitioned tables, and upserts via MERGE or INSERT ... ON CONFLICT. The basis of incremental loading.
- **Covers:** idempotent loads, staging-table swap pattern, truncate-insert vs delete-insert per partition

### 2.14 SQL Dialects Across Engines
`sql-dialects` · 🟡 Intermediate · P2 · ~2h · Concept
- **FA:** تفاوت‌های مهم PostgreSQL، Oracle (رایج در بانک‌ها)، Spark SQL / Hive، ClickHouse و StarRocks. در مصاحبه معمولاً اجازه‌ی انتخاب dialect دارید؛ PostgreSQL انتخاب امن است.
- **EN:** Key differences between PostgreSQL, Oracle (common in banks), Spark SQL / Hive, ClickHouse and StarRocks. Interviews usually let you pick a dialect; PostgreSQL is the safe choice.
- **Covers:** ClickHouse array functions & argMax, Spark explode/lateral view, Oracle analytic functions, date function differences

---

# Track 3 — Python & Coding | پایتون و کدنویسی

### 3.1 Core Python
`python-core` · 🟢 Beginner · P0 · ~4h · Technology
- **FA:** ساختارهای داده‌ی داخلی (list، dict، set، tuple)، comprehension، iterator و generator، context manager، decorator، و ماژول‌های collections و itertools.
- **EN:** Built-in data structures (list, dict, set, tuple), comprehensions, iterators and generators, context managers, decorators, and the collections and itertools modules.
- **Covers:** mutability, dict ordering, defaultdict/Counter/deque, heapq, functools (lru_cache, partial), dataclasses, typing

### 3.2 Python Concurrency & Internals
`python-concurrency` · 🟠 Advanced · P1 · ~3h · Concept
- **FA:** GIL و اثرش، تفاوت threading، multiprocessing و asyncio، کار I/O-bound در برابر CPU-bound، و مدیریت حافظه.
- **EN:** The GIL and its impact, threading vs multiprocessing vs asyncio, I/O-bound vs CPU-bound work, and memory management.
- **Covers:** concurrent.futures, asyncio for API ingestion, reference counting & GC

### 3.3 Processing Large Files in Python
`python-large-files` · 🟡 Intermediate · P0 · ~3h · Practice
- **FA:** پردازش فایلی بزرگ‌تر از RAM: خواندن جریانی خط‌به‌خط، chunking، generatorها، external sort و map-reduce دستی. سؤال رایج کدنویسی در مصاحبه‌های داده.
- **EN:** Processing files larger than RAM: line-by-line streaming, chunking, generators, external sort and hand-rolled map-reduce. A common DE coding question.
- **Interview Qs:** Find top-10 URLs in a 50 GB log with 8 GB RAM · Merge K sorted files · Deduplicate a stream with bounded memory

### 3.4 DataFrames: pandas, Polars & DuckDB
`python-dataframes` · 🟡 Intermediate · P1 · ~4h · Technology
- **FA:** کار با DataFrame روی یک ماشین. pandas رایج‌ترین است؛ Polars و DuckDB سریع‌تر و ستونی‌اند. کی دیگر به Spark نیاز نداری؟
- **EN:** Single-machine DataFrame work. pandas is most common; Polars and DuckDB are faster and columnar. When do you no longer need Spark?
- **Covers:** vectorization, groupby/merge, memory dtypes, lazy evaluation in Polars, DuckDB over Parquet, Apache Arrow

### 3.5 PySpark API
`pyspark-api` · 🟡 Intermediate · P0 · ~5h · Technology
- **FA:** نوشتن جاب با DataFrame API پایتون: transformation و action، UDF و هزینه‌اش، Pandas UDF، window در PySpark، و تست جاب‌ها.
- **EN:** Writing jobs with the Python DataFrame API: transformations vs actions, UDFs and their cost, Pandas UDFs, windows in PySpark, and testing jobs.
- **Covers:** Python ↔ JVM serialization overhead, Arrow optimization, explode/struct/array ops
- **Interview Qs:** Rewrite this UDF with built-in functions · Why is my Python UDF slow?

### 3.6 Production-Grade Python for Pipelines
`python-production` · 🟡 Intermediate · P1 · ~3h · Practice
- **FA:** کد پایپ‌لاین قابل اعتماد: retry با backoff، idempotency، لاگ ساخت‌یافته، مدیریت پیکربندی و secret، تست با pytest و Testcontainers، بسته‌بندی با uv یا poetry، lint و type check.
- **EN:** Reliable pipeline code: retry with backoff, idempotency, structured logging, config and secrets management, testing with pytest and Testcontainers, packaging with uv/poetry, linting and type checking.
- **Covers:** project structure, unit vs integration tests for data, mocking sources, pydantic for schema validation

### 3.7 DE Coding Interview Patterns
`de-coding-patterns` · 🟡 Intermediate · P0 · ~10h · Practice
- **FA:** مسائل کدنویسی خاص مصاحبه‌های داده که با LeetCode عمومی فرق دارند.
- **EN:** Coding problems specific to data interviews, distinct from generic LeetCode.
- **Problems:**
  1. Parse & aggregate log lines
  2. Flatten nested JSON
  3. Merge overlapping time intervals / sessions
  4. Top-K with heap (stream)
  5. Sliding-window rate limiter
  6. Deduplicate events with TTL
  7. LRU / TTL cache
  8. Implement a simple map-reduce word count
  9. Topological sort of job dependencies
  10. Join two large sorted files (merge join)
  11. Reservoir sampling
  12. Validate and clean CSV rows with schema
  13. Compute sessions per user from click stream
  14. Implement a mini scheduler with retries

### 3.8 Java/Scala for Data Systems
`jvm-languages-for-de` · 🟡 Intermediate · P2 · ~3h · Technology
- **FA:** Spark و Flink و Kafka بومی JVM هستند. دانستن Java یا Scala در حد خواندن کد، نوشتن UDF یا کانکتور، و فهم stack trace یک برتری است.
- **EN:** Spark, Flink and Kafka are JVM-native. Knowing Java or Scala well enough to read source, write UDFs or connectors, and read stack traces is an advantage.
- **Covers:** Scala basics for Spark, Flink DataStream API in Java, Kafka client in Java, when to choose JVM over Python

---

# Track 4 — Database Internals & Storage | معماری داخلی پایگاه داده

### 4.1 OLTP vs OLAP
`oltp-vs-olap` · 🟢 Beginner · P0 · ~1h · Concept
- **FA:** تفاوت بارکاری تراکنشی (نوشتن‌های کوچک و زیاد، کلید اصلی) و تحلیلی (اسکن حجیم، تجمیع). چرا انبار داده را جدا از دیتابیس عملیاتی می‌سازیم. HTAP چیست.
- **EN:** Transactional workloads (many small writes, point lookups) vs analytical workloads (large scans, aggregations). Why warehouses are separate from operational databases. What HTAP is.
- **Covers:** access patterns, normalization choices, HTAP (TiDB, etc.)

### 4.2 Row vs Columnar Storage & Vectorized Execution
`columnar-storage` · 🟡 Intermediate · P0 · ~3h · Concept
- **FA:** چرا ذخیره‌سازی ستونی برای تحلیل سریع‌تر است: خواندن فقط ستون‌های لازم، فشرده‌سازی بهتر، اجرای برداری (SIMD). پایه‌ی ClickHouse، StarRocks و Parquet.
- **EN:** Why columnar storage is faster for analytics: read only needed columns, better compression, vectorized (SIMD) execution. The foundation of ClickHouse, StarRocks and Parquet.
- **Covers:** late materialization, zone maps / min-max indexes, sparse primary index, column encodings

### 4.3 Storage Engines: B-Tree vs LSM-Tree
`storage-engines` · 🟠 Advanced · P1 · ~4h · Concept
- **FA:** دو خانواده‌ی اصلی موتور ذخیره‌سازی. B-tree برای خواندن، LSM برای نوشتن سنگین (Cassandra، RocksDB، HBase). مفاهیم memtable، SSTable، compaction و تقویت نوشتن/خواندن/فضا.
- **EN:** The two main storage engine families. B-trees favor reads, LSM-trees favor heavy writes (Cassandra, RocksDB, HBase). Memtable, SSTable, compaction, and write/read/space amplification.
- **Covers:** WAL, compaction strategies (size-tiered vs leveled), Bloom filters in LSM, RocksDB as state backend in Flink/Kafka Streams

### 4.4 Replication
`replication` · 🟠 Advanced · P0 · ~4h · Concept
- **FA:** تکثیر داده: leader-follower، multi-leader، leaderless (quorum). همگام در برابر ناهمگام، replication lag و مشکلاتش، failover.
- **EN:** Data replication: leader-follower, multi-leader, leaderless (quorum). Sync vs async, replication lag and its anomalies, failover.
- **Covers:** read-your-writes, monotonic reads, quorum math (W + R > N), split brain

### 4.5 Partitioning & Sharding
`partitioning-sharding` · 🟠 Advanced · P0 · ~4h · Concept
- **FA:** تقسیم داده بین نودها: range، hash، consistent hashing، و partition بر اساس زمان. مشکل hot spot و کلید داغ، و rebalancing.
- **EN:** Splitting data across nodes: range, hash, consistent hashing, and time-based partitioning. Hot spots and hot keys, and rebalancing.
- **Covers:** secondary indexes on partitioned data (local vs global), request routing, partition vs bucket in Hive/Spark

### 4.6 WAL, MVCC & Durability
`wal-mvcc` · 🟠 Advanced · P1 · ~2h · Concept
- **FA:** Write-Ahead Log چطور دوام را تضمین می‌کند و همان چیزی است که CDC از آن می‌خواند. MVCC چطور خواندن و نوشتن هم‌زمان را بدون قفل ممکن می‌کند.
- **EN:** How the write-ahead log guarantees durability and is exactly what CDC reads from. How MVCC enables concurrent reads and writes without locks.
- **Covers:** fsync, checkpoints, PostgreSQL WAL & logical decoding, MySQL binlog

### 4.7 PostgreSQL Deep Dive
`postgresql` · 🟡 Intermediate · P1 · ~4h · Technology
- **FA:** پرکاربردترین دیتابیس عملیاتی در شرکت‌های ایرانی. معماری، VACUUM و bloat، partitioning، logical replication، اکستنشن‌ها (TimescaleDB، Citus)، و تنظیمات کارایی.
- **EN:** The most common operational database in Iranian companies. Architecture, VACUUM and bloat, partitioning, logical replication, extensions (TimescaleDB, Citus), performance tuning.
- **Covers:** connection pooling (PgBouncer), replication slots and CDC, JSONB, when Postgres is enough as a warehouse

### 4.8 NoSQL Families
`nosql-families` · 🟡 Intermediate · P1 · ~4h · Concept
- **FA:** دسته‌های NoSQL و کاربرد هرکدام: key-value (Redis)، document (MongoDB)، wide-column (Cassandra، HBase، ScyllaDB)، graph (Neo4j)، search (Elasticsearch)، time-series. مدل‌سازی query-first.
- **EN:** NoSQL families and when to use each: key-value (Redis), document (MongoDB), wide-column (Cassandra, HBase, ScyllaDB), graph (Neo4j), search (Elasticsearch), time-series. Query-first modeling.
- **Covers:** Cassandra partition key & clustering key, HBase row key design, Redis data structures, Elasticsearch inverted index

### 4.9 MPP Query Engine Architecture
`mpp-architecture` · 🟠 Advanced · P0 · ~3h · Concept
- **FA:** معماری shared-nothing در انبارهای توزیع‌شده: coordinator و worker، توزیع داده، و joinهای توزیع‌شده (broadcast، shuffle، colocate). پایه‌ی فهم StarRocks، ClickHouse، Trino و Greenplum.
- **EN:** Shared-nothing architecture in distributed warehouses: coordinator and workers, data distribution, and distributed joins (broadcast, shuffle, colocate). Foundation for StarRocks, ClickHouse, Trino and Greenplum.
- **Covers:** shared-nothing vs shared-disk vs storage-compute separation, pipeline execution, runtime filters, query queues & resource groups

---

# Track 5 — Distributed Systems Theory | تئوری سیستم‌های توزیع‌شده

> **FA:** منبع اصلی: کتاب DDIA. سنیورها باید بتوانند «چرا» پشت رفتار Kafka و Spark را با این مفاهیم توضیح دهند.
> **EN:** Primary source: DDIA. Seniors must explain the "why" behind Kafka and Spark behavior using these concepts.

### 5.1 Fundamentals of Distributed Systems
`distributed-fundamentals` · 🟡 Intermediate · P0 · ~3h · Concept
- **FA:** چرا سیستم توزیع‌شده سخت است: خرابی جزئی، شبکه‌ی غیرقابل‌اعتماد، ساعت‌های ناهماهنگ، و تشخیص خرابی با timeout.
- **EN:** Why distributed systems are hard: partial failures, unreliable networks, unsynchronized clocks, and failure detection via timeouts.
- **Covers:** fallacies of distributed computing, heartbeats, fencing tokens, idempotency as a core tool

### 5.2 CAP & PACELC
`cap-pacelc` · 🟡 Intermediate · P0 · ~2h · Concept
- **FA:** قضیه‌ی CAP و تفسیر درست آن (فقط هنگام partition شبکه انتخاب بین C و A داری)، و PACELC که تأخیر را هم وارد می‌کند. کاربرد در انتخاب دیتابیس.
- **EN:** The CAP theorem and its correct interpretation (the C vs A choice only applies during a network partition), and PACELC which adds latency. Applying it to database choice.
- **Covers:** CP vs AP examples (ZooKeeper, Cassandra, Kafka config)

### 5.3 Consistency Models
`consistency-models` · 🟠 Advanced · P1 · ~3h · Concept
- **FA:** طیف سازگاری: linearizable، sequential، causal، eventual، read-your-writes. تفاوت consistency در ACID و در CAP.
- **EN:** The consistency spectrum: linearizable, sequential, causal, eventual, read-your-writes. The difference between C in ACID and C in CAP.

### 5.4 Consensus & Coordination
`consensus` · 🟠 Advanced · P1 · ~3h · Concept
- **FA:** الگوریتم Raft در حد مفهوم، leader election، و ابزارهای هماهنگی ZooKeeper و etcd. مهاجرت Kafka از ZooKeeper به KRaft.
- **EN:** Raft at a conceptual level, leader election, and coordination services ZooKeeper and etcd. Kafka's migration from ZooKeeper to KRaft.
- **Covers:** quorum, term/epoch, distributed locks and their pitfalls, ClickHouse Keeper

### 5.5 Distributed Transactions, Sagas & Outbox
`distributed-transactions` · 🟠 Advanced · P1 · ~3h · Concept
- **FA:** 2PC و مشکلاتش، الگوی Saga، و الگوی Transactional Outbox برای انتشار مطمئن رویداد از سرویس به Kafka بدون dual-write.
- **EN:** 2PC and its problems, the Saga pattern, and the transactional outbox pattern for reliably publishing events from a service to Kafka without dual writes.
- **Covers:** dual-write problem, outbox + Debezium, inbox pattern for consumers

### 5.6 Delivery Semantics & Exactly-Once
`delivery-semantics` · 🔴 Senior · P0 · ~4h · Concept
- **FA:** at-most-once، at-least-once و exactly-once؛ و اینکه exactly-once واقعی در عمل یعنی «at-least-once + idempotency» یا تراکنش انتها-به-انتها. یکی از پرتکرارترین سؤال‌های سنیور.
- **EN:** At-most-once, at-least-once and exactly-once; and why practical exactly-once means "at-least-once + idempotency" or end-to-end transactions. One of the most frequent senior questions.
- **Covers:** idempotent producers, transactional writes, idempotent sinks (upsert, dedup keys), two-phase commit sinks, effectively-once
- **Interview Qs:** How do you guarantee no duplicate rows in StarRocks from Kafka? · What happens to exactly-once if the sink is not transactional?

### 5.7 Time, Clocks & Ordering
`time-and-ordering` · 🟠 Advanced · P1 · ~2h · Concept
- **FA:** ساعت فیزیکی و منطقی (Lamport، vector clock)، clock skew، و اینکه ترتیب رویدادها فقط داخل یک partition تضمین می‌شود.
- **EN:** Physical vs logical clocks (Lamport, vector clocks), clock skew, and why event ordering is guaranteed only within a partition.
- **Covers:** event time vs ingestion time vs processing time, NTP, hybrid logical clocks

### 5.8 Backpressure & Flow Control
`backpressure` · 🟠 Advanced · P1 · ~2h · Concept
- **FA:** وقتی مصرف‌کننده کندتر از تولیدکننده است چه می‌شود. backpressure در Flink، lag در Kafka، و راهکارها: بافر، مقیاس افقی، rate limit، shedding.
- **EN:** What happens when consumers are slower than producers. Backpressure in Flink, consumer lag in Kafka, and remedies: buffering, horizontal scaling, rate limiting, load shedding.

### 5.9 Fault Tolerance Patterns
`fault-tolerance` · 🟡 Intermediate · P1 · ~2h · Concept
- **FA:** retry با exponential backoff و jitter، circuit breaker، checkpointing، dead letter queue، و replay.
- **EN:** Retry with exponential backoff and jitter, circuit breakers, checkpointing, dead letter queues, and replay.

---

# Track 6 — Data Modeling | مدل‌سازی داده

> **FA:** در کنار SQL، مهم‌ترین مبحثی که تجربه‌ی عملی به‌تنهایی برایش کافی نیست. سنیور باید بتواند grain را تعریف کند و از انتخاب‌هایش دفاع کند.
> **EN:** Alongside SQL, the topic where hands-on experience alone is not enough. Seniors must define grain and defend their choices.

### 6.1 Conceptual, Logical & Physical Models
`modeling-levels` · 🟢 Beginner · P1 · ~2h · Concept
- **FA:** سه سطح مدل‌سازی، ERD، موجودیت و رابطه، و اینکه مدل فیزیکی به موتور (Postgres، ClickHouse، Cassandra) وابسته است.
- **EN:** The three modeling levels, ERDs, entities and relationships, and why the physical model depends on the engine (Postgres, ClickHouse, Cassandra).

### 6.2 Normalization & Denormalization
`normalization` · 🟢 Beginner · P0 · ~2h · Concept
- **FA:** فرم‌های نرمال (1NF تا BCNF)، ناهنجاری‌های به‌روزرسانی، و اینکه چرا در تحلیل عمداً denormalize می‌کنیم.
- **EN:** Normal forms (1NF to BCNF), update anomalies, and why analytics deliberately denormalizes.

### 6.3 Dimensional Modeling (Kimball)
`dimensional-modeling` · 🟡 Intermediate · P0 · ~8h · Concept
- **FA:** روش Kimball: چهار گام (فرایند کسب‌وکار، grain، ابعاد، معیارها)، جدول fact و dimension، surrogate key، conformed dimension و bus matrix.
- **EN:** The Kimball method: four steps (business process, grain, dimensions, facts), fact and dimension tables, surrogate keys, conformed dimensions and the bus matrix.
- **Covers:** additive / semi-additive / non-additive measures, star schema, enterprise bus architecture
- **Interview Qs:** Define the grain of an orders fact table · Why surrogate keys instead of natural keys?

### 6.4 Fact Table Types
`fact-table-types` · 🟡 Intermediate · P0 · ~3h · Concept
- **FA:** fact تراکنشی، periodic snapshot (مثلاً موجودی روزانه‌ی حساب)، accumulating snapshot (چرخه‌ی سفارش با چند تاریخ)، و factless fact (حضور، ثبت‌نام).
- **EN:** Transaction facts, periodic snapshots (e.g., daily account balance), accumulating snapshots (order lifecycle with multiple dates), and factless facts (attendance, enrollment).

### 6.5 Slowly Changing Dimensions
`scd` · 🟡 Intermediate · P0 · ~4h · Concept
- **FA:** نگهداری تاریخچه‌ی ابعاد: SCD نوع ۰، ۱، ۲، ۳، ۴، ۶. پیاده‌سازی SCD2 با valid_from/valid_to/is_current و MERGE، و join «در لحظه‌ی زمانی».
- **EN:** Keeping dimension history: SCD types 0, 1, 2, 3, 4, 6. Implementing SCD2 with valid_from/valid_to/is_current and MERGE, and point-in-time joins.
- **Covers:** dbt snapshots, SCD2 in lakehouse (MERGE INTO), late-arriving dimensions
- **Interview Qs:** Implement SCD2 in SQL · A customer changes city — how does last year's revenue report stay correct?

### 6.6 Advanced Dimension Techniques
`advanced-dimensions` · 🟠 Advanced · P1 · ~3h · Concept
- **FA:** ابعاد junk، degenerate، role-playing (چند نقش برای تاریخ)، mini-dimension، outrigger، bridge table برای رابطه‌ی چندبه‌چند، و late-arriving facts/dimensions.
- **EN:** Junk, degenerate, role-playing (date in multiple roles), mini-dimensions, outriggers, bridge tables for many-to-many, and late-arriving facts/dimensions.

### 6.7 Star vs Snowflake vs One Big Table
`star-snowflake-obt` · 🟡 Intermediate · P0 · ~2h · Concept
- **FA:** مقایسه‌ی star، snowflake و جدول پهن (OBT). در موتورهای ستونی مثل ClickHouse جدول پهن رایج است؛ trade-offها را بدان.
- **EN:** Star vs snowflake vs wide One Big Table. Wide tables are common in columnar engines like ClickHouse; know the trade-offs.

### 6.8 Inmon, Kimball & Data Vault 2.0
`modeling-methodologies` · 🟠 Advanced · P1 · ~4h · Concept
- **FA:** مقایسه‌ی روش‌های کلان: Inmon (انبار نرمال سازمانی)، Kimball (data mart ابعادی)، Data Vault 2.0 (hub، link، satellite) که در بانک‌ها برای حسابرسی و تاریخچه محبوب است.
- **EN:** Comparing methodologies: Inmon (normalized enterprise warehouse), Kimball (dimensional marts), Data Vault 2.0 (hubs, links, satellites), popular in banks for auditability and history.

### 6.9 Physical Modeling for Columnar & MPP Engines
`physical-modeling-olap` · 🟠 Advanced · P0 · ~4h · Concept
- **FA:** طراحی فیزیکی برای موتورهای تحلیلی: کلید مرتب‌سازی، partition، توزیع/bucket، انتخاب مدل جدول در StarRocks و موتور MergeTree در ClickHouse، و materialized view برای تجمیع از پیش.
- **EN:** Physical design for analytical engines: sort keys, partitioning, distribution/bucketing, StarRocks table models and ClickHouse MergeTree engines, and materialized views for pre-aggregation.
- **Covers:** cardinality and sort-key order, partition granularity mistakes, colocate joins, rollups

### 6.10 Modeling for NoSQL & Serving Stores
`nosql-modeling` · 🟠 Advanced · P2 · ~2h · Concept
- **FA:** مدل‌سازی بر اساس الگوی کوئری برای Cassandra، HBase و Redis؛ طراحی row key برای جلوگیری از hot spot.
- **EN:** Query-first modeling for Cassandra, HBase and Redis; row-key design to avoid hot spots.

### 6.11 Event & Clickstream Modeling
`event-modeling` · 🟡 Intermediate · P1 · ~3h · Concept
- **FA:** طراحی schema برای رویدادها: envelope استاندارد (event_id، event_time، user_id، properties)، نسخه‌بندی schema، و مدل‌سازی sessions و funnels.
- **EN:** Designing event schemas: a standard envelope (event_id, event_time, user_id, properties), schema versioning, and modeling sessions and funnels.
- **Covers:** wide events vs narrow events, semi-structured columns (JSON/Map), tracking plans

### 6.12 Schema Evolution
`schema-evolution` · 🟠 Advanced · P0 · ~2h · Concept
- **FA:** تغییر schema بدون شکستن مصرف‌کننده‌ها: سازگاری backward، forward و full؛ اضافه/حذف/تغییرنام ستون در Avro، Parquet، Iceberg و ClickHouse.
- **EN:** Changing schemas without breaking consumers: backward, forward and full compatibility; adding/removing/renaming columns in Avro, Parquet, Iceberg and ClickHouse.

### 6.13 Semantic & Metrics Layer
`semantic-layer` · 🟠 Advanced · P2 · ~2h · Concept
- **FA:** یک تعریف واحد برای معیارهای کسب‌وکار (مثلاً «کاربر فعال») تا همه‌ی داشبوردها عدد یکسان بدهند. dbt Semantic Layer، Cube، LookML.
- **EN:** A single definition for business metrics (e.g., "active user") so every dashboard agrees. dbt Semantic Layer, Cube, LookML.

### 6.14 Data Modeling Interview Problems
`modeling-problems` · 🔴 Senior · P0 · ~10h · Practice
- **FA:** تمرین مدل‌سازی کامل برای دامنه‌های واقعی؛ در هر کدام grain، fact، dimension، SCD و کوئری‌های کلیدی را تعریف کن.
- **EN:** Full modeling exercises for real domains; in each, define grain, facts, dimensions, SCDs and key queries.
- **Problems:**
  1. Ride-hailing (trips, drivers, pricing, surge)
  2. E-commerce (orders, items, inventory, returns)
  3. Telco CDR & billing (calls, SMS, data sessions, subscribers, tariffs)
  4. Banking transactions & account balances
  5. Food delivery marketplace (three-sided)
  6. Ads (impressions, clicks, conversions, attribution)
  7. Subscription / SaaS (MRR, churn)
  8. Music/video streaming (plays, sessions)
  9. Classifieds marketplace (listings, contacts)
  10. HR / payroll with historical changes

---

# Track 7 — Batch Processing (Hadoop & Spark) | پردازش دسته‌ای

### 7.1 MapReduce & the Origins of Big Data
`mapreduce` · 🟢 Beginner · P1 · ~2h · Concept
- **FA:** مدل MapReduce (map، shuffle، reduce) و چرا هنوز فهمش لازم است: تمام مفاهیم Spark از آن می‌آید. محلی‌بودن داده (data locality).
- **EN:** The MapReduce model (map, shuffle, reduce) and why it still matters: every Spark concept descends from it. Data locality.

### 7.2 Hadoop Ecosystem: HDFS, YARN, Hive
`hadoop-ecosystem` · 🟡 Intermediate · P1 · ~5h · Technology
- **FA:** هنوز در بانک‌ها و اپراتورهای ایرانی رایج است. HDFS (NameNode، DataNode، بلاک، replication)، YARN (ResourceManager، NodeManager، صف‌ها)، Hive (Metastore، partition، bucket، ORC)، و HBase. مشکل small files در HDFS.
- **EN:** Still common in Iranian banks and telcos. HDFS (NameNode, DataNode, blocks, replication), YARN (ResourceManager, NodeManager, queues), Hive (Metastore, partitions, buckets, ORC), and HBase. The HDFS small-files problem.
- **Covers:** NameNode HA, Hive Metastore as the de-facto catalog, Hive on Tez/Spark, Sqoop (legacy), Oozie (legacy), migration paths to lakehouse

### 7.3 Spark Architecture
`spark-architecture` · 🟡 Intermediate · P0 · ~5h · Technology
- **FA:** driver و executor، cluster managerها (YARN، Kubernetes، standalone)، و سلسله‌مراتب job → stage → task. ارزیابی تنبل، DAG، و تفاوت narrow و wide transformation.
- **EN:** Driver and executors, cluster managers (YARN, Kubernetes, standalone), and the job → stage → task hierarchy. Lazy evaluation, the DAG, and narrow vs wide transformations.
- **Interview Qs:** What triggers a new stage? · What runs on the driver vs executors? · What happens when an executor dies?

### 7.4 Spark APIs & the Catalyst Optimizer
`spark-catalyst` · 🟠 Advanced · P0 · ~4h · Technology
- **FA:** RDD در برابر DataFrame و Dataset، بهینه‌ساز Catalyst (logical plan → optimized → physical)، Tungsten و whole-stage codegen، و خواندن explain().
- **EN:** RDD vs DataFrame vs Dataset, the Catalyst optimizer (logical plan → optimized → physical), Tungsten and whole-stage codegen, and reading explain().
- **Covers:** predicate & projection pushdown, constant folding, why UDFs block optimization

### 7.5 Shuffle & Partitioning
`spark-shuffle` · 🟠 Advanced · P0 · ~4h · Concept
- **FA:** shuffle گران‌ترین عملیات Spark است. فایل‌های shuffle، تعداد partition (spark.sql.shuffle.partitions)، repartition در برابر coalesce، و partitionBy هنگام نوشتن.
- **EN:** Shuffle is Spark's most expensive operation. Shuffle files, partition count (spark.sql.shuffle.partitions), repartition vs coalesce, and partitionBy on write.
- **Covers:** external shuffle service, shuffle spill, push-based shuffle, ideal partition size (~128–256 MB)

### 7.6 Joins & Data Skew in Spark
`spark-joins-skew` · 🔴 Senior · P0 · ~5h · Concept
- **FA:** استراتژی‌های join (broadcast hash، sort-merge، shuffle hash) و کی هرکدام انتخاب می‌شود. skew: تشخیص در Spark UI و راه‌حل‌ها (salting، broadcast، AQE skew join، جداکردن کلیدهای داغ). سؤال تقریباً قطعی برای سنیور.
- **EN:** Join strategies (broadcast hash, sort-merge, shuffle hash) and when each is chosen. Skew: detecting it in the Spark UI and fixing it (salting, broadcasting, AQE skew join, isolating hot keys). Almost guaranteed senior question.
- **Covers:** autoBroadcastJoinThreshold, join hints, bucketed joins to avoid shuffle
- **Interview Qs:** One task runs 2 hours while others finish in 2 minutes — what do you do? · Join a 5 TB table with a 50 GB table

### 7.7 Adaptive Query Execution & Dynamic Pruning
`spark-aqe` · 🟠 Advanced · P1 · ~2h · Concept
- **FA:** AQE در زمان اجرا plan را بر اساس آمار واقعی تغییر می‌دهد: ادغام partitionهای کوچک، تبدیل join به broadcast، و رفع skew. Dynamic Partition Pruning.
- **EN:** AQE re-plans at runtime using real statistics: coalescing small partitions, switching to broadcast joins, and handling skew. Dynamic partition pruning.

### 7.8 Spark Memory Management & Tuning
`spark-memory-tuning` · 🔴 Senior · P0 · ~5h · Concept
- **FA:** مدل حافظه‌ی unified (execution و storage)، overhead و off-heap، علت‌های OOM در driver و executor، spill، اندازه‌گذاری executor (cores، memory)، و dynamic allocation.
- **EN:** The unified memory model (execution vs storage), overhead and off-heap, causes of driver vs executor OOM, spill, executor sizing (cores, memory), and dynamic allocation.
- **Covers:** collect() dangers, GC tuning, serialization (Kryo), memoryOverhead on Kubernetes
- **Interview Qs:** Executors keep getting OOM-killed on Kubernetes — debug it · How do you size executors for a 2 TB job?

### 7.9 Caching, Persistence & Checkpointing
`spark-caching` · 🟡 Intermediate · P1 · ~1h · Concept
- **FA:** کی cache کنیم و کی نه، سطوح persist، و checkpoint برای قطع lineage طولانی.
- **EN:** When to cache and when not to, storage levels, and checkpointing to truncate long lineage.

### 7.10 Spark UI & Performance Debugging
`spark-ui-debugging` · 🔴 Senior · P0 · ~3h · Practice
- **FA:** خواندن Spark UI: تب‌های Jobs، Stages، SQL، Executors؛ پیدا کردن stage کند، skew، spill و GC. روش سیستماتیک بهینه‌سازی یک جاب کند.
- **EN:** Reading the Spark UI: Jobs, Stages, SQL and Executors tabs; finding slow stages, skew, spill and GC. A systematic method to optimize a slow job.
- **Covers:** History Server, event logs, task metrics distribution (min/median/max)

### 7.11 The Small Files Problem
`small-files` · 🟠 Advanced · P0 · ~2h · Pattern
- **FA:** فایل‌های کوچک زیاد HDFS، Hive و S3 را کند می‌کنند. علت‌ها (partition زیاد، streaming) و راه‌حل‌ها (coalesce، compaction، table formats).
- **EN:** Many small files slow down HDFS, Hive and S3. Causes (over-partitioning, streaming) and fixes (coalesce, compaction, table formats).

### 7.12 Writing Data Reliably from Spark
`spark-writes` · 🟠 Advanced · P0 · ~3h · Concept
- **FA:** حالت‌های ذخیره (append، overwrite، dynamic partition overwrite)، commit protocol و مشکل S3، نوشتن idempotent، bucketing، و نوشتن به JDBC و Kafka و StarRocks.
- **EN:** Save modes (append, overwrite, dynamic partition overwrite), commit protocols and object-store issues, idempotent writes, bucketing, and writing to JDBC, Kafka and StarRocks.
- **Covers:** output committers (S3A magic committer), partial-failure cleanup, table-format atomic commits

### 7.13 Spark on Kubernetes
`spark-on-kubernetes` · 🟠 Advanced · P1 · ~3h · Technology
- **FA:** اجرای Spark روی K8s: spark-submit مستقیم، Spark Operator، Livy، dynamic allocation بدون external shuffle service، مدیریت shuffle و spot/preemption.
- **EN:** Running Spark on Kubernetes: native spark-submit, Spark Operator, Livy, dynamic allocation without an external shuffle service, shuffle storage and preemption.
- **Covers:** pod templates, node selectors, Apache Celeborn / Uniffle remote shuffle, YARN vs K8s trade-offs

### 7.14 Other Batch & Query Engines
`other-batch-engines` · 🟡 Intermediate · P2 · ~3h · Technology
- **FA:** Trino/Presto برای کوئری فدرال، DuckDB برای داده‌ی متوسط روی یک ماشین، و Ray/Dask برای پایتون توزیع‌شده. کی Spark انتخاب اشتباه است.
- **EN:** Trino/Presto for federated queries, DuckDB for medium data on one machine, and Ray/Dask for distributed Python. When Spark is the wrong choice.

### 7.15 Apache SeaTunnel & Data Integration Engines
`seatunnel` · 🟡 Intermediate · P2 · ~2h · Technology
- **FA:** موتورهای یکپارچه‌سازی داده با کانکتورهای آماده روی Spark/Flink/Zeta. مزایا و محدودیت‌ها در برابر نوشتن جاب سفارشی.
- **EN:** Data integration engines with ready connectors on Spark/Flink/Zeta. Pros and limits vs writing custom jobs.

---

# Track 8 — Messaging & Stream Processing | پیام‌رسانی و پردازش جریانی

### 8.1 Queues vs Logs
`queues-vs-logs` · 🟢 Beginner · P0 · ~2h · Concept
- **FA:** تفاوت صف پیام (RabbitMQ) و log توزیع‌شده (Kafka): حذف پس از مصرف در برابر نگهداری و replay، رقابت مصرف‌کننده‌ها در برابر consumer group، ترتیب.
- **EN:** Message queue (RabbitMQ) vs distributed log (Kafka): delete-on-consume vs retention and replay, competing consumers vs consumer groups, ordering.

### 8.2 Kafka Architecture
`kafka-architecture` · 🟡 Intermediate · P0 · ~5h · Technology
- **FA:** broker، topic، partition، segment، offset، replication، leader و follower، ISR، controller و KRaft. چرا Kafka سریع است (نوشتن ترتیبی، page cache، zero-copy، batching).
- **EN:** Brokers, topics, partitions, segments, offsets, replication, leaders and followers, ISR, the controller and KRaft. Why Kafka is fast (sequential I/O, page cache, zero-copy, batching).
- **Interview Qs:** How do you choose the number of partitions? · What happens when a leader broker dies? · min.insync.replicas and acks=all together

### 8.3 Kafka Producers
`kafka-producers` · 🟠 Advanced · P0 · ~3h · Technology
- **FA:** تنظیمات acks، idempotent producer، batching (linger.ms، batch.size)، فشرده‌سازی، partitioner و نقش کلید در ترتیب، retry و مشکل تغییر ترتیب.
- **EN:** acks, idempotent producer, batching (linger.ms, batch.size), compression, the partitioner and key-based ordering, retries and reordering risks.
- **Covers:** max.in.flight.requests, delivery.timeout.ms, hot partitions from skewed keys

### 8.4 Kafka Consumers & Consumer Groups
`kafka-consumers` · 🟠 Advanced · P0 · ~3h · Technology
- **FA:** consumer group و تخصیص partition، rebalance (eager در برابر cooperative، static membership)، commit آفست (خودکار در برابر دستی)، lag و مانیتورینگش، و مشکل پردازش تکراری.
- **EN:** Consumer groups and partition assignment, rebalancing (eager vs cooperative, static membership), offset commits (auto vs manual), lag and monitoring, duplicate processing.
- **Covers:** max.poll.interval.ms, poison pills and DLQ, consumer scaling limits (≤ partitions)

### 8.5 Kafka Exactly-Once & Transactions
`kafka-exactly-once` · 🔴 Senior · P0 · ~3h · Concept
- **FA:** idempotent producer + transaction + read_committed. الگوی read-process-write اتمی، و محدودیت آن وقتی sink خارج از Kafka است.
- **EN:** Idempotent producer + transactions + read_committed. Atomic read-process-write, and its limits when the sink is outside Kafka.
- **Covers:** transactional.id, zombie fencing, EOS in Kafka Streams and Flink

### 8.6 Retention, Compaction & Tiered Storage
`kafka-retention-compaction` · 🟡 Intermediate · P1 · ~2h · Concept
- **FA:** نگهداری بر اساس زمان یا حجم، log compaction برای نگه‌داشتن آخرین مقدار هر کلید (changelog)، tombstone، و tiered storage.
- **EN:** Time- or size-based retention, log compaction to keep the latest value per key (changelogs), tombstones, and tiered storage.

### 8.7 Schema Registry & Serialization
`schema-registry` · 🟠 Advanced · P1 · ~2h · Technology
- **FA:** Schema Registry با Avro یا Protobuf، حالت‌های سازگاری، و جلوگیری از شکستن مصرف‌کننده‌ها. حل مسئله‌ی «producer فیلدی را حذف کرد و پایپ‌لاین شکست».
- **EN:** Schema Registry with Avro or Protobuf, compatibility modes, and protecting consumers. Solving "a producer dropped a field and the pipeline broke."
- **Covers:** Confluent vs Apicurio vs Karapace, subject naming strategies

### 8.8 Kafka Connect & Operations
`kafka-connect-ops` · 🟠 Advanced · P1 · ~4h · Technology
- **FA:** Kafka Connect (source و sink، worker توزیع‌شده، SMT)، MirrorMaker 2 برای تکثیر بین دیتاسنترها، اندازه‌گذاری کلاستر، مانیتورینگ (under-replicated partitions، lag)، و Strimzi روی Kubernetes.
- **EN:** Kafka Connect (source/sink, distributed workers, SMTs), MirrorMaker 2 for cross-DC replication, cluster sizing, monitoring (under-replicated partitions, lag), and Strimzi on Kubernetes.
- **Covers:** rack awareness, partition reassignment, quotas, Cruise Control

### 8.9 Stream Processing Concepts
`stream-concepts` · 🟠 Advanced · P0 · ~6h · Concept
- **FA:** event time در برابر processing time، انواع window (tumbling، sliding/hopping، session، global)، watermark، داده‌ی دیررس (allowed lateness، side output)، trigger. منبع اصلی: کتاب Streaming Systems.
- **EN:** Event time vs processing time, window types (tumbling, sliding/hopping, session, global), watermarks, late data (allowed lateness, side outputs), triggers. Primary source: *Streaming Systems*.
- **Interview Qs:** An event arrives 2 hours late — what happens to your hourly aggregate? · How do you choose the watermark delay?

### 8.10 State Management in Streaming
`streaming-state` · 🔴 Senior · P0 · ~4h · Concept
- **FA:** پردازش stateful: state store، RocksDB، TTL برای state، checkpoint و savepoint، مقیاس‌دهی state و مهاجرت schema آن.
- **EN:** Stateful processing: state stores, RocksDB, state TTL, checkpoints and savepoints, rescaling state and evolving its schema.
- **Covers:** keyed vs operator state, incremental checkpoints, state size growth problems

### 8.11 Apache Flink Deep Dive
`flink` · 🔴 Senior · P0 · ~8h · Technology
- **FA:** معماری (JobManager، TaskManager، slot)، DataStream API و Flink SQL، checkpointing با الگوریتم barrier (Chandy-Lamport)، exactly-once با two-phase commit sink، watermark و window، و Flink Kubernetes Operator.
- **EN:** Architecture (JobManager, TaskManager, slots), DataStream API and Flink SQL, checkpointing via barriers (Chandy-Lamport), exactly-once with two-phase-commit sinks, watermarks and windows, and the Flink Kubernetes Operator.
- **Covers:** operator chaining, parallelism, backpressure monitoring, unaligned checkpoints, Flink CDC
- **Interview Qs:** How does Flink achieve exactly-once? · Flink vs Spark Structured Streaming?

### 8.12 Spark Structured Streaming
`spark-structured-streaming` · 🟠 Advanced · P1 · ~4h · Technology
- **FA:** مدل micro-batch، output modeها (append، update، complete)، watermark و stateful ops، checkpoint location، foreachBatch برای sink سفارشی و idempotent.
- **EN:** The micro-batch model, output modes (append, update, complete), watermarks and stateful operations, checkpoint location, foreachBatch for custom idempotent sinks.

### 8.13 Kafka Streams & ksqlDB
`kafka-streams` · 🟡 Intermediate · P2 · ~3h · Technology
- **FA:** پردازش جریان به‌صورت کتابخانه داخل اپلیکیشن: KStream و KTable، changelog topic، و ksqlDB. مناسب میکروسرویس‌ها.
- **EN:** Stream processing as a library inside your app: KStream and KTable, changelog topics, and ksqlDB. Good fit for microservices.

### 8.14 Stream Joins
`stream-joins` · 🔴 Senior · P1 · ~3h · Concept
- **FA:** join جریان-جریان (با window)، جریان-جدول (enrichment)، temporal join، و lookup join به دیتابیس خارجی. مدیریت state و داده‌ی دیررس در join.
- **EN:** Stream-stream joins (windowed), stream-table joins (enrichment), temporal joins, and lookup joins against external stores. State and late data in joins.

### 8.15 Lambda vs Kappa Architecture
`lambda-kappa` · 🟠 Advanced · P0 · ~2h · Concept
- **FA:** Lambda (لایه‌ی batch + speed) در برابر Kappa (فقط جریان + replay). هزینه‌ی نگهداری دو کدبیس، و معماری‌های امروزی مثل streaming lakehouse.
- **EN:** Lambda (batch + speed layers) vs Kappa (stream-only + replay). The cost of two codebases, and modern approaches like the streaming lakehouse.

### 8.16 Other Messaging Systems
`other-messaging` · 🟡 Intermediate · P2 · ~2h · Technology
- **FA:** RabbitMQ، Pulsar، Redpanda، NATS و کی هرکدام را به Kafka ترجیح می‌دهیم.
- **EN:** RabbitMQ, Pulsar, Redpanda, NATS, and when each beats Kafka.

---

# Track 9 — Data Ingestion & Integration | دریافت و یکپارچه‌سازی داده

### 9.1 Batch Ingestion Patterns
`batch-ingestion` · 🟡 Intermediate · P0 · ~3h · Pattern
- **FA:** بارگذاری کامل در برابر افزایشی، ستون watermark (updated_at، id)، high-water mark، و مشکل حذف‌ها و ردیف‌های دیرآپدیت‌شده در روش query-based.
- **EN:** Full vs incremental loads, watermark columns (updated_at, id), high-water marks, and the deletes / late-updated rows problem in query-based extraction.

### 9.2 Change Data Capture
`cdc` · 🔴 Senior · P0 · ~5h · Concept
- **FA:** CDC مبتنی بر log (Debezium روی binlog در MySQL و WAL در PostgreSQL) در برابر query-based. snapshot اولیه، ترتیب، تغییر schema، و ساخت جدول آینه‌ای در انبار با upsert.
- **EN:** Log-based CDC (Debezium on MySQL binlog and PostgreSQL WAL) vs query-based. Initial snapshots, ordering, schema changes, and building mirror tables in the warehouse via upserts.
- **Covers:** Debezium on Kafka Connect, Flink CDC, outbox events, replication slot risks (WAL bloat), Oracle CDC options (common in banks)
- **Interview Qs:** Design CDC from 50 microservice databases to the warehouse · How do you handle deletes?

### 9.3 File-Based Ingestion
`file-ingestion` · 🟠 Advanced · P0 · ~3h · Pattern
- **FA:** دریافت فایل از SFTP یا object storage: landing zone، manifest و فایل علامت پایان، ثبت فایل‌های پردازش‌شده برای جلوگیری از بارگذاری تکراری، فایل‌های خراب و ناقص، و فرمت‌های باینری (مثل ASN.1 در اپراتورها).
- **EN:** Ingesting files from SFTP or object storage: landing zones, manifests and done-markers, tracking processed files to avoid reloads, corrupt and partial files, and binary formats (e.g., ASN.1 CDRs in telcos).
- **Covers:** file registry table, checksum, archive/quarantine folders, event-driven vs polling

### 9.4 API Ingestion
`api-ingestion` · 🟡 Intermediate · P1 · ~2h · Pattern
- **FA:** دریافت از API: pagination (offset، cursor)، rate limit، retry، احراز هویت، و بارگذاری افزایشی از API.
- **EN:** Ingesting from APIs: pagination (offset, cursor), rate limits, retries, authentication, and incremental loads from APIs.

### 9.5 ETL vs ELT
`etl-vs-elt` · 🟢 Beginner · P0 · ~1h · Concept
- **FA:** تبدیل قبل از بارگذاری یا بعد از آن. چرا ELT با انبارهای قدرتمند رایج شد و کی ETL هنوز بهتر است (پاک‌سازی PII، داده‌ی باینری).
- **EN:** Transform before loading or after. Why ELT became common with powerful warehouses, and when ETL is still better (PII scrubbing, binary data).

### 9.6 Ingestion Tooling Landscape
`ingestion-tools` · 🟡 Intermediate · P1 · ~3h · Technology
- **FA:** ابزارها و کاربردشان: Apache NiFi (رایج در سازمان‌ها)، Kafka Connect، Debezium، Airbyte، SeaTunnel، Logstash/Fluent Bit/Vector برای لاگ.
- **EN:** Tools and when to use them: Apache NiFi (common in enterprises), Kafka Connect, Debezium, Airbyte, SeaTunnel, Logstash/Fluent Bit/Vector for logs.

### 9.7 Dead Letter Queues & Bad Records
`dlq-bad-records` · 🟠 Advanced · P0 · ~2h · Pattern
- **FA:** با رکورد خراب چه کنیم: متوقف‌کردن پایپ‌لاین یا ادامه دادن. الگوی DLQ و quarantine، ثبت دلیل خطا، و reprocess.
- **EN:** What to do with bad records: halt the pipeline or continue. The DLQ/quarantine pattern, recording error reasons, and reprocessing.

### 9.8 Deduplication & Idempotent Loads
`dedup-idempotent-loads` · 🔴 Senior · P0 · ~3h · Pattern
- **FA:** چرا تکرار اجتناب‌ناپذیر است (retry، at-least-once) و حذفش در هر لایه: کلید یکتای رویداد، upsert، ReplacingMergeTree در ClickHouse، Primary Key table در StarRocks، و dedup در Flink با state.
- **EN:** Why duplicates are inevitable (retries, at-least-once) and removing them at each layer: event IDs, upserts, ClickHouse ReplacingMergeTree, StarRocks primary-key tables, and stateful dedup in Flink.

---

# Track 10 — Warehouses, Lakes & Lakehouse | انبار، دریاچه و لیک‌هاوس

### 10.1 Data Warehouse Fundamentals
`dwh-fundamentals` · 🟢 Beginner · P0 · ~2h · Concept
- **FA:** انبار داده چیست، لایه‌ها (staging، integration، mart)، و تفاوت آن با دیتابیس عملیاتی.
- **EN:** What a data warehouse is, its layers (staging, integration, marts), and how it differs from operational databases.

### 10.2 Data Lakes & Object Storage
`data-lake-object-storage` · 🟡 Intermediate · P0 · ~3h · Concept
- **FA:** دریاچه‌ی داده روی object storage (S3، MinIO، Ceph RGW) یا HDFS. تفاوت HDFS و object store (rename اتمی، consistency، list)، و مشکل «باتلاق داده».
- **EN:** Data lakes on object storage (S3, MinIO, Ceph RGW) or HDFS. HDFS vs object store (atomic rename, consistency, listing), and the "data swamp" problem.
- **Covers:** S3 API on-prem, erasure coding vs replication, storage-compute separation

### 10.3 Lakehouse & Open Table Formats
`lakehouse-table-formats` · 🔴 Senior · P0 · ~6h · Technology
- **FA:** چرا table format لازم شد: ACID روی فایل، time travel، تکامل schema. Apache Iceberg (snapshot، manifest، hidden partitioning، compaction)، Delta Lake، Hudi (Copy-on-Write و Merge-on-Read). مقایسه و انتخاب.
- **EN:** Why table formats emerged: ACID on files, time travel, schema evolution. Apache Iceberg (snapshots, manifests, hidden partitioning, compaction), Delta Lake, Hudi (Copy-on-Write vs Merge-on-Read). Comparison and selection.
- **Covers:** MERGE INTO and row-level deletes, equality vs position deletes, snapshot expiration, optimistic concurrency
- **Interview Qs:** How does Iceberg give atomic commits on S3? · CoW vs MoR for a CDC-heavy table?

### 10.4 Catalogs & Metastores
`catalogs` · 🟠 Advanced · P1 · ~2h · Technology
- **FA:** Hive Metastore، REST catalog، Nessie، Polaris، Unity Catalog. نقش catalog در lakehouse و دسترسی چند موتور به یک جدول.
- **EN:** Hive Metastore, REST catalogs, Nessie, Polaris, Unity Catalog. The catalog's role in the lakehouse and multi-engine access to one table.

### 10.5 Medallion Architecture
`medallion-architecture` · 🟡 Intermediate · P0 · ~2h · Pattern
- **FA:** لایه‌های Bronze (خام)، Silver (تمیز و یکپارچه)، Gold (آماده‌ی کسب‌وکار). قرارداد هر لایه و تفاوتش با لایه‌بندی کلاسیک انبار.
- **EN:** Bronze (raw), Silver (cleaned, conformed), Gold (business-ready) layers. Each layer's contract and how it compares to classic warehouse layering.

### 10.6 Partitioning, File Layout & Compaction
`file-layout` · 🔴 Senior · P0 · ~3h · Concept
- **FA:** انتخاب کلید partition، اندازه‌ی ایده‌آل فایل، compaction، sorting، Z-order و clustering برای data skipping. اشتباه رایج: partition بیش از حد.
- **EN:** Choosing partition keys, ideal file sizes, compaction, sorting, Z-ordering and clustering for data skipping. Common mistake: over-partitioning.

### 10.7 ClickHouse Deep Dive
`clickhouse` · 🔴 Senior · P0 · ~8h · Technology
- **FA:** رایج‌ترین موتور تحلیلی در شرکت‌های محصولی ایرانی. خانواده‌ی MergeTree (Replacing، Summing، Aggregating، Collapsing)، ORDER BY و primary index پراکنده، partition، merge در پس‌زمینه، materialized view به‌عنوان trigger درج، ReplicatedMergeTree و Keeper، جدول Distributed، و Kafka engine.
- **EN:** The most common analytical engine in Iranian product companies. The MergeTree family (Replacing, Summing, Aggregating, Collapsing), ORDER BY and the sparse primary index, partitions, background merges, materialized views as insert triggers, ReplicatedMergeTree and Keeper, Distributed tables, and the Kafka engine.
- **Covers:** too many parts error, insert batching, FINAL cost, projections, skipping indexes, TTL, dictionaries, JOIN limitations
- **Interview Qs:** Why should you not insert row by row? · How does ReplacingMergeTree dedup and why isn't it immediate?

### 10.8 StarRocks / Apache Doris Deep Dive
`starrocks-doris` · 🔴 Senior · P1 · ~6h · Technology
- **FA:** معماری FE و BE (و CN در حالت shared-data)، مدل‌های جدول (Duplicate، Aggregate، Unique، Primary Key)، materialized view همگام و ناهمگام، بارگذاری (Stream Load، Routine Load از Kafka، Broker Load)، external catalog روی Iceberg/Hive، و عملیات کلاستر (replica، tablet، compaction).
- **EN:** FE and BE architecture (plus CN in shared-data mode), table models (Duplicate, Aggregate, Unique, Primary Key), sync and async materialized views, loading (Stream Load, Routine Load from Kafka, Broker Load), external catalogs on Iceberg/Hive, and cluster ops (replicas, tablets, compaction).
- **Covers:** ClickHouse vs StarRocks trade-offs (joins, updates), colocate groups, query profile

### 10.9 Real-Time OLAP: Druid & Pinot
`realtime-olap` · 🟠 Advanced · P2 · ~3h · Technology
- **FA:** موتورهای تحلیل بلادرنگ با ingestion مستقیم از Kafka و تأخیر زیر ثانیه برای داشبوردهای کاربرمحور. مقایسه با ClickHouse.
- **EN:** Real-time analytics engines with direct Kafka ingestion and sub-second latency for user-facing dashboards. Comparison with ClickHouse.

### 10.10 Federated Query Engines
`trino` · 🟠 Advanced · P1 · ~3h · Technology
- **FA:** Trino برای کوئری روی چند منبع (Hive، Iceberg، Postgres) بدون جابه‌جایی داده. معماری coordinator/worker، connectorها، و محدودیت‌ها (spill، تحمل خرابی).
- **EN:** Trino for querying across sources (Hive, Iceberg, Postgres) without moving data. Coordinator/worker architecture, connectors, and limits (spill, fault tolerance).

### 10.11 Cloud Warehouse Concepts
`cloud-warehouses` · 🟡 Intermediate · P2 · ~2h · Concept
- **FA:** Snowflake، BigQuery، Redshift در حد مفهوم: جداسازی ذخیره و پردازش، micro-partition، pricing. حتی بدون دسترسی به کلاد، این مفاهیم در کتاب‌ها و مصاحبه‌ها می‌آیند.
- **EN:** Snowflake, BigQuery, Redshift at a conceptual level: storage-compute separation, micro-partitions, pricing models. Even without cloud access, these concepts appear in books and interviews.

### 10.12 Serving Layers
`serving-layer` · 🟠 Advanced · P1 · ~3h · Pattern
- **FA:** رساندن داده‌ی پردازش‌شده به اپلیکیشن: Redis برای lookup سریع، Elasticsearch برای جستجو، OLAP برای داشبورد، و reverse ETL به سیستم‌های عملیاتی.
- **EN:** Delivering processed data to applications: Redis for fast lookups, Elasticsearch for search, OLAP for dashboards, and reverse ETL into operational systems.
- **Covers:** pre-aggregation vs on-the-fly, cache invalidation, Elasticsearch as analytics (and its limits)

### 10.13 Time-Series Databases
`time-series-db` · 🟡 Intermediate · P2 · ~2h · Technology
- **FA:** Prometheus، VictoriaMetrics، TimescaleDB، InfluxDB؛ downsampling، retention و cardinality بالا.
- **EN:** Prometheus, VictoriaMetrics, TimescaleDB, InfluxDB; downsampling, retention and high cardinality.

---

# Track 11 — Orchestration & Transformation | ارکستراسیون و تبدیل داده

### 11.1 Orchestration Concepts
`orchestration-concepts` · 🟢 Beginner · P0 · ~2h · Concept
- **FA:** DAG، زمان‌بندی، وابستگی بین تسک‌ها، sensor، SLA، retry و هشدار. تفاوت scheduler با orchestrator.
- **EN:** DAGs, scheduling, task dependencies, sensors, SLAs, retries and alerting. Scheduler vs orchestrator.

### 11.2 Apache Airflow Deep Dive
`airflow` · 🔴 Senior · P0 · ~6h · Technology
- **FA:** معماری (scheduler، webserver/API server، executor، metadata DB، worker)، انواع executor (Local، Celery، Kubernetes)، operator و hook، XCom و محدودیتش، catchup و backfill، data interval و logical date، dataset/asset scheduling، dynamic task mapping، و تغییرات Airflow 3.
- **EN:** Architecture (scheduler, webserver/API server, executor, metadata DB, workers), executor types (Local, Celery, Kubernetes), operators and hooks, XCom and its limits, catchup and backfill, data intervals and logical dates, dataset/asset scheduling, dynamic task mapping, and Airflow 3 changes.
- **Covers:** top-level code anti-pattern, idempotent tasks with templated dates, pools, KubernetesPodOperator, scaling the scheduler
- **Interview Qs:** Why should a task not use `datetime.now()`? · How do you backfill 6 months safely?

### 11.3 Alternative Orchestrators
`orchestrator-alternatives` · 🟡 Intermediate · P2 · ~2h · Technology
- **FA:** Dagster (مدل asset-محور)، Prefect، Argo Workflows روی Kubernetes، و DolphinScheduler (محبوب در اکوسیستم چینی و برخی شرکت‌ها). مقایسه با Airflow.
- **EN:** Dagster (asset-centric), Prefect, Argo Workflows on Kubernetes, and DolphinScheduler. Comparison with Airflow.

### 11.4 dbt Deep Dive
`dbt` · 🟠 Advanced · P0 · ~5h · Technology
- **FA:** تبدیل داده با SQL و مهندسی نرم‌افزار: model، ref و source، materializationها (view، table، incremental، ephemeral)، تست‌ها، snapshot برای SCD2، macro و Jinja، مستندسازی و lineage. adapterها برای ClickHouse، StarRocks، Spark و Trino.
- **EN:** SQL transformation with software engineering: models, ref and source, materializations (view, table, incremental, ephemeral), tests, snapshots for SCD2, macros and Jinja, docs and lineage. Adapters for ClickHouse, StarRocks, Spark and Trino.
- **Covers:** incremental strategies (append, merge, delete+insert, insert_overwrite), model layering (staging → intermediate → marts), CI with slim builds

### 11.5 Pipeline Design Principles
`pipeline-design-principles` · 🔴 Senior · P0 · ~3h · Pattern
- **FA:** اصول طلایی: idempotency، determinism، اجرای مجدد امن، پردازش بر اساس partition زمانی، جدا کردن compute از state، functional data engineering (بازنویسی partition به‌جای mutate).
- **EN:** Golden rules: idempotency, determinism, safe re-runs, partition-based processing, separating compute from state, functional data engineering (overwrite partitions instead of mutating).
- **Covers:** Maxime Beauchemin's "Functional Data Engineering", immutable raw layer, reproducibility

### 11.6 Backfills & Reprocessing
`backfills` · 🔴 Senior · P0 · ~2h · Pattern
- **FA:** پر کردن داده‌ی گذشته یا اصلاح باگ تاریخی بدون آسیب به اجرای روزانه: موازی‌سازی، کنترل منابع، نسخه‌بندی کد، و replay از Kafka یا لایه‌ی خام.
- **EN:** Filling historical data or fixing historical bugs without disrupting daily runs: parallelization, resource control, code versioning, and replay from Kafka or the raw layer.

---

# Track 12 — Data Quality, Observability & Governance | کیفیت، مشاهده‌پذیری و حاکمیت داده

### 12.1 Data Quality Dimensions & Testing
`data-quality` · 🟡 Intermediate · P0 · ~3h · Concept
- **FA:** ابعاد کیفیت: کامل‌بودن، یکتایی، اعتبار، سازگاری، به‌موقع‌بودن، دقت. ابزارها: dbt tests، Great Expectations، Soda. کجای پایپ‌لاین تست بگذاریم و کی پایپ‌لاین را متوقف کنیم.
- **EN:** Quality dimensions: completeness, uniqueness, validity, consistency, timeliness, accuracy. Tools: dbt tests, Great Expectations, Soda. Where to place tests and when to halt the pipeline.
- **Covers:** write-audit-publish pattern, circuit breakers for data, reconciliation (source vs target counts)

### 12.2 Data Observability
`data-observability` · 🟠 Advanced · P1 · ~2h · Concept
- **FA:** پنج ستون: freshness، volume، schema، distribution، lineage. تشخیص ناهنجاری و هشدار هوشمند.
- **EN:** Five pillars: freshness, volume, schema, distribution, lineage. Anomaly detection and smart alerting.

### 12.3 Pipeline Monitoring & Data SLAs
`pipeline-monitoring` · 🟠 Advanced · P0 · ~3h · Concept
- **FA:** مانیتورینگ با Prometheus و Grafana، متریک‌های کلیدی (Kafka lag، مدت جاب، رکورد پردازش‌شده)، تعریف SLA/SLO برای تازگی داده، on-call و postmortem.
- **EN:** Monitoring with Prometheus and Grafana, key metrics (Kafka lag, job duration, records processed), freshness SLAs/SLOs, on-call and postmortems.

### 12.4 Data Lineage
`data-lineage` · 🟡 Intermediate · P1 · ~2h · Concept
- **FA:** ردیابی منشأ داده در سطح جدول و ستون، OpenLineage و Marquez، و کاربرد در تحلیل اثر تغییرات.
- **EN:** Tracking data provenance at table and column level, OpenLineage and Marquez, and impact analysis for changes.

### 12.5 Metadata & Data Catalogs
`data-catalogs` · 🟡 Intermediate · P1 · ~2h · Technology
- **FA:** OpenMetadata، DataHub، Amundsen؛ کشف داده، مالکیت، glossary کسب‌وکار.
- **EN:** OpenMetadata, DataHub, Amundsen; data discovery, ownership, business glossary.

### 12.6 Data Governance, Contracts & Data Mesh
`governance-contracts-mesh` · 🔴 Senior · P1 · ~3h · Concept
- **FA:** حاکمیت داده: مالکیت، سیاست‌ها، کیفیت. Data Contract بین تولیدکننده و مصرف‌کننده. Data Mesh (دامنه‌محور، داده به‌عنوان محصول، پلتفرم self-serve، حاکمیت فدرال) و نقدهای آن.
- **EN:** Data governance: ownership, policies, quality. Data contracts between producers and consumers. Data Mesh (domain ownership, data as a product, self-serve platform, federated governance) and its criticisms.

### 12.7 Security, Privacy & Access Control
`data-security` · 🟠 Advanced · P1 · ~3h · Concept
- **FA:** RBAC و ABAC، Apache Ranger، row-level و column-level security، masking و tokenization داده‌ی حساس (کد ملی، شماره‌ی کارت، تلفن)، رمزنگاری در حالت سکون و انتقال، و audit. برای بانک‌ها و اپراتورها بسیار مهم است.
- **EN:** RBAC and ABAC, Apache Ranger, row- and column-level security, masking and tokenizing sensitive data (national ID, card numbers, phone numbers), encryption at rest and in transit, and auditing. Critical for banks and telcos.
- **Covers:** Kerberos in Hadoop, Keycloak/OIDC integration, PII discovery, right-to-delete in immutable lakes

### 12.8 Data Lifecycle & Retention
`data-retention` · 🟡 Intermediate · P2 · ~1h · Concept
- **FA:** سیاست نگهداری، داده‌ی گرم/ولرم/سرد، آرشیو، TTL، و الزامات قانونی نگهداری.
- **EN:** Retention policies, hot/warm/cold tiers, archiving, TTLs, and regulatory retention requirements.

---

# Track 13 — Infrastructure & Platform | زیرساخت و پلتفرم

### 13.1 Containers & Docker
`docker` · 🟢 Beginner · P1 · ~2h · Technology
- **FA:** ایمیج، کانتینر، لایه‌ها، Docker Compose برای محیط توسعه‌ی محلی پایپ‌لاین (Kafka + Postgres + Spark).
- **EN:** Images, containers, layers, Docker Compose for local pipeline dev environments (Kafka + Postgres + Spark).

### 13.2 Kubernetes for Data Workloads
`kubernetes-for-data` · 🟠 Advanced · P1 · ~5h · Technology
- **FA:** StatefulSet و Persistent Volume برای سیستم‌های stateful، Operatorها (Strimzi برای Kafka، Spark Operator، Flink Operator، ClickHouse Operator)، مدیریت منابع (requests/limits)، و چالش‌های اجرای دیتابیس روی K8s.
- **EN:** StatefulSets and persistent volumes for stateful systems, operators (Strimzi for Kafka, Spark Operator, Flink Operator, ClickHouse Operator), resource management (requests/limits), and challenges of running databases on Kubernetes.
- **Covers:** local PV vs Ceph RBD for Kafka/ClickHouse, pod anti-affinity, YuniKorn/Volcano for batch scheduling

### 13.3 IaC, CI/CD & DataOps
`dataops-cicd` · 🟡 Intermediate · P1 · ~3h · Concept
- **FA:** زیرساخت به‌عنوان کد (Terraform، Ansible، Helm)، GitOps با ArgoCD، CI/CD برای پایپ‌لاین داده (تست، محیط staging، استقرار DAG)، و Blue/Green برای جدول‌ها.
- **EN:** Infrastructure as code (Terraform, Ansible, Helm), GitOps with ArgoCD, CI/CD for data pipelines (tests, staging environments, DAG deployment), and blue/green for tables.

### 13.4 On-Premise Data Platforms in Iran
`onprem-iran` · 🟠 Advanced · P1 · ~2h · Guide
- **FA:** واقعیت‌های ایران: نبود کلاد عمومی، محدودیت سخت‌افزار و قیمت دیسک، نیاز به mirror داخلی برای Maven/PyPI/Docker، کلاد‌های داخلی، و طراحی با توجه به هزینه‌ی سخت‌افزار. این‌ها را در System Design مطرح کن تا نشان دهی واقع‌بین هستی.
- **EN:** Iranian realities: no public cloud, constrained hardware and disk pricing, need for internal mirrors for Maven/PyPI/Docker, domestic cloud providers, and designing around hardware cost. Raise these in system design to show realism.

### 13.5 Capacity Planning & Cost
`capacity-planning` · 🔴 Senior · P0 · ~3h · Concept
- **FA:** تخمین CPU، RAM، دیسک و شبکه برای Kafka، Spark و ClickHouse؛ ضریب replication، رشد سالانه، headroom. سؤال سنیور: «برای این حجم چند سرور لازم داری؟»
- **EN:** Estimating CPU, RAM, disk and network for Kafka, Spark and ClickHouse; replication factor, annual growth, headroom. Senior question: "how many servers do you need for this volume?"

### 13.6 YARN vs Kubernetes
`yarn-vs-k8s` · 🟡 Intermediate · P2 · ~1h · Concept
- **FA:** مقایسه‌ی دو مدیر منابع برای جاب‌های داده: صف‌ها و data locality در YARN در برابر انعطاف و یکپارچگی K8s.
- **EN:** Comparing the two resource managers for data jobs: YARN queues and data locality vs Kubernetes flexibility and unification.

---

# Track 14 — Data System Design | طراحی سیستم داده

> **FA:** مهم‌ترین بخش برای سنیور. معادل «Patterns» و «Problem Breakdowns» در HelloInterview. هر مسئله در سایت شامل: نیازمندی‌ها، تخمین، مدل داده، معماری، Deep Diveها، انتظارات هر سطح (Mid / Senior / Staff)، و سؤال‌های پیگیری مصاحبه‌گر خواهد بود.
> **EN:** The most important part for senior. Equivalent to HelloInterview's "Patterns" and "Problem Breakdowns". Each problem page will include: requirements, estimation, data model, architecture, deep dives, per-level expectations (Mid / Senior / Staff), and interviewer follow-ups.

## 14A — Core Building Blocks | اجزای پایه

### 14.1 Back-of-Envelope Estimation for Data
`data-estimation` · 🟡 Intermediate · P0 · ~3h · Practice
- **FA:** تبدیل «۱۰ میلیون کاربر فعال روزانه» به رویداد بر ثانیه، حجم روزانه، تعداد partition و فضای سالانه. تمرین تا سرعتت زیر ۳ دقیقه شود.
- **EN:** Converting "10M daily active users" into events/sec, daily volume, partition count and yearly storage. Drill until it takes under 3 minutes.

### 14.2 Reference Architecture of a Modern Data Platform
`reference-architecture` · 🟡 Intermediate · P0 · ~3h · Concept
- **FA:** نقشه‌ی کامل یک پلتفرم داده: منابع → ingestion (batch، CDC، stream) → ذخیره (lake/lakehouse) → پردازش (Spark، Flink) → سرو (OLAP، cache، API) → مصرف (BI، ML، اپ). به‌علاوه‌ی لایه‌های افقی: ارکستراسیون، کیفیت، catalog، امنیت، مانیتورینگ.
- **EN:** The full map of a data platform: sources → ingestion (batch, CDC, stream) → storage (lake/lakehouse) → processing (Spark, Flink) → serving (OLAP, cache, API) → consumption (BI, ML, apps). Plus horizontal layers: orchestration, quality, catalog, security, monitoring.

### 14.3 Technology Selection & Trade-offs
`tech-selection` · 🔴 Senior · P0 · ~3h · Concept
- **FA:** جدول‌های تصمیم: Kafka یا RabbitMQ، Flink یا Spark Streaming، ClickHouse یا StarRocks یا Druid، Iceberg یا Hudi یا Delta، Airflow یا Dagster، batch یا stream. سنیور باید معیار انتخاب داشته باشد، نه اسم ابزار.
- **EN:** Decision tables: Kafka vs RabbitMQ, Flink vs Spark Streaming, ClickHouse vs StarRocks vs Druid, Iceberg vs Hudi vs Delta, Airflow vs Dagster, batch vs stream. Seniors bring selection criteria, not tool names.

## 14B — Patterns | الگوها

### P1. Batch ETL Pipeline
`pattern-batch-etl` · 🟡 Intermediate · P0 · ~2h · Pattern
- **FA:** پایپ‌لاین روزانه‌ی partition‌محور، idempotent، با لایه‌ی خام تغییرناپذیر و بازنویسی partition.
- **EN:** A daily, partition-based, idempotent pipeline with an immutable raw layer and partition overwrites.

### P2. Real-Time Aggregation
`pattern-realtime-aggregation` · 🟠 Advanced · P0 · ~3h · Pattern
- **FA:** شمارش و تجمیع بلادرنگ با window: pre-aggregation در Flink، یا ingestion خام به ClickHouse با materialized view. انتخاب بین دو رویکرد.
- **EN:** Real-time counts and aggregates with windows: pre-aggregation in Flink, or raw ingestion into ClickHouse with materialized views. Choosing between the two.

### P3. Late & Out-of-Order Data
`pattern-late-data` · 🔴 Senior · P0 · ~2h · Pattern
- **FA:** watermark، allowed lateness، به‌روزرسانی نتایج قبلی، و آشتی‌دادن خروجی بلادرنگ با batch شبانه.
- **EN:** Watermarks, allowed lateness, updating earlier results, and reconciling real-time output with nightly batch.

### P4. End-to-End Exactly-Once
`pattern-exactly-once` · 🔴 Senior · P0 · ~3h · Pattern
- **FA:** طراحی انتها-به-انتها از منبع تا sink بدون تکرار و از دست رفتن داده: منبع replayable، پردازش با checkpoint، sink تراکنشی یا idempotent.
- **EN:** End-to-end design from source to sink with no loss or duplicates: replayable source, checkpointed processing, transactional or idempotent sink.

### P5. CDC to Analytics
`pattern-cdc-analytics` · 🔴 Senior · P0 · ~3h · Pattern
- **FA:** از دیتابیس عملیاتی تا انبار: Debezium → Kafka → Flink/Spark → Iceberg/StarRocks با upsert. snapshot اولیه، تغییر schema، و حذف‌ها.
- **EN:** From operational DB to warehouse: Debezium → Kafka → Flink/Spark → Iceberg/StarRocks with upserts. Initial snapshot, schema change, and deletes.

### P6. Deduplication at Scale
`pattern-dedup` · 🔴 Senior · P0 · ~2h · Pattern
- **FA:** حذف تکرار روی میلیاردها رویداد: dedup با state و TTL، Bloom filter، dedup در sink، و trade-off بین دقت و هزینه.
- **EN:** Deduplicating billions of events: stateful dedup with TTL, Bloom filters, sink-side dedup, and the accuracy vs cost trade-off.

### P7. Backfill & Reprocessing
`pattern-backfill` · 🔴 Senior · P0 · ~2h · Pattern
- **FA:** بازپردازش داده‌ی گذشته در سیستم batch و stream بدون اختلال در جریان زنده.
- **EN:** Reprocessing historical data in batch and streaming systems without disrupting live flow.

### P8. History & Point-in-Time Correctness
`pattern-history` · 🟠 Advanced · P1 · ~2h · Pattern
- **FA:** نگه‌داشتن تاریخچه (SCD2، snapshot روزانه، time travel) و جواب‌دادن به سؤال «در تاریخ X وضعیت چه بود».
- **EN:** Keeping history (SCD2, daily snapshots, time travel) and answering "what was the state at time X?"

### P9. Hot Keys & Skew
`pattern-hot-keys` · 🔴 Senior · P0 · ~2h · Pattern
- **FA:** کلید داغ در Kafka، Spark، Flink و دیتابیس: salting، تجمیع دومرحله‌ای، جداکردن کلیدهای بزرگ.
- **EN:** Hot keys in Kafka, Spark, Flink and databases: salting, two-phase aggregation, isolating heavy keys.

### P10. Serving Low-Latency Analytics
`pattern-serving` · 🟠 Advanced · P1 · ~2h · Pattern
- **FA:** داشبورد کاربرمحور با هزاران کاربر هم‌زمان: pre-aggregation، cache، OLAP بلادرنگ، و محدودیت هم‌زمانی.
- **EN:** User-facing dashboards with thousands of concurrent users: pre-aggregation, caching, real-time OLAP, and concurrency limits.

### P11. Schema Evolution in Pipelines
`pattern-schema-evolution` · 🟠 Advanced · P1 · ~2h · Pattern
- **FA:** پذیرفتن تغییر schema از منبع بدون شکستن پایپ‌لاین: registry، قرارداد، ستون‌های انعطاف‌پذیر، و هشدار.
- **EN:** Absorbing source schema changes without breaking the pipeline: registry, contracts, flexible columns, and alerting.

### P12. Multi-Tenant Data Platform
`pattern-multi-tenant` · 🔴 Senior · P2 · ~2h · Pattern
- **FA:** پلتفرم مشترک برای چند تیم: ایزوله‌سازی منابع، quota، دسترسی، و self-service.
- **EN:** A shared platform for many teams: resource isolation, quotas, access control, and self-service.

### P13. Lambda vs Kappa in Practice
`pattern-lambda-kappa` · 🟠 Advanced · P1 · ~1h · Pattern
- **FA:** کی دو مسیر batch و stream نگه داریم و کی همه را با stream و replay حل کنیم.
- **EN:** When to keep separate batch and stream paths and when to solve everything with streaming plus replay.

### P14. Data Deletion & Privacy in Immutable Stores
`pattern-data-deletion` · 🔴 Senior · P2 · ~2h · Pattern
- **FA:** حذف داده‌ی یک کاربر از Kafka، lake و انبار: compaction با tombstone، row-level delete در Iceberg، crypto-shredding.
- **EN:** Deleting a user's data from Kafka, the lake and the warehouse: compaction with tombstones, Iceberg row-level deletes, crypto-shredding.

### P15. Reconciliation
`pattern-reconciliation` · 🟠 Advanced · P1 · ~2h · Pattern
- **FA:** تطبیق داده بین دو سیستم (مثلاً تراکنش‌های بانک با سوئیچ پرداخت یا CDR با سیستم صورتحساب) و گزارش اختلاف.
- **EN:** Matching data between two systems (e.g., bank transactions vs payment switch, or CDRs vs billing) and reporting discrepancies.

## 14C — Problem Breakdowns | مسائل کامل مصاحبه

> **FA:** هر مسئله در سایت پاسخ تشریحی کامل خواهد داشت. ستاره‌ها اولویت را نشان می‌دهند.
> **EN:** Every problem will get a full worked solution on the site. Stars show priority.

| # | Problem | `id` | Level | Priority | Key deep dives |
|---|---|---|---|---|---|
| 1 | **Ad Click Aggregator** | `design-ad-click-aggregator` | 🔴 | ★★★ P0 | streaming aggregation, dedup, exactly-once, reconciliation with batch |
| 2 | **Ride-Hailing Real-Time Analytics** (surge, supply/demand) | `design-ride-hailing-analytics` | 🔴 | ★★★ P0 | geo bucketing, windowing, late GPS events, serving layer |
| 3 | **E-commerce Data Warehouse** (orders, inventory, returns) | `design-ecommerce-dwh` | 🟠 | ★★★ P0 | dimensional model, CDC, SCD2, daily marts |
| 4 | **Telco CDR Processing & Billing Analytics** | `design-telco-cdr` | 🔴 | ★★★ P0 | file ingestion, ASN.1 decoding, 3TB/day sizing, exactly-once loads, reconciliation |
| 5 | **Real-Time Fraud Detection for Banking** | `design-fraud-detection` | 🔴 | ★★★ P0 | stream-table enrichment, stateful rules, feature freshness, latency budget |
| 6 | **Clickstream / Product Analytics Platform** (funnels, retention) | `design-clickstream-analytics` | 🟠 | ★★★ P0 | event schema, sessionization, ClickHouse modeling |
| 7 | **CDC-Based Data Lake for Microservices** | `design-cdc-lake` | 🔴 | ★★★ P0 | Debezium, Iceberg upserts, schema evolution, deletes |
| 8 | **Top-K Trending** (products / hashtags / searches) | `design-top-k` | 🟠 | ★★ P1 | Count-Min sketch, heap, windowed top-K, accuracy trade-offs |
| 9 | **Metrics & Monitoring System** | `design-metrics-monitoring` | 🟠 | ★★ P1 | time-series storage, downsampling, high cardinality |
| 10 | **Log Aggregation Platform** | `design-log-platform` | 🟠 | ★★ P1 | collectors, Kafka buffer, Elasticsearch/ClickHouse, retention |
| 11 | **Unique Visitors Dashboard** (DAU/MAU at scale) | `design-unique-counts` | 🟠 | ★★ P1 | HyperLogLog, rollups, exact vs approximate |
| 12 | **Feature Store / Recommendation Feature Pipeline** | `design-feature-store` | 🔴 | ★★ P1 | online/offline consistency, point-in-time joins, backfill |
| 13 | **Payment Reconciliation Pipeline** | `design-payment-reconciliation` | 🟠 | ★★ P1 | matching keys, late records, exception workflow |
| 14 | **Data Platform From Scratch** (company with no platform) | `design-platform-from-scratch` | 🔴 | ★★ P1 | prioritization, phased roadmap, build vs buy, team |
| 15 | **Hadoop → Lakehouse Migration** | `design-hadoop-migration` | 🔴 | ★★ P1 | dual-run, validation, cutover, catalog migration |
| 16 | **Search Indexing Pipeline** | `design-search-indexing` | 🟠 | ★ P2 | CDC to Elasticsearch, reindexing, Persian text analyzers |
| 17 | **Real-Time Leaderboard** | `design-leaderboard` | 🟡 | ★ P2 | Redis sorted sets, windowed resets |
| 18 | **Notification / Campaign Analytics** | `design-campaign-analytics` | 🟠 | ★ P2 | attribution windows, joins across event streams |
| 19 | **Web Crawler Data Pipeline** | `design-crawler-pipeline` | 🟠 | ★ P2 | frontier queue, dedup by URL/content hash, storage |
| 20 | **GDPR-Style User Deletion Across the Platform** | `design-user-deletion` | 🔴 | ★ P2 | lineage-driven deletion, immutable stores |
| 21 | **IoT / Sensor Data Platform** | `design-iot-platform` | 🟠 | ★ P2 | high-frequency ingestion, time-series, edge buffering |
| 22 | **Data Quality Platform** | `design-dq-platform` | 🔴 | ★ P2 | checks as code, anomaly detection, alert routing |

**Standard page template for each problem | قالب استاندارد صفحه‌ی هر مسئله**
1. Problem statement — صورت مسئله
2. Functional requirements — نیازمندی‌های کارکردی
3. Non-functional requirements — نیازمندی‌های غیرکارکردی
4. Estimation — تخمین حجم
5. Data model — مدل داده
6. High-level design (diagram) — طراحی سطح بالا
7. Deep dives — بررسی‌های عمیق
8. What is expected at each level (Mid / Senior / Staff) — انتظارات هر سطح
9. Interviewer follow-ups & curveballs — سؤال‌های پیگیری
10. Common mistakes — اشتباهات رایج
11. Quiz — آزمونک

## 14D — In the Wild (Case Studies) | مطالعه‌ی موردی

`case-studies` · 🟠 Advanced · P1 · ~8h · Concept
- **FA:** خواندن مقالات مهندسی شرکت‌های بزرگ برای داشتن مثال واقعی در مصاحبه. هر مطالعه در سایت یک خلاصه‌ی فارسی و درس‌های کلیدی خواهد داشت.
- **EN:** Reading engineering write-ups from large companies to have real examples in interviews. Each case study will get a Persian summary and key lessons on the site.
- **Candidates:** Uber (Hudi origin, real-time platform) · Netflix (Iceberg origin, data platform) · LinkedIn (Kafka origin) · Airbnb (Minerva metrics layer) · Spotify (event delivery) · Cloudflare (ClickHouse for analytics) · Discord (data storage evolution) · Iranian engineering blogs (Virgool, company tech blogs) on Kafka/ClickHouse/Spark experiences

---

# Track 15 — ML & AI Data Engineering | مهندسی داده برای ML و AI

### 15.1 Feature Engineering Pipelines & Feature Stores
`feature-stores` · 🟠 Advanced · P1 · ~3h · Technology
- **FA:** online و offline store، point-in-time correctness برای جلوگیری از leakage، Feast، و training-serving skew.
- **EN:** Online vs offline stores, point-in-time correctness to avoid leakage, Feast, and training-serving skew.

### 15.2 MLOps Data Foundations
`mlops-data` · 🟡 Intermediate · P2 · ~2h · Concept
- **FA:** داده برای چرخه‌ی ML: نسخه‌بندی داده، MLflow، مانیتورینگ drift، و پایپ‌لاین آموزش مجدد.
- **EN:** Data for the ML lifecycle: data versioning, MLflow, drift monitoring, and retraining pipelines.

### 15.3 Data Pipelines for LLMs & RAG
`llm-data-pipelines` · 🟠 Advanced · P2 · ~3h · Concept
- **FA:** ingestion اسناد، chunking، embedding، vector DB (Qdrant، Milvus، pgvector)، به‌روزرسانی افزایشی ایندکس، و کیفیت داده برای LLM. موضوع داغ و نقطه‌ی تمایز.
- **EN:** Document ingestion, chunking, embedding, vector databases (Qdrant, Milvus, pgvector), incremental index updates, and data quality for LLMs. A hot topic and differentiator.

### 15.4 BI & Analytics Engineering
`bi-analytics-engineering` · 🟡 Intermediate · P2 · ~2h · Technology
- **FA:** Superset و Metabase (رایج در ایران)، نقش Analytics Engineer، و طراحی mart برای BI.
- **EN:** Superset and Metabase (common in Iran), the analytics engineer role, and designing marts for BI.

---

# Track 16 — Behavioral & Leadership | رفتاری و رهبری

### 16.1 The STAR(R) Framework
`star-framework` · 🟢 Beginner · P0 · ~1h · Guide
- **FA:** ساختار پاسخ رفتاری: Situation، Task، Action، Result، و Reflection (چه یاد گرفتی). برای سنیور روی Action شخصی و اثر قابل‌اندازه‌گیری تأکید کن.
- **EN:** Behavioral answer structure: Situation, Task, Action, Result, plus Reflection. For seniors, emphasize personal actions and measurable impact.

### 16.2 Project Deep-Dive Narratives
`project-narratives` · 🔴 Senior · P0 · ~6h · Practice
- **FA:** آماده‌کردن ۴ تا ۶ داستان پروژه که مصاحبه‌گر تا عمق آن‌ها را می‌کاود: معماری، چرا این تصمیم، چه گزینه‌هایی رد شد، چه چیزی شکست خورد، اگر دوباره می‌ساختی چه تغییری می‌دادی، عددها.
- **EN:** Prepare 4–6 project stories the interviewer will drill into: architecture, why each decision, which alternatives were rejected, what failed, what you'd change, the numbers.
- **Template:** context & scale · your role · architecture diagram · 3 key decisions with trade-offs · biggest incident · metrics · lessons

### 16.3 Senior Signals
`senior-signals` · 🔴 Senior · P0 · ~2h · Guide
- **FA:** نشانه‌هایی که مصاحبه‌گر دنبالش است: مالکیت، مدیریت ابهام، تأثیر فراتر از تیم، منتورینگ، تصمیم‌گیری با داده، مدیریت بدهی فنی، و مذاکره با ذی‌نفعان.
- **EN:** Signals interviewers look for: ownership, handling ambiguity, impact beyond the team, mentoring, data-driven decisions, managing tech debt, and stakeholder negotiation.

### 16.4 Behavioral Question Bank
`behavioral-questions` · 🟡 Intermediate · P0 · ~4h · Practice
- **FA:** بانک سؤال رفتاری با دسته‌بندی؛ برای هر دسته حداقل یک داستان آماده داشته باش.
- **EN:** Categorized behavioral question bank; have at least one story ready per category.
- **Categories:** conflict with a colleague/manager · a failure / production incident · a hard technical decision · disagreeing and committing · mentoring someone · influencing without authority · tight deadline trade-offs · improving a process · dealing with unclear requirements · saying no to a stakeholder

### 16.5 Incidents & Postmortems
`incidents-postmortems` · 🟠 Advanced · P1 · ~2h · Practice
- **FA:** چطور از یک حادثه‌ی تولید (داده‌ی خراب، از دست رفتن داده، کلاستر از کار افتاده) تعریف کنی: تشخیص، مهار، علت ریشه‌ای، اقدامات پیشگیرانه، فرهنگ بدون سرزنش.
- **EN:** How to narrate a production incident (corrupted data, data loss, cluster outage): detection, mitigation, root cause, preventive actions, blameless culture.

### 16.6 Questions to Ask the Interviewer
`questions-to-ask` · 🟢 Beginner · P1 · ~1h · Guide
- **FA:** سؤال‌های هوشمندانه درباره‌ی معماری داده، حجم، تیم، on-call، بدهی فنی و مسیر رشد که هم اطلاعات می‌دهد هم سنیوربودنت را نشان می‌دهد.
- **EN:** Smart questions about data architecture, scale, team, on-call, tech debt and growth path that both inform you and signal seniority.

### 16.7 Offer & Salary Negotiation in Iran
`negotiation-iran` · 🟡 Intermediate · P1 · ~2h · Guide
- **FA:** تحقیق بازه‌ی حقوق، بسته‌ی کامل (حقوق، پاداش، بیمه‌ی تکمیلی، وام، دورکاری، سهام)، زمان‌بندی، مذاکره با چند پیشنهاد هم‌زمان، و تورم و بازبینی حقوق.
- **EN:** Researching salary ranges, the full package (salary, bonus, supplementary insurance, loans, remote work, equity), timing, negotiating with multiple offers, inflation and salary reviews.

---

# Appendix A — 8-Week Study Plan | پیوست الف — برنامه‌ی ۸ هفته‌ای

> **FA:** فرض: حدود ۳ ساعت در روزهای کاری و ۶ ساعت در آخر هفته (~۲۷ ساعت در هفته). SQL روزانه ۳۰ تا ۴۵ دقیقه در کل دوره. اگر زمانت متفاوت است، برنامه را مقیاس می‌دهیم.
> **EN:** Assumption: ~3 h on weekdays and ~6 h on weekends (~27 h/week). Daily SQL drill of 30–45 min throughout. We will rescale if your time differs.

| Week | Focus (EN) | تمرکز (FA) | Tracks | Deliverables |
|---|---|---|---|---|
| 1 | Interview guide, SQL core, data formats, numbers | راهنما، SQL پایه و window، فرمت‌ها، اعداد مرجع | 0, 1.6–1.8, 2.1–2.9 | 25 SQL problems · job-posting analysis |
| 2 | Data modeling + advanced SQL | مدل‌سازی داده + SQL پیشرفته | 2.10–2.13, 6.1–6.9 | 3 modeling exercises · 20 SQL problems |
| 3 | Distributed systems & DB internals | سیستم‌های توزیع‌شده و معماری دیتابیس | 4, 5 | DDIA ch. 3, 5–9 notes · flashcards |
| 4 | Spark deep dive + Python coding | اسپارک عمیق + کدنویسی پایتون | 3, 7 | Spark tuning notes · 10 coding problems |
| 5 | Kafka + stream processing + Flink | کافکا + پردازش جریانی + فلینک | 8 | 2 design problems (Ad click, Ride-hailing) |
| 6 | Ingestion, CDC, lakehouse, ClickHouse/StarRocks | دریافت، CDC، لیک‌هاوس، ClickHouse/StarRocks | 9, 10 | 2 design problems (CDC lake, Telco CDR) |
| 7 | Orchestration, quality, platform + system design drills | ارکستراسیون، کیفیت، پلتفرم + تمرین طراحی | 11, 12, 13, 14 | 4 timed design problems · project narratives drafted |
| 8 | Mock interviews, behavioral, weak-spot review | مصاحبه‌ی آزمایشی، رفتاری، مرور نقاط ضعف | 14, 16, review | 3+ mocks · story bank final · cheat sheets |

---

# Appendix B — Resources | پیوست ب — منابع

**Books | کتاب‌ها**
- *Designing Data-Intensive Applications* — Martin Kleppmann (P0, core theory)
- *Fundamentals of Data Engineering* — Joe Reis & Matt Housley (P0, lifecycle & architecture)
- *The Data Warehouse Toolkit* — Ralph Kimball & Margy Ross (P0, modeling)
- *Streaming Systems* — Tyler Akidau et al. (P1, streaming theory)
- *Spark: The Definitive Guide* / *Learning Spark* (P1)
- *High Performance Spark* (P2, tuning)
- *Kafka: The Definitive Guide* (P1)
- *Database Internals* — Alex Petrov (P2, storage engines)
- *Data Pipelines Pocket Reference* — James Densmore (P2, quick read)

**Practice | تمرین**
- SQL: DataLemur, LeetCode SQL (50 + Hard), StrataScratch
- Coding: LeetCode (Easy/Medium, tags: hash table, heap, intervals, sliding window)
- System design: HelloInterview (method & generic SD), this roadmap's Track 14

**Docs to skim | مستندات**
- Kafka, Flink, Spark, Iceberg, ClickHouse, StarRocks, Airflow, dbt official docs — architecture/concepts pages only

---

# Appendix C — Glossary (seed) | پیوست ج — واژه‌نامه (اولیه)

| Term | FA |
|---|---|
| Grain | دانه‌بندی / سطح جزئیات هر ردیف fact |
| Idempotency | خودتوانی؛ اجرای چندباره = همان نتیجه |
| Watermark | نشانگر پیشرفت زمان رویداد در جریان |
| Backfill | پرکردن داده‌ی دوره‌های گذشته |
| Skew | عدم توازن توزیع داده بین partitionها |
| Shuffle | جابه‌جایی داده بین نودها برای گروه‌بندی یا join |
| Compaction | ادغام فایل‌ها/قطعه‌های کوچک به بزرگ |
| Lineage | ردیابی منشأ و مسیر داده |
| Exactly-once | پردازش دقیقاً یک‌باره‌ی هر رکورد از نظر اثر |
| Surrogate key | کلید جانشین (بی‌معنا) در جدول بعد |
| Data contract | قرارداد داده بین تولیدکننده و مصرف‌کننده |
| Consumer lag | عقب‌ماندگی مصرف‌کننده از آخرین پیام |

---

# Appendix D — Content Model for the Website | پیوست د — مدل محتوای سایت

> **FA:** این بخش پل بین این سند و سایت است. هر مبحث بالا به یک صفحه با فیلدهای زیر تبدیل می‌شود.
> **EN:** This bridges the document and the site. Each topic above becomes a page with the fields below.

```yaml
id: spark-joins-skew            # URL slug
track: 7                        # Track number
title_en: "Joins & Data Skew in Spark"
title_fa: "join و عدم توازن داده در اسپارک"
type: concept                   # guide | concept | technology | pattern | problem | practice
level: senior                   # beginner | intermediate | advanced | senior
priority: P0                    # P0 | P1 | P2
est_hours: 5
prerequisites: [spark-architecture, spark-shuffle]
summary_fa: "..."
summary_en: "..."
body_fa: "..."                  # full lesson (Persian)
body_en: "..."                  # full lesson (English)
diagrams: []                    # Mermaid / SVG
interview_questions: []         # with model answers
quiz: []                        # multiple choice
flashcards: []                  # term ↔ definition
practice: []                    # SQL / coding / design exercises
related: [pattern-hot-keys, spark-aqe]
sources: []
```

**HelloInterview → this site mapping | نگاشت ساختار**

| HelloInterview | This site | FA |
|---|---|---|
| In a Hurry | Track 0 Interview Guide | راهنمای سریع |
| Core Concepts | Tracks 1, 4, 5, 6 | مفاهیم پایه |
| Key Technologies | Tracks 7, 8, 10, 11 deep dives | تکنولوژی‌های کلیدی |
| Patterns | Track 14B | الگوها |
| Question Breakdowns | Track 14C | مسائل کامل |
| Advanced Topics | 1.5, 10.9, 15.x | مباحث پیشرفته |
| In the Wild | Track 14D | مطالعه‌ی موردی |
| Coding (DSA) | Tracks 2 (SQL), 3 (Python) | کدنویسی و SQL |
| Behavioral | Track 16 | رفتاری |
| Guided Practice / AI tutor | (to be designed) | تمرین هدایت‌شده / معلم هوشمند |
| Mark as read / progress | (to be designed) | پیگیری پیشرفت |

---

## Summary Stats | آمار کلی

- **Tracks:** 17 (0–16) + 4 appendices
- **Topic pages:** 172 (P0: 90 · P1: 62 · P2: 20), including 15 patterns; plus 22 design problems, 17 SQL patterns, 14 coding problems, 10 modeling problems
- **Estimated hours:** P0 ≈ 316h · P1 ≈ 182h · P2 ≈ 43h
- **FA:** کل P0 از ظرفیت ۸ هفته (~۲۱۶ ساعت با ۲۷ ساعت در هفته) بیشتر است. چون در Kafka، Spark، ClickHouse، StarRocks، Airflow و Kubernetes تجربه‌ی عملی داری، بخشی از این زمان فشرده می‌شود؛ در سایت باید بتوانی مباحثی را که بلدی «علامت بزنی» تا برنامه خودش را تنظیم کند. P2 فقط در صورت وقت اضافه.
- **EN:** Total P0 exceeds the 8-week capacity (~216h at 27h/week). Hands-on experience with Kafka, Spark, ClickHouse, StarRocks, Airflow and Kubernetes compresses much of it; the site should let you mark known topics so the plan rebalances. P2 only if time allows.
