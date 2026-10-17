// Iran data-engineering job market report. Every pair is [en, fa].
// Data collected 2026-10-17 (Mehr 1405) from Jobinja, JobVision and Iran Talent pages.
export type L = readonly [string, string];

export const meta = {
  collected: '2026-10-17',
  collectedFa: '۱۴۰۵/۰۷/۲۵',
  postingsRead: 13,
};

export const title: L = ['Data Engineering in Iran: Jobs, Salary, Stack and Companies', 'مهندسی داده در ایران: بازار کار، حقوق، استک و شرکت‌ها'];
export const intro: L = [
  'A market report built from 13 data engineer job postings read in full (Jobinja, JobVision), salary reports from Iran Talent and Fatehi School, and an employer ranking. It shows what Iranian employers actually ask for, what they pay, and what to learn first.',
  'گزارشی بر پایه‌ی ۱۳ آگهی مهندس داده که کامل خوانده شد (جابینجا، جاب‌ویژن)، گزارش‌های حقوق ایران تلنت و فاتحی اسکول، و رتبه‌بندی کارفرماها. نشان می‌دهد کارفرماهای ایرانی واقعاً چه می‌خواهند، چه می‌پردازند و اول چه باید یاد گرفت.',
];

export const caveats: L[] = [
  ['JobVision renders its search results with JavaScript and hides most details behind login. Its keyword pages returned no listings to the crawler, so only one JobVision posting (Algorithm, 80–100M) could be read in full. Jobinja and search-indexed JobVision pages supplied the rest.', 'جاب‌ویژن نتایج را با جاوااسکریپت می‌سازد و بیشتر جزئیات پشت لاگین است. صفحه‌های جستجوی آن برای خزنده نتیجه‌ای نداد، پس فقط یک آگهی جاب‌ویژن (الگوریتم، ۸۰ تا ۱۰۰ میلیون) کامل خوانده شد. بقیه از جابینجا و صفحه‌های ایندکس‌شده‌ی جاب‌ویژن آمده است.'],
  ['Most postings hide salary ("negotiable" or login required). Explicit numbers are rare: treat salary figures as ranges, not quotes.', 'بیشتر آگهی‌ها حقوق را پنهان می‌کنند («توافقی» یا نیازمند ورود). عدد صریح کم است: ارقام حقوق را بازه بدانید، نه قول قطعی.'],
  ['Several postings contradict themselves (header says 3–6 years, text says 5+). Many listed postings were already expired. Sample size is small (13), so percentages below describe this sample only.', 'چند آگهی با خودش در تضاد است (سربرگ ۳ تا ۶ سال، متن ۵+ سال). بسیاری از آگهی‌ها منقضی شده بودند. نمونه کوچک است (۱۳ آگهی) و درصدها فقط همین نمونه را توصیف می‌کنند.'],
  ['All salaries are monthly, in million toman. Iran Talent figures are net and mostly requested (not paid) salaries. The junior/mid/senior table at the end of the salary section is our estimate, not a published statistic.', 'همه‌ی حقوق‌ها ماهانه و به میلیون تومان است. ارقام ایران تلنت خالص و عمدتاً حقوق درخواستی (نه دریافتی) است. جدول جونیور/مید/سنیور در پایان بخش حقوق برآورد خودمان است، نه آمار منتشرشده.'],
];

export const findings: L[] = [
  ['The market wants experienced people: 11 of 13 postings ask for 3+ years (the lowest asks are Algorithm 2 and Adin 1), and none is a pure entry-level role. Only internships (Shiveh) were found for beginners.', 'بازار آدم باتجربه می‌خواهد: ۱۱ از ۱۳ آگهی حداقل ۳ سال سابقه می‌خواهند (کمترین‌ها الگوریتم ۲ و آدین ۱ سال) و هیچ‌کدام نقش ورودی محض نیست. برای مبتدی فقط کارآموزی (شیوه) دیده شد.'],
  ['The Iranian "default stack" is Python + SQL + Kafka + Airflow + Spark + ClickHouse, self-hosted on Linux/Docker/Kubernetes. Cloud-managed services (BigQuery, Redshift, Snowflake) barely appear.', '«استک پیش‌فرض» ایران: Python + SQL + Kafka + Airflow + Spark + ClickHouse، خودمیزبان روی Linux/Docker/Kubernetes است. سرویس‌های مدیریت‌شده‌ی ابری (BigQuery، Redshift، Snowflake) تقریباً دیده نمی‌شوند.'],
  ['ClickHouse is the standout Iranian choice for analytics (8 of 13), far more than in many other markets.', 'ClickHouse انتخاب شاخص ایران برای تحلیل است (۸ از ۱۳)، خیلی بیشتر از بسیاری از بازارهای دیگر.'],
  ['Fintech (Zibal, Digipay, Azki, SnappPay, Bitpin), telecom (Irancell Labs) and media (Telewebion) are the main hirers. Banking-adjacent shops still use SSIS/ODI/SQL Server.', 'فین‌تک (زیبال، دیجی‌پی، ازکی، اسنپ‌پی، بیت‌پین)، مخابرات (ایرانسل لبز) و رسانه (تلوبیون) کارفرماهای اصلی‌اند. شرکت‌های نزدیک به بانک هنوز SSIS/ODI/SQL Server می‌خواهند.'],
  ['Newest postings (1405) add CDC with Debezium, lakehouse formats (Iceberg, Hudi), OpenMetadata and vector databases (Qdrant) for AI work.', 'آگهی‌های تازه (۱۴۰۵) CDC با Debezium، فرمت‌های لیک‌هاوس (Iceberg، Hudi)، OpenMetadata و دیتابیس‌های برداری (Qdrant) برای کارهای AI اضافه کرده‌اند.'],
];

// ---- Salary ----
export const salaryNote: L = [
  'Monthly, million toman. Source reliability differs, so the sources are shown separately before our estimate.',
  'ماهانه، میلیون تومان. اعتبار منابع یکسان نیست، پس منابع جدا نشان داده شده و برآورد ما بعد از آن آمده است.',
];
export type SalaryRow = { source: L; scope: L; value: L; kind: L };
export const salarySources: SalaryRow[] = [
  { source: ['JobVision posting, Algorithm Data & AI (Mehr 1405)', 'آگهی جاب‌ویژن، الگوریتم (مهر ۱۴۰۵)'], scope: ['Data Engineer, 2+ yrs, Tajrish', 'مهندس داده، ۲+ سال، تجریش'], value: ['80–100', '۸۰ تا ۱۰۰'], kind: ['Stated by employer', 'اعلام‌شده توسط کارفرما'] },
  { source: ['Jobinja posting, Booran Group', 'آگهی جابینجا، گروه بوران'], scope: ['Senior Data Engineer, 4+ yrs', 'سنیور، ۴+ سال'], value: ['from 80', 'از ۸۰'], kind: ['Stated by employer (minimum)', 'اعلام‌شده (حداقل)'] },
  { source: ['Jobinja coverage of JobVision/Jobinja ads', 'گزارش جابینجا از آگهی‌ها'], scope: ['Top ad: data engineer with Python', 'بالاترین آگهی: مهندس داده با پایتون'], value: ['up to 110', 'تا ۱۱۰'], kind: ['Highest ad seen', 'بالاترین آگهی'] },
  { source: ['Iran Talent, Data Science & AI specialist (1405)', 'ایران تلنت، کارشناس علوم داده و AI (۱۴۰۵)'], scope: ['Tehran, net. P10 / P25 / median / P75 / P90', 'تهران، خالص. صدک ۱۰/۲۵/میانه/۷۵/۹۰'], value: ['28 / 37 / 50 / 71 / 85', '۲۸ / ۳۷ / ۵۰ / ۷۱ / ۸۵'], kind: ['Requested salaries, adjacent role', 'حقوق درخواستی، نقش مجاور'] },
  { source: ['Iran Talent, same role, 1404 received', 'ایران تلنت، همان نقش، ۱۴۰۴ دریافتی'], scope: ['Tehran, net. P10 / P25 / median / P75 / P90', 'تهران، خالص. صدک‌ها'], value: ['20 / 26 / 35 / 45 / 60', '۲۰ / ۲۶ / ۳۵ / ۴۵ / ۶۰'], kind: ['Received salaries (+~42% median to 1405)', 'دریافتی (رشد میانه ≈ ۴۲٪)'] },
  { source: ['Fatehi School (1404), Data Science/ML', 'فاتحی اسکول (۱۴۰۴)، علم داده/ML'], scope: ['Junior / Mid / Senior', 'جونیور / مید / سنیور'], value: ['25–40 / 40–60 / 60–100', '۲۵–۴۰ / ۴۰–۶۰ / ۶۰–۱۰۰'], kind: ['Course seller, no stated method', 'فروشنده‌ی دوره، بدون روش مشخص'] },
  { source: ['Fatehi School (1404), DevOps', 'فاتحی اسکول (۱۴۰۴)، DevOps'], scope: ['Junior / Mid / Senior', 'جونیور / مید / سنیور'], value: ['20–40 / 40–75 / 75–130', '۲۰–۴۰ / ۴۰–۷۵ / ۷۵–۱۳۰'], kind: ['Neighbour role, same caveat', 'نقش مجاور، همان محدودیت'] },
  { source: ['Programmer-salary aggregator (1405), Data Engineer/ETL', 'سایت تجمیعی حقوق برنامه‌نویس (۱۴۰۵)، مهندس داده/ETL'], scope: ['0–2 / 2–5 / 5–10 yrs', '۰–۲ / ۲–۵ / ۵–۱۰ سال'], value: ['22–45 / 50–100 / 105–200', '۲۲–۴۵ / ۵۰–۱۰۰ / ۱۰۵–۲۰۰'], kind: ['Unsourced, treat as upper bound', 'بدون منبع، سقف خوش‌بینانه'] },
];
export type EstRow = { level: L; years: L; range: L; note: L };
export const salaryEstimate: EstRow[] = [
  { level: ['Junior', 'جونیور'], years: ['0–2 yrs', '۰ تا ۲ سال'], range: ['25–45', '۲۵ تا ۴۵'], note: ['Few true junior openings; often analyst or ETL-developer titles first.', 'آگهی جونیور واقعی کم است؛ معمولاً اول با عنوان تحلیلگر یا توسعه‌دهنده‌ی ETL شروع می‌شود.'] },
  { level: ['Mid', 'مید'], years: ['2–5 yrs', '۲ تا ۵ سال'], range: ['50–90', '۵۰ تا ۹۰'], note: ['Matches stated 80–100 for 2+ years in a fintech/data company; Tehran pays the most.', 'با ۸۰ تا ۱۰۰ برای ۲+ سال در شرکت داده/فین‌تک هم‌خوان است؛ تهران بیشتر می‌دهد.'] },
  { level: ['Senior', 'سنیور'], years: ['5+ yrs', '۵+ سال'], range: ['90–150+', '۹۰ تا ۱۵۰+'], note: ['Above 150 is rare and usually means equity, a large company or foreign-paid work.', 'بالای ۱۵۰ نادر است و معمولاً یعنی سهام، شرکت بزرگ یا درآمد ارزی.'] },
];
export const salaryTips: L[] = [
  ['Outside Tehran pay is lower by about 25–30% (Iran Talent: large cities median 37 vs Tehran 50).', 'خارج از تهران حقوق حدود ۲۵ تا ۳۰ درصد کمتر است (ایران تلنت: شهرهای بزرگ میانه ۳۷ در برابر تهران ۵۰).'],
  ['Variable pay is small (about 4–10% of total). Negotiate fixed salary, not bonus promises.', 'پرداخت متغیر کوچک است (حدود ۴ تا ۱۰ درصد). روی حقوق ثابت چانه بزنید، نه وعده‌ی پاداش.'],
  ['Inflation is fast: the Iran Talent median grew about 42% from 1404 to 1405. Ask for a written yearly raise rule.', 'تورم سریع است: میانه‌ی ایران تلنت از ۱۴۰۴ تا ۱۴۰۵ حدود ۴۲٪ رشد کرده. قاعده‌ی افزایش سالانه را مکتوب بخواهید.'],
  ['Overtime, New Year bonus and severance are legal rights under Iranian labor law. Adin states them in the ad; ask other employers.', 'اضافه‌کار، عیدی و سنوات طبق قانون کار حق است. آدین در آگهی نوشته؛ از بقیه بپرسید.'],
];

// ---- Technologies ----
export type TechRow = { name: string; n: number; group: L; note: L };
export const techCount: TechRow[] = [
  { name: 'SQL', n: 13, group: ['Core', 'پایه'], note: ['Every posting. PostgreSQL/MySQL most common; SQL Server in banking.', 'همه‌ی آگهی‌ها. PostgreSQL/MySQL رایج‌تر؛ SQL Server در بانکی.'] },
  { name: 'Python', n: 12, group: ['Core', 'پایه'], note: ['Scala or Java accepted at Snapp, Telewebion, Irancell, Azki.', 'اسنپ، تلوبیون، ایرانسل و ازکی Scala یا Java هم می‌پذیرند.'] },
  { name: 'Apache Airflow', n: 10, group: ['Orchestration', 'ارکستریشن'], note: ['De-facto scheduler. Dagster/ArgoCD appear as alternatives.', 'زمان‌بند استاندارد. Dagster و ArgoCD جایگزین‌اند.'] },
  { name: 'Apache Kafka', n: 9, group: ['Streaming', 'استریم'], note: ['Plus Kafka Connect, Debezium CDC, Kafka Streams/ksqlDB.', 'به‌علاوه‌ی Kafka Connect، Debezium CDC و Kafka Streams/ksqlDB.'] },
  { name: 'Docker / Kubernetes', n: 9, group: ['Platform', 'پلتفرم'], note: ['Docker expected; Kubernetes + Helm + GitLab CI at senior level.', 'Docker انتظار پایه؛ Kubernetes + Helm + GitLab CI در سطح سنیور.'] },
  { name: 'Apache Spark', n: 8, group: ['Processing', 'پردازش'], note: ['PySpark and Scala; dbt accepted instead at Zibal.', 'PySpark و Scala؛ زیبال dbt را هم می‌پذیرد.'] },
  { name: 'ClickHouse', n: 8, group: ['Storage', 'ذخیره‌سازی'], note: ['The Iranian OLAP engine of choice. Also Druid, Trino.', 'موتور OLAP محبوب ایران. Druid و Trino هم دیده می‌شود.'] },
  { name: 'Lakehouse / S3 / MinIO / Iceberg / Parquet', n: 5, group: ['Storage', 'ذخیره‌سازی'], note: ['Self-hosted S3 (MinIO, Ceph) because public cloud is hard to use.', 'S3 خودمیزبان (MinIO، Ceph) چون استفاده از ابر عمومی سخت است.'] },
  { name: 'Git / CI-CD (GitLab, Jenkins)', n: 4, group: ['Platform', 'پلتفرم'], note: ['GitLab is the usual self-hosted choice.', 'GitLab انتخاب معمول خودمیزبان است.'] },
  { name: 'BI: Metabase / Superset / Zeppelin', n: 4, group: ['Serving', 'سرویس‌دهی'], note: ['Open-source BI, rarely Tableau or Power BI.', 'BI متن‌باز؛ Tableau و Power BI کم‌تر.'] },
  { name: 'NoSQL: MongoDB / Redis / Elasticsearch', n: 4, group: ['Storage', 'ذخیره‌سازی'], note: ['Used beside the warehouse.', 'کنار انبار داده استفاده می‌شود.'] },
  { name: 'LLM / vector DB (Qdrant)', n: 3, group: ['AI', 'هوش مصنوعی'], note: ['New in 1405 postings: feeding RAG and AI products.', 'تازه در آگهی‌های ۱۴۰۵: تغذیه‌ی RAG و محصولات AI.'] },
  { name: 'SSIS / ODI (classic ETL)', n: 1, group: ['Legacy', 'سنتی'], note: ['Banking and enterprise shops only.', 'فقط شرکت‌های بانکی و سازمانی.'] },
];

// ---- Learning roadmap ----
export type Step = { n: number; title: L; items: L; why: L; tag: 'P0' | 'P1' | 'P2'; link?: string };
export const roadmap: Step[] = [
  { n: 1, tag: 'P0', title: ['SQL and relational databases', 'SQL و دیتابیس رابطه‌ای'], items: ['Joins, windows, CTEs, query plans, indexes, PostgreSQL internals, ClickHouse basics', 'Join، پنجره‌ای، CTE، پلن اجرا، ایندکس، PostgreSQL، مبانی ClickHouse'], why: ['Asked in every interview; Booran wants plan analysis, partitioning, RLS.', 'در هر مصاحبه پرسیده می‌شود؛ بوران تحلیل پلن، پارتیشن و RLS می‌خواهد.'], link: 'playground/' },
  { n: 2, tag: 'P0', title: ['Python for data', 'پایتون برای داده'], items: ['Testable modules, typing, pandas/PySpark, packaging, Git', 'ماژول تست‌پذیر، تایپ، pandas/PySpark، بسته‌بندی، Git'], why: ['12 of 13 postings.', '۱۲ از ۱۳ آگهی.'] },
  { n: 3, tag: 'P0', title: ['Linux, Docker, CI/CD', 'لینوکس، Docker و CI/CD'], items: ['Shell, networking basics, Compose, GitLab CI, then Kubernetes + Helm', 'شل، مبانی شبکه، Compose، GitLab CI، سپس Kubernetes و Helm'], why: ['Almost everything is self-hosted.', 'تقریباً همه‌چیز خودمیزبان است.'] },
  { n: 4, tag: 'P0', title: ['Data modeling and warehousing', 'مدل‌سازی داده و انبار داده'], items: ['Star/snowflake, SCD, Data Vault, normalization vs denormalization, partitioning', 'ستاره/برف‌دانه، SCD، Data Vault، نرمال‌سازی و دنرمال‌سازی، پارتیشن'], why: ['Core of ETL/DWH roles including banking.', 'هسته‌ی نقش‌های ETL/DWH از جمله بانکی.'], link: 'tracks/' },
  { n: 5, tag: 'P0', title: ['Airflow orchestration', 'ارکستریشن با Airflow'], items: ['DAG design, idempotency, backfills, sensors, retries, monitoring', 'طراحی DAG، ایدمپوتنسی، backfill، sensor، retry، مانیتورینگ'], why: ['10 of 13 postings.', '۱۰ از ۱۳ آگهی.'] },
  { n: 6, tag: 'P0', title: ['Kafka and CDC', 'Kafka و CDC'], items: ['Partitions, consumer groups, delivery semantics, Kafka Connect, Debezium', 'پارتیشن، consumer group، تضمین تحویل، Kafka Connect، Debezium'], why: ['9 of 13; CDC from PostgreSQL into ClickHouse is the common Iranian pipeline.', '۹ از ۱۳؛ CDC از PostgreSQL به ClickHouse پایپ‌لاین رایج ایران است.'] },
  { n: 7, tag: 'P1', title: ['Spark', 'Spark'], items: ['DataFrames, shuffle, skew, partitioning, Structured Streaming, tuning', 'DataFrame، shuffle، skew، پارتیشن، Structured Streaming، تیونینگ'], why: ['8 of 13, mostly senior roles.', '۸ از ۱۳، بیشتر نقش‌های ارشد.'] },
  { n: 8, tag: 'P1', title: ['ClickHouse and OLAP', 'ClickHouse و OLAP'], items: ['MergeTree, ordering keys, materialized views, dedup (ReplacingMergeTree), TTL', 'MergeTree، کلید مرتب‌سازی، materialized view، dedup، TTL'], why: ['8 of 13, and rarely taught in courses, so it differentiates you.', '۸ از ۱۳ و کم آموزش داده می‌شود؛ شما را متمایز می‌کند.'] },
  { n: 9, tag: 'P1', title: ['Lakehouse and object storage', 'لیک‌هاوس و ذخیره‌سازی شیء'], items: ['Parquet, Iceberg/Hudi/Delta, MinIO/S3, Trino, compaction', 'Parquet، Iceberg/Hudi/Delta، MinIO/S3، Trino، compaction'], why: ['Senior postings (Adin, Irancell, SnappPay, Telewebion).', 'آگهی‌های ارشد (آدین، ایرانسل، اسنپ‌پی، تلوبیون).'] },
  { n: 10, tag: 'P1', title: ['Data quality, observability, governance', 'کیفیت، مشاهده‌پذیری و حاکمیت داده'], items: ['Validation, freshness, lineage, SLAs/SLOs, OpenMetadata, data contracts', 'اعتبارسنجی، freshness، lineage، SLA/SLO، OpenMetadata، data contract'], why: ['Zibal, Telewebion, Bitpin, Fanavin all list it.', 'زیبال، تلوبیون، بیت‌پین و فناوین همگی ذکر کرده‌اند.'] },
  { n: 11, tag: 'P2', title: ['AI-adjacent data work', 'کار داده مرتبط با AI'], items: ['Vector DBs (Qdrant), embeddings pipelines, RAG data prep, prompt basics', 'دیتابیس برداری (Qdrant)، پایپ‌لاین embedding، آماده‌سازی داده RAG، مبانی پرامپت'], why: ['Appears in the newest postings; a growing differentiator.', 'در تازه‌ترین آگهی‌ها دیده می‌شود؛ تمایزدهنده‌ی رو‌به‌رشد.'] },
  { n: 12, tag: 'P2', title: ['Distributed-systems theory', 'مبانی سیستم‌های توزیع‌شده'], items: ['CAP, consistency, idempotency, Raft/Paxos, backpressure', 'CAP، سازگاری، ایدمپوتنسی، Raft/Paxos، backpressure'], why: ['Telewebion asks for it explicitly at senior level.', 'تلوبیون صریحاً در سطح ارشد می‌خواهد.'], link: 'drill/' },
];

// ---- Companies ----
export type Co = { name: string; sector: L; stack: string; level: L; pay: L; perks: L; src: string; date: string };
export const companies: Co[] = [
  { name: 'Adin (آدین)', sector: ['IT & telecom services', 'فناوری اطلاعات و ارتباطات'], stack: 'ClickHouse, Iceberg, Hudi, Spark, Airflow, Trino', level: ['Senior, 1+ yr, path to lead', 'سنیور، ۱+ سال، مسیر لید'], pay: ['Negotiable; paid from day one', 'توافقی؛ از روز اول'], perks: ['Profit/equity share on KPIs, 1–2 remote days per week, uncapped overtime per labor law, New Year bonus, severance, flexible 8–20 hours', 'سهم سود/سهام بر پایه KPI، ۱ تا ۲ روز دورکاری در هفته، اضافه‌کار بدون سقف طبق قانون، عیدی، سنوات، ساعت انعطاف‌پذیر'], src: 'jobinja', date: '1405' },
  { name: 'Zibal (زیبال)', sector: ['Fintech / payments', 'فین‌تک / پرداخت'], stack: 'Python, Airflow, Spark/dbt, Kafka, PostgreSQL, MongoDB, lakehouse', level: ['Senior, 3–6 yrs', 'سنیور، ۳ تا ۶ سال'], pay: ['Negotiable', 'توافقی'], perks: ['Timely pay, insurance + supplementary health, bank/org loans, flexible hours, military-service placement, monthly welfare credit, personal-development support, breakfast, game room, team events', 'پرداخت به‌موقع، بیمه و تکمیلی، وام، ساعت انعطاف‌پذیر، امریه سربازی، اعتبار رفاهی ماهانه، حمایت از رشد فردی، صبحانه، اتاق بازی، رویدادهای تیمی'], src: 'jobinja', date: '1405' },
  { name: 'Algorithm Data & AI (الگوریتم)', sector: ['Banking data / AI', 'داده و AI بانکی'], stack: 'SQL Server, SSIS, ODI, ClickHouse, star/snowflake', level: ['Mid, 2+ yrs', 'مید، ۲+ سال'], pay: ['80–100 (stated)', '۸۰ تا ۱۰۰ (اعلام‌شده)'], perks: ['Not listed. 07:00–16:00 schedule, 1 h flexibility', 'ذکر نشده. ساعت ۷ تا ۱۶ با یک ساعت انعطاف'], src: 'jobvision', date: '1405' },
  { name: 'Booran Group (بوران)', sector: ['E-commerce marketing', 'بازاریابی تجارت الکترونیک'], stack: 'PostgreSQL (deep), CDC, APIs, optional Python/ETL', level: ['Senior, 4+ yrs', 'سنیور، ۴+ سال'], pay: ['from 80 (stated)', 'از ۸۰ (اعلام‌شده)'], perks: ['Own an in-house product\'s data architecture; no concrete perks listed', 'مالکیت معماری داده‌ی محصول داخلی؛ مزیت مشخصی ذکر نشده'], src: 'jobinja', date: '1405' },
  { name: 'Telewebion (تلوبیون)', sector: ['Streaming media / CDN', 'رسانه‌ی استریم / CDN'], stack: 'Python/Java/Scala, ClickHouse, Kafka, Spark/Flink/Trino, Iceberg, Ceph', level: ['Senior, 5+ yrs', 'سنیور، ۵+ سال'], pay: ['Negotiable', 'توافقی'], perks: ['Not listed. Hardest technical bar: CAP, Raft/Paxos, SLO ownership', 'ذکر نشده. بالاترین سطح فنی: CAP، Raft/Paxos، SLO'], src: 'jobinja', date: '1405' },
  { name: 'Irancell Labs (ایرانسل لبز)', sector: ['Telecom', 'مخابرات'], stack: 'Spark, Kafka, ClickHouse, Airflow, MinIO, Parquet, Superset', level: ['Senior, 5+ yrs', 'سنیور، ۵+ سال'], pay: ['Negotiable', 'توافقی'], perks: ['Not listed. Multi-terabyte scale', 'ذکر نشده. مقیاس چندترابایتی'], src: 'jobinja', date: '1405' },
  { name: 'Bitpin (بیت‌پین)', sector: ['Crypto exchange', 'صرافی رمزارز'], stack: 'Airflow, PySpark, PostgreSQL→ClickHouse, Debezium, Metabase, OpenMetadata, Qdrant', level: ['Senior, 3–6 yrs', 'سنیور، ۳ تا ۶ سال'], pay: ['Not listed', 'ذکر نشده'], perks: ['Not listed', 'ذکر نشده'], src: 'jobinja', date: '1405/05' },
  { name: 'Azki (ازکی)', sector: ['Insurtech', 'بیمه‌تک'], stack: 'Kafka, Spark (Scala/Python), MySQL/ClickHouse, Airflow, Metabase, Zeppelin', level: ['Senior, 5+ yrs', 'سنیور، ۵+ سال'], pay: ['Not listed', 'ذکر نشده'], perks: ['Not listed', 'ذکر نشده'], src: 'jobinja', date: '1405/06' },
  { name: 'SnappPay (اسنپ‌پی)', sector: ['BNPL fintech (Snapp group)', 'پرداخت اقساطی (گروه اسنپ)'], stack: 'Airflow, Spark, Kafka, ClickHouse, lakehouse, Kubernetes, Helm, GitLab CI', level: ['Senior, 3–6 yrs', 'سنیور، ۳ تا ۶ سال'], pay: ['Not listed', 'ذکر نشده'], perks: ['Not listed', 'ذکر نشده'], src: 'jobinja', date: '1403' },
  { name: 'Digipay (دیجی‌پی)', sector: ['Fintech / payments', 'فین‌تک / پرداخت'], stack: 'Kafka, ClickHouse, Python, Airflow', level: ['Mid, 3+ yrs', 'مید، ۳+ سال'], pay: ['Not listed', 'ذکر نشده'], perks: ['Not listed', 'ذکر نشده'], src: 'jobinja', date: '1403' },
  { name: 'Fanavin (فناوین)', sector: ['AI/data products for enterprises', 'محصولات داده/AI سازمانی'], stack: 'Python, SQL/NoSQL, DMBOK data quality, LLM prompts, Airflow/dbt (nice)', level: ['Mid, 3–6 yrs', 'مید، ۳ تا ۶ سال'], pay: ['Negotiable', 'توافقی'], perks: ['Insurance + supplementary, on-time pay, lunch, loans, flexible hours, sports, military-service help', 'بیمه و تکمیلی، پرداخت به‌موقع، ناهار، وام، ساعت انعطاف‌پذیر، ورزش، کمک به سربازی'], src: 'jobinja', date: '1405' },
  { name: 'Nik Amooz (نیک آموز)', sector: ['Training / data-lake projects', 'آموزش / پروژه‌های دیتالیک'], stack: 'Linux, Docker, Python, SQL, NoSQL, ETL, Kafka, Jenkins/GitLab', level: ['Mid, 3–6 yrs', 'مید، ۳ تا ۶ سال'], pay: ['Negotiable', 'توافقی'], perks: ['Hands-on training next to senior engineers', 'آموزش عملی کنار مهندسان ارشد'], src: 'jobinja', date: 'older' },
];

export const employerRanking: L[] = [
  ['Cafe Bazaar ranks 1st for learning opportunities, work-life balance, promotion and culture in the Iran Talent employer report. No tech company appears in its top "salary and benefits" list (led by Henkel, Pakshoma, GTI). Hamkaran System is 5th for learning. This is a general employee-rating survey, not specific to data engineers.', 'کافه‌بازار در گزارش کارفرماهای ایران تلنت رتبه‌ی اول فرصت یادگیری، تعادل کار و زندگی، ارتقا و فرهنگ را دارد. هیچ شرکت فناوری در فهرست برتر «حقوق و مزایا» نیست (هنکل، پاک‌شما، GTI در صدر). همکاران سیستم پنجم یادگیری است. این نظرسنجی عمومی کارکنان است و مخصوص مهندس داده نیست.'],
  ['Companies that publish salary or perks in the ad (Zibal, Adin, Fanavin, Algorithm, Booran) are a good sign: transparency usually goes with predictable pay.', 'شرکت‌هایی که حقوق یا مزایا را در آگهی می‌نویسند (زیبال، آدین، فناوین، الگوریتم، بوران) نشانه‌ی خوبی‌اند: شفافیت معمولاً با پرداخت قابل‌پیش‌بینی همراه است.'],
];

export const perks: { perk: L; seen: L }[] = [
  { perk: ['Supplementary health insurance', 'بیمه‌ی تکمیلی درمان'], seen: ['Zibal, Fanavin', 'زیبال، فناوین'] },
  { perk: ['Bank / organisational loans', 'وام بانکی / سازمانی'], seen: ['Zibal, Fanavin', 'زیبال، فناوین'] },
  { perk: ['Flexible hours', 'ساعت کاری انعطاف‌پذیر'], seen: ['Zibal, Fanavin, Adin, Algorithm', 'زیبال، فناوین، آدین، الگوریتم'] },
  { perk: ['Remote days', 'دورکاری'], seen: ['Adin (1–2 days/week after trial)', 'آدین (۱ تا ۲ روز در هفته پس از آزمایشی)'] },
  { perk: ['Military-service placement (امریه)', 'امریه سربازی'], seen: ['Zibal, Fanavin', 'زیبال، فناوین'] },
  { perk: ['Meals and snacks', 'غذا و میان‌وعده'], seen: ['Zibal (breakfast), Fanavin (lunch)', 'زیبال (صبحانه)، فناوین (ناهار)'] },
  { perk: ['Welfare credit, training budget', 'اعتبار رفاهی، بودجه آموزش'], seen: ['Zibal', 'زیبال'] },
  { perk: ['Equity / profit share', 'سهام / سهم سود'], seen: ['Adin', 'آدین'] },
  { perk: ['Game room, sports, team events', 'اتاق بازی، ورزش، رویداد تیمی'], seen: ['Zibal, Fanavin', 'زیبال، فناوین'] },
];

export const hiringRules: L[] = [
  ['Military service: most postings require completion or an exemption (Algorithm, Digipay, Irancell, SnappPay). Some offer امریه placement instead (Zibal, Fanavin).', 'سربازی: بیشتر آگهی‌ها پایان خدمت یا معافیت می‌خواهند (الگوریتم، دیجی‌پی، ایرانسل، اسنپ‌پی). بعضی امریه ارائه می‌دهند (زیبال، فناوین).'],
  ['Gender: most say "not important". A minority restrict (Fanavin listed men only, plus a values requirement). Read each ad before investing.', 'جنسیت: بیشتر «مهم نیست» نوشته‌اند. اقلیتی محدود می‌کنند (فناوین فقط آقایان و شرط ارزشی). پیش از وقت گذاشتن آگهی را بخوانید.'],
  ['Education: bachelor\'s in computer science or IT is the common floor; a degree is rarely the deciding factor.', 'تحصیلات: کارشناسی کامپیوتر یا IT حداقل رایج است؛ مدرک به‌ندرت عامل تعیین‌کننده است.'],
  ['English: "medium" is typical (Algorithm 50%). Docs and tooling are English, so reading fluently is mandatory.', 'انگلیسی: سطح «متوسط» رایج است (الگوریتم ۵۰٪). مستندات و ابزارها انگلیسی است، پس خواندن روان الزامی است.'],
  ['Process (Digikala example): 2–3 interviews plus technical and behavioural assessments for tech roles.', 'فرایند (نمونه دیجی‌کالا): ۲ تا ۳ مصاحبه به‌علاوه‌ی ارزیابی فنی و رفتاری برای نقش‌های فناوری.'],
];

export const actions: L[] = [
  ['Build one portfolio pipeline in the Iranian stack: PostgreSQL → Debezium → Kafka → ClickHouse, orchestrated by Airflow, run with Docker Compose, with data-quality checks and a Metabase dashboard. Put it on GitHub.', 'یک پروژه‌ی نمونه با استک ایرانی بسازید: PostgreSQL → Debezium → Kafka → ClickHouse، با Airflow، اجرا با Docker Compose، همراه با چک کیفیت داده و داشبورد Metabase. در GitHub بگذارید.'],
  ['Get Python + SQL + Airflow + Kafka solid first (steps 1–6). Add Spark and ClickHouse next; that covers about 80% of the postings read.', 'اول Python + SQL + Airflow + Kafka را محکم کنید (گام‌های ۱ تا ۶). بعد Spark و ClickHouse؛ این حدود ۸۰٪ آگهی‌های خوانده‌شده را پوشش می‌دهد.'],
  ['For a first job, also search "ETL developer", "BI developer", "data analyst" and internships; many data-engineering careers start there.', 'برای اولین شغل «ETL developer»، «BI developer»، «data analyst» و کارآموزی را هم جست‌وجو کنید؛ بسیاری از مسیرهای مهندسی داده از آنجا شروع می‌شود.'],
  ['Practise with this site: question bank, mock interview, SQL playground and the senior design drill.', 'با همین سایت تمرین کنید: بانک سؤال، مصاحبه‌ی آزمایشی، SQL playground و تمرین طراحی سنیور.'],
];

export const sources: { name: string; url: string }[] = [
  { name: 'Jobinja: Data Engineer search (479 results at crawl time)', url: 'https://jobinja.ir/jobs?filters%5Bkeywords%5D%5B0%5D=data+engineer' },
  { name: 'JobVision: Algorithm Data Engineer posting', url: 'https://jobvision.ir/jobs/1514295' },
  { name: 'Iran Talent: Data Science & AI salary 1405', url: 'https://salary.irantalent.com/salary-report/data-science-ai-expert-salary/' },
  { name: 'Iran Talent: best employers in Iran (via Digiato)', url: 'https://digiato.com/iran-technology-news/iran-talent-report-of-the-best-employers-in-iran' },
  { name: 'Fatehi School: programmer salaries 1404', url: 'https://fatehischool.ir/programing-salary-iran/' },
  { name: 'Digikala: hiring process report', url: 'https://www.digikala.com/mag/digikala-hiring-process-hr-report-2/' },
  { name: 'Zibal Senior Data Engineer', url: 'https://jobinja.ir/companies/zibal/jobs/tE5k/' },
  { name: 'Telewebion Senior Data Engineer', url: 'https://jobinja.ir/companies/simra-telewebion/jobs/tNEn/' },
  { name: 'Adin Data Engineer', url: 'https://jobinja.ir/companies/zarebin-1/jobs/t7db/' },
  { name: 'Booran Group Senior Data Engineer', url: 'https://jobinja.ir/companies/boorangroup/jobs/tERF/' },
  { name: 'Bitpin Senior Data Engineer', url: 'https://jobinja.ir/companies/bitpin/jobs/tmX1/' },
  { name: 'Azki Senior Data Engineer', url: 'https://jobinja.ir/companies/azki/jobs/tjA7/' },
  { name: 'Digipay Data Engineer', url: 'https://jobinja.ir/companies/digipay/jobs/AOYq/' },
  { name: 'SnappPay Senior Data Engineer', url: 'https://jobinja.ir/companies/snapp-pay/jobs/Awmm/' },
  { name: 'Fanavin Data Engineer', url: 'https://jobinja.ir/companies/fanavin/jobs/tUhK/' },
  { name: 'Irancell Labs Senior Data Engineer', url: 'https://jobinja.ir/1506430' },
];
