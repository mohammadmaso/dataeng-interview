export const plan = [
  { w: 1, en: 'Interview guide, SQL core, data formats, numbers', fa: 'راهنما، SQL پایه و window، فرمت‌ها، اعداد مرجع', tracks: '0, 1.6–1.8, 2.1–2.9', out_en: '25 SQL problems · job-posting analysis', out_fa: '۲۵ مسئله SQL · تحلیل آگهی‌های شغلی' },
  { w: 2, en: 'Data modeling + advanced SQL', fa: 'مدل‌سازی داده + SQL پیشرفته', tracks: '2.10–2.13, 6.1–6.9', out_en: '3 modeling exercises · 20 SQL problems', out_fa: '۳ تمرین مدل‌سازی · ۲۰ مسئله SQL' },
  { w: 3, en: 'Distributed systems & DB internals', fa: 'سیستم‌های توزیع‌شده و معماری دیتابیس', tracks: '4, 5', out_en: 'DDIA ch. 3, 5–9 notes · flashcards', out_fa: 'یادداشت DDIA فصل ۳ و ۵ تا ۹ · فلش‌کارت' },
  { w: 4, en: 'Spark deep dive + Python coding', fa: 'اسپارک عمیق + کدنویسی پایتون', tracks: '3, 7', out_en: 'Spark tuning notes · 10 coding problems', out_fa: 'یادداشت tuning اسپارک · ۱۰ مسئله کدنویسی' },
  { w: 5, en: 'Kafka + stream processing + Flink', fa: 'کافکا + پردازش جریانی + فلینک', tracks: '8', out_en: '2 design problems (Ad click, Ride-hailing)', out_fa: '۲ مسئله طراحی (Ad click، Ride-hailing)' },
  { w: 6, en: 'Ingestion, CDC, lakehouse, ClickHouse/StarRocks', fa: 'دریافت، CDC، لیک‌هاوس، ClickHouse/StarRocks', tracks: '9, 10', out_en: '2 design problems (CDC lake, Telco CDR)', out_fa: '۲ مسئله طراحی (CDC lake، Telco CDR)' },
  { w: 7, en: 'Orchestration, quality, platform + system design drills', fa: 'ارکستراسیون، کیفیت، پلتفرم + تمرین طراحی', tracks: '11, 12, 13, 14', out_en: '4 timed design problems · project narratives drafted', out_fa: '۴ مسئله طراحی زمان‌دار · پیش‌نویس داستان پروژه‌ها' },
  { w: 8, en: 'Mock interviews, behavioral, weak-spot review', fa: 'مصاحبه‌ی آزمایشی، رفتاری، مرور نقاط ضعف', tracks: '14, 16, review', out_en: '3+ mocks · story bank final · cheat sheets', out_fa: '۳+ مصاحبه‌ی آزمایشی · بانک داستان نهایی · چیت‌شیت' },
];
export type Book = { title: string; author: string; isbn: string; pri: 'P0' | 'P1' | 'P2'; topic: [string, string]; why: [string, string] };
export type Link = { name: string; url: string; note: [string, string] };

export const resources = {
  books: [
    { title: 'Designing Data-Intensive Applications', author: 'Martin Kleppmann', isbn: '9781449373320', pri: 'P0', topic: ['Core theory', 'تئوری هسته'], why: ['The single best book on replication, partitioning, transactions, consensus and streams. Re-read chapters 3, 5-9, 11.', 'بهترین کتاب درباره‌ی replication، partitioning، تراکنش، consensus و استریم. فصل‌های ۳، ۵ تا ۹ و ۱۱ را دوباره بخوانید.'] },
    { title: 'Fundamentals of Data Engineering', author: 'Joe Reis & Matt Housley', isbn: '9781098108304', pri: 'P0', topic: ['Lifecycle & architecture', 'چرخه‌ی عمر و معماری'], why: ['The data engineering lifecycle and the vocabulary interviewers expect.', 'چرخه‌ی عمر مهندسی داده و واژگانی که مصاحبه‌گر انتظار دارد.'] },
    { title: 'The Data Warehouse Toolkit (3rd ed.)', author: 'Ralph Kimball & Margy Ross', isbn: '9781118530801', pri: 'P0', topic: ['Modeling', 'مدل‌سازی'], why: ['Dimensional modeling: grain, facts, dimensions, SCDs, bus matrix.', 'مدل‌سازی بعدی: grain، fact، dimension، SCD و bus matrix.'] },
    { title: 'Streaming Systems', author: 'Tyler Akidau, Slava Chernyak & Reuven Lax', isbn: '9781491983874', pri: 'P1', topic: ['Streaming theory', 'تئوری استریم'], why: ['Event time, watermarks, windows and triggers explained from first principles.', 'event time، watermark، window و trigger از پایه.'] },
    { title: 'Spark: The Definitive Guide', author: 'Bill Chambers & Matei Zaharia', isbn: '9781491912218', pri: 'P1', topic: ['Spark', 'اسپارک'], why: ['Complete Spark reference; pair with Learning Spark 2nd ed. for newer APIs.', 'مرجع کامل Spark؛ برای APIهای جدیدتر با Learning Spark ویرایش دوم ترکیب کنید.'] },
    { title: 'Learning Spark (2nd ed.)', author: 'Jules Damji et al.', isbn: '9781492050049', pri: 'P1', topic: ['Spark', 'اسپارک'], why: ['A shorter, modern intro: DataFrames, Structured Streaming, Delta.', 'مقدمه‌ی کوتاه‌تر و مدرن: DataFrame، Structured Streaming و Delta.'] },
    { title: 'High Performance Spark', author: 'Holden Karau & Rachel Warren', isbn: '9781491943205', pri: 'P2', topic: ['Tuning', 'بهینه‌سازی'], why: ['Shuffles, skew, memory and partitioning in depth.', 'shuffle، skew، حافظه و partitioning به‌صورت عمیق.'] },
    { title: 'Kafka: The Definitive Guide (2nd ed.)', author: 'Gwen Shapira et al.', isbn: '9781492043089', pri: 'P1', topic: ['Kafka', 'کافکا'], why: ['Brokers, partitions, consumer groups, delivery semantics, exactly-once.', 'broker، partition، consumer group، semantics تحویل و exactly-once.'] },
    { title: 'Database Internals', author: 'Alex Petrov', isbn: '9781492040347', pri: 'P2', topic: ['Storage engines', 'موتورهای ذخیره‌سازی'], why: ['B-trees, LSM-trees, and distributed consensus in detail.', 'درخت B، LSM و consensus توزیع‌شده با جزئیات.'] },
    { title: 'Apache Iceberg: The Definitive Guide', author: 'Tomer Shiran et al.', isbn: '9781098148621', pri: 'P1', topic: ['Lakehouse', 'لیک‌هوس'], why: ['Table format internals: snapshots, manifests, partition evolution.', 'درون table format: snapshot، manifest و تکامل پارتیشن.'] },
    { title: 'Data Pipelines Pocket Reference', author: 'James Densmore', isbn: '9781492087830', pri: 'P2', topic: ['Quick read', 'مطالعه‌ی سریع'], why: ['A fast practical overview of ingestion, transform and orchestration.', 'مرور عملی و سریع ingestion، transform و orchestration.'] },
    { title: 'Data Mesh', author: 'Zhamak Dehghani', isbn: '9781492092391', pri: 'P2', topic: ['Org & architecture', 'سازمان و معماری'], why: ['Data products and domain ownership, a frequent senior-level topic.', 'محصول داده و مالکیت دامنه‌ای؛ موضوع پرتکرار سطح سنیور.'] },
    { title: 'Data Quality Fundamentals', author: 'Barr Moses et al.', isbn: '9781098112042', pri: 'P2', topic: ['Quality & observability', 'کیفیت و observability'], why: ['Testing, monitoring and incident handling for data pipelines.', 'تست، پایش و مدیریت حادثه برای pipelineهای داده.'] },
    { title: 'Designing Machine Learning Systems', author: 'Chip Huyen', isbn: '9781098107963', pri: 'P2', topic: ['ML platforms', 'پلتفرم ML'], why: ['Feature stores, training-serving skew and data for ML.', 'feature store، training-serving skew و داده برای ML.'] },
    { title: 'SQL Performance Explained', author: 'Markus Winand', isbn: '9783950307825', pri: 'P2', topic: ['SQL & indexes', 'SQL و ایندکس'], why: ['How indexes and execution plans really work.', 'ایندکس و plan اجرا واقعاً چطور کار می‌کنند.'] },
    { title: 'System Design Interview, Vol. 1', author: 'Alex Xu', isbn: '9798664653403', pri: 'P2', topic: ['System design', 'طراحی سیستم'], why: ['A repeatable framework for design rounds.', 'چارچوب تکرارپذیر برای راندهای طراحی.'] },
  ] as Book[],
  practice: [
    { name: 'DataLemur', url: 'https://datalemur.com', note: ['SQL interview questions from real companies', 'سؤال‌های SQL مصاحبه از شرکت‌های واقعی'] },
    { name: 'LeetCode SQL 50', url: 'https://leetcode.com/studyplan/top-sql-50/', note: ['Curated SQL study plan', 'برنامه‌ی مطالعه‌ی منتخب SQL'] },
    { name: 'StrataScratch', url: 'https://www.stratascratch.com', note: ['SQL and Python data questions', 'سؤال‌های SQL و پایتون'] },
    { name: 'NeetCode 150', url: 'https://neetcode.io/practice', note: ['Coding patterns: hash tables, heaps, intervals, sliding window', 'الگوهای کدنویسی: hash table، heap، intervals، sliding window'] },
    { name: 'HelloInterview', url: 'https://www.hellointerview.com/learn/system-design/in-a-hurry/introduction', note: ['System design delivery framework', 'چارچوب ارائه‌ی طراحی سیستم'] },
    { name: 'Mode SQL Tutorial', url: 'https://mode.com/sql-tutorial/', note: ['Window functions and analytics SQL basics', 'window function و مبانی SQL تحلیلی'] },
  ] as Link[],
  docs: [
    { name: 'Apache Kafka', url: 'https://kafka.apache.org/documentation/#design', note: ['Design chapter', 'فصل design'] },
    { name: 'Apache Flink', url: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/overview/', note: ['Concepts', 'مفاهیم'] },
    { name: 'Apache Spark', url: 'https://spark.apache.org/docs/latest/rdd-programming-guide.html', note: ['Programming guide and tuning', 'راهنمای برنامه‌نویسی و tuning'] },
    { name: 'Apache Iceberg', url: 'https://iceberg.apache.org/spec/', note: ['Table spec', 'مشخصات جدول'] },
    { name: 'Delta Lake', url: 'https://docs.delta.io/latest/delta-intro.html', note: ['Introduction and transaction log', 'مقدمه و transaction log'] },
    { name: 'ClickHouse', url: 'https://clickhouse.com/docs/en/guides/best-practices/sparse-primary-indexes', note: ['Sparse primary indexes', 'ایندکس‌های primary اسپارس'] },
    { name: 'StarRocks', url: 'https://docs.starrocks.io/docs/introduction/StarRocks_intro/', note: ['Architecture', 'معماری'] },
    { name: 'Apache Airflow', url: 'https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html', note: ['Core concepts', 'مفاهیم هسته'] },
    { name: 'dbt', url: 'https://docs.getdbt.com/docs/build/projects', note: ['Projects and models', 'پروژه‌ها و مدل‌ها'] },
  ] as Link[],
  articles: [
    { name: 'The Log: What every software engineer should know', url: 'https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying', note: ['Jay Kreps on logs as the core abstraction', 'جی کرپس درباره‌ی لاگ به‌عنوان انتزاع اصلی'] },
    { name: 'The Dataflow Model (paper)', url: 'https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/', note: ['Origin of event time, windows and triggers', 'خاستگاه event time، window و trigger'] },
    { name: 'Streaming 101 / 102', url: 'https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/', note: ['Tyler Akidau\'s streaming primers', 'مقدمه‌های استریم از تایلر آکیدائو'] },
    { name: 'Dynamo (paper)', url: 'https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf', note: ['Consistent hashing, quorums, eventual consistency', 'consistent hashing، quorum و eventual consistency'] },
    { name: 'Spark RDD paper', url: 'https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf', note: ['Why Spark works the way it does', 'چرا Spark اینطور کار می‌کند'] },
  ] as Link[],
};
