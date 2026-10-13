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
export const resources = {
  books: [
    ['Designing Data-Intensive Applications', 'Martin Kleppmann', 'P0', 'Core theory', 'تئوری هسته'],
    ['Fundamentals of Data Engineering', 'Joe Reis & Matt Housley', 'P0', 'Lifecycle & architecture', 'چرخه‌ی عمر و معماری'],
    ['The Data Warehouse Toolkit', 'Ralph Kimball & Margy Ross', 'P0', 'Modeling', 'مدل‌سازی'],
    ['Streaming Systems', 'Tyler Akidau et al.', 'P1', 'Streaming theory', 'تئوری استریم'],
    ['Spark: The Definitive Guide / Learning Spark', '', 'P1', 'Spark', 'اسپارک'],
    ['High Performance Spark', '', 'P2', 'Tuning', 'بهینه‌سازی'],
    ['Kafka: The Definitive Guide', '', 'P1', 'Kafka', 'کافکا'],
    ['Database Internals', 'Alex Petrov', 'P2', 'Storage engines', 'موتورهای ذخیره‌سازی'],
    ['Data Pipelines Pocket Reference', 'James Densmore', 'P2', 'Quick read', 'مطالعه‌ی سریع'],
  ],
  practice: [
    ['SQL', 'DataLemur, LeetCode SQL (50 + Hard), StrataScratch', 'SQL', 'DataLemur، LeetCode SQL ‏(۵۰ + Hard)، StrataScratch'],
    ['Coding', 'LeetCode Easy/Medium — hash table, heap, intervals, sliding window', 'کدنویسی', 'LeetCode آسان/متوسط — hash table، heap، intervals، sliding window'],
    ['System design', 'HelloInterview (method), Track 14 of this site', 'طراحی سیستم', 'HelloInterview (روش)، مسیر ۱۴ همین سایت'],
  ],
  docs: 'Kafka, Flink, Spark, Iceberg, ClickHouse, StarRocks, Airflow, dbt — architecture/concepts pages only',
  docs_fa: 'مستندات Kafka، Flink، Spark، Iceberg، ClickHouse، StarRocks، Airflow، dbt — فقط صفحات معماری و مفاهیم',
};
