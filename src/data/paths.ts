// Curated study paths. Every id must exist in src/content/topics/en (checked at build time in paths/index.astro).
export interface StudyPath {
  id: string;
  icon: string;
  en: { title: string; blurb: string };
  fa: { title: string; blurb: string };
  tids: string[];
}

export const paths: StudyPath[] = [
  {
    id: 'two-weeks-junior',
    icon: '🌱',
    en: { title: '2 weeks to a junior interview', blurb: 'SQL, Python and warehouse basics, the pieces nearly every junior loop tests. About 45 hours.' },
    fa: { title: '۲ هفته تا مصاحبهٔ جونیور', blurb: 'SQL، پایتون و مبانی انبار داده؛ چیزهایی که تقریباً هر مصاحبهٔ جونیور می‌پرسد. حدود ۴۵ ساعت.' },
    tids: ['de-role-overview', 'interview-loop', 'how-to-prepare', 'sql-fundamentals', 'sql-joins', 'sql-nulls', 'sql-subqueries-ctes', 'sql-window-functions', 'sql-advanced-grouping', 'python-core', 'python-dataframes', 'oltp-vs-olap', 'etl-vs-elt', 'dwh-fundamentals', 'normalization', 'star-snowflake-obt', 'data-formats', 'orchestration-concepts', 'star-framework'],
  },
  {
    id: 'sql-master',
    icon: '🧮',
    en: { title: 'SQL deep dive', blurb: 'From joins to window functions, recursive CTEs, indexing and query plans. The most tested skill.' },
    fa: { title: 'SQL در عمق', blurb: 'از join تا window function، CTE بازگشتی، ایندکس و query plan؛ پرتکرارترین مهارت مصاحبه.' },
    tids: ['sql-fundamentals', 'sql-joins', 'sql-nulls', 'sql-subqueries-ctes', 'sql-advanced-grouping', 'sql-window-functions', 'sql-datetime', 'sql-recursive-ctes', 'sql-ddl-merge', 'sql-transactions', 'sql-indexing', 'sql-query-optimization', 'sql-dialects', 'sql-interview-patterns'],
  },
  {
    id: 'modeling',
    icon: '🏗️',
    en: { title: 'Data modeling track', blurb: 'Normalization, dimensional modeling, SCDs, fact tables and the modeling case questions.' },
    fa: { title: 'مسیر مدل‌سازی داده', blurb: 'نرمال‌سازی، مدل‌سازی ابعادی، SCD، جدول‌های fact و سؤال‌های موردی مدل‌سازی.' },
    tids: ['modeling-levels', 'normalization', 'dimensional-modeling', 'star-snowflake-obt', 'fact-table-types', 'scd', 'advanced-dimensions', 'event-modeling', 'modeling-methodologies', 'physical-modeling-olap', 'schema-evolution', 'semantic-layer', 'modeling-problems'],
  },
  {
    id: 'spark-batch',
    icon: '⚡',
    en: { title: 'Spark and batch processing', blurb: 'Architecture, shuffle, joins and skew, memory tuning, AQE and debugging with the Spark UI.' },
    fa: { title: 'Spark و پردازش دسته‌ای', blurb: 'معماری، shuffle، join و skew، تنظیم حافظه، AQE و دیباگ با Spark UI.' },
    tids: ['mapreduce', 'hadoop-ecosystem', 'spark-architecture', 'spark-shuffle', 'spark-joins-skew', 'spark-catalyst', 'spark-aqe', 'spark-caching', 'spark-memory-tuning', 'spark-writes', 'small-files', 'spark-ui-debugging', 'pyspark-api'],
  },
  {
    id: 'streaming',
    icon: '🌊',
    en: { title: 'Streaming and Kafka', blurb: 'Logs and queues, Kafka internals, delivery semantics, Flink, state, time and joins.' },
    fa: { title: 'استریمینگ و Kafka', blurb: 'لاگ و صف، معماری Kafka، delivery semantics، Flink، state، زمان و join.' },
    tids: ['queues-vs-logs', 'kafka-architecture', 'kafka-producers', 'kafka-consumers', 'kafka-retention-compaction', 'delivery-semantics', 'kafka-exactly-once', 'stream-concepts', 'time-and-ordering', 'streaming-state', 'stream-joins', 'flink', 'spark-structured-streaming', 'backpressure', 'lambda-kappa'],
  },
  {
    id: 'lakehouse',
    icon: '🏔️',
    en: { title: 'Lakehouse and warehouses', blurb: 'Object storage, table formats, file layout, catalogs and the OLAP engines you may be asked about.' },
    fa: { title: 'Lakehouse و انبار داده', blurb: 'ذخیره‌سازی شیء‌محور، table formatها، چیدمان فایل، catalog و موتورهای OLAP.' },
    tids: ['columnar-storage', 'compression', 'data-lake-object-storage', 'lakehouse-table-formats', 'file-layout', 'medallion-architecture', 'catalogs', 'cloud-warehouses', 'trino', 'clickhouse', 'starrocks-doris', 'realtime-olap', 'serving-layer'],
  },
  {
    id: 'senior-design',
    icon: '🧠',
    en: { title: 'Senior system-design focus', blurb: 'Distributed-systems theory, ingestion patterns and the design problems senior loops use.' },
    fa: { title: 'تمرکز بر طراحی سیستم (سنیور)', blurb: 'تئوری سیستم‌های توزیع‌شده، الگوهای ingestion و مسئله‌های طراحی مصاحبهٔ سنیور.' },
    tids: ['delivery-framework', 'data-estimation', 'senior-expectations', 'distributed-fundamentals', 'cap-pacelc', 'consistency-models', 'consensus', 'partitioning-sharding', 'replication', 'cdc', 'dedup-idempotent-loads', 'pattern-exactly-once', 'pattern-late-data', 'pattern-backfill', 'design-ad-click-aggregator', 'design-cdc-lake', 'design-fraud-detection', 'design-platform-from-scratch', 'tech-selection', 'senior-signals'],
  },
  {
    id: 'platform-ops',
    icon: '🛠️',
    en: { title: 'Pipelines, quality and operations', blurb: 'Orchestration, dbt, data quality, observability, governance and running data systems in production.' },
    fa: { title: 'پایپ‌لاین، کیفیت و عملیات', blurb: 'orchestration، dbt، کیفیت داده، observability، governance و اجرای سیستم‌های داده در production.' },
    tids: ['orchestration-concepts', 'airflow', 'dbt', 'pipeline-design-principles', 'backfills', 'data-quality', 'data-observability', 'pipeline-monitoring', 'data-lineage', 'data-catalogs', 'governance-contracts-mesh', 'dataops-cicd', 'capacity-planning'],
  },
  {
    id: 'behavioral',
    icon: '💬',
    en: { title: 'Behavioral and career', blurb: 'STAR stories, project narratives, post-mortems, the questions to ask and negotiation.' },
    fa: { title: 'مصاحبهٔ رفتاری و مسیر شغلی', blurb: 'داستان‌های STAR، روایت پروژه، postmortem، سؤال‌هایی که باید بپرسی و مذاکره.' },
    tids: ['star-framework', 'behavioral-questions', 'project-narratives', 'incidents-postmortems', 'questions-to-ask', 'negotiation-iran', 'iran-de-market'],
  },
];
