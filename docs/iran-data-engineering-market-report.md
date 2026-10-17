# Data Engineering in Iran: market report (Mehr 1405 / Oct 2026)

Bilingual version with charts: `/en/market/` and `/fa/market/` on the site (data in `src/data/market.ts`).

## Method and limits

- 13 data-engineer postings read in full (12 Jobinja, 1 JobVision), plus Iran Talent and Fatehi School salary pages and the Iran Talent best-employer report.
- JobVision renders results with JavaScript and gates detail behind login; its keyword pages returned nothing to the crawler. Treat JobVision coverage as thin.
- Most ads hide salary ("negotiable"). Salary figures below are ranges, monthly, million toman. Iran Talent numbers are net and mostly requested salaries.
- Sample is small (13). Percentages describe this sample only.

## Key findings

1. 11 of 13 postings want 3+ years; no pure entry-level role found (only internships).
2. Default stack: Python + SQL + Airflow + Kafka + Spark + ClickHouse, self-hosted on Linux/Docker/Kubernetes. Managed cloud warehouses barely appear.
3. ClickHouse appears in 8 of 13, unusually high versus other markets.
4. Main hirers: fintech (Zibal, Digipay, Azki, SnappPay, Bitpin), telecom (Irancell Labs), media (Telewebion), banking-data shops (SSIS/ODI/SQL Server).
5. Newest ads add Debezium CDC, Iceberg/Hudi, OpenMetadata, vector DBs (Qdrant) for AI.

## Salary (monthly, million toman)

| Level | Years | Our estimate (Tehran) |
|---|---|---|
| Junior | 0-2 | 25-45 |
| Mid | 2-5 | 50-90 |
| Senior | 5+ | 90-150+ |

The estimate is ours, derived from the evidence below.

| Source | Scope | Value | Reliability |
|---|---|---|---|
| JobVision, Algorithm Data & AI | Data Engineer, 2+ yrs | 80-100 | Stated by employer |
| Jobinja, Booran Group | Senior, 4+ yrs | from 80 | Stated minimum |
| Jobinja coverage | Top ad, DE with Python | up to 110 | Highest ad seen |
| Iran Talent, DS & AI specialist 1405 (Tehran, net) | P10/P25/median/P75/P90 | 28/37/50/71/85 | Requested, adjacent role |
| Iran Talent, same role 1404 | same | 20/26/35/45/60 | Received |
| Fatehi School 1404, DS/ML | Jr/Mid/Sr | 25-40 / 40-60 / 60-100 | Course seller, no method |
| Fatehi School 1404, DevOps | Jr/Mid/Sr | 20-40 / 40-75 / 75-130 | Same caveat |
| Programmer-salary aggregator 1405, DE/ETL | 0-2 / 2-5 / 5-10 yrs | 22-45 / 50-100 / 105-200 | Unsourced, upper bound |

Notes: outside Tehran about 25-30% lower (large-city median 37 vs 50); variable pay only 4-10%; median grew about 42% from 1404 to 1405; overtime, New Year bonus and severance are legal rights.

## Technologies (mentions in 13 postings)

SQL 13 · Python 12 · Airflow 10 · Kafka 9 · Docker/Kubernetes 9 · Spark 8 · ClickHouse 8 · Lakehouse/S3/MinIO/Iceberg/Parquet 5 · Git/CI-CD 4 · Metabase/Superset 4 · NoSQL 4 · LLM/vector DB 3 · SSIS/ODI 1

## What to learn (in order)

1. SQL and PostgreSQL internals, ClickHouse basics (P0)
2. Python for data, testable code (P0)
3. Linux, Docker, GitLab CI, then Kubernetes/Helm (P0)
4. Data modeling and warehousing (P0)
5. Airflow (P0)
6. Kafka, Kafka Connect, Debezium CDC (P0)
7. Spark / PySpark / Structured Streaming (P1)
8. ClickHouse MergeTree family, materialized views (P1)
9. Lakehouse: Parquet, Iceberg/Hudi, MinIO/S3, Trino (P1)
10. Data quality, observability, lineage, SLOs (P1)
11. AI-adjacent: vector DBs, embedding pipelines (P2)
12. Distributed-systems theory: CAP, idempotency, Raft/Paxos (P2)

## Companies

| Company | Sector | Stack | Level | Pay | Perks |
|---|---|---|---|---|---|
| Adin | IT services | ClickHouse, Iceberg, Hudi, Spark, Airflow, Trino | Senior, 1+ yr | Negotiable | Equity/profit share on KPIs, 1-2 remote days, uncapped overtime, New Year bonus, severance, flexible hours |
| Zibal | Fintech | Python, Airflow, Spark/dbt, Kafka, Postgres, Mongo | Senior, 3-6 | Negotiable | Supplementary insurance, loans, flexible hours, military-service placement, welfare credit, development budget, breakfast, game room |
| Algorithm Data & AI | Banking data | SQL Server, SSIS, ODI, ClickHouse | Mid, 2+ | 80-100 | Not listed; 07:00-16:00 |
| Booran Group | E-commerce marketing | PostgreSQL, CDC, APIs | Senior, 4+ | from 80 | Not listed |
| Telewebion | Streaming/CDN | ClickHouse, Kafka, Spark/Flink/Trino, Iceberg, Ceph | Senior, 5+ | Negotiable | Not listed; highest technical bar |
| Irancell Labs | Telecom | Spark, Kafka, ClickHouse, Airflow, MinIO, Superset | Senior, 5+ | Negotiable | Not listed |
| Bitpin | Crypto exchange | Airflow, PySpark, Postgres to ClickHouse, Debezium, Qdrant | Senior | Not listed | Not listed |
| Azki | Insurtech | Kafka, Spark, ClickHouse, Airflow, Metabase | Senior, 5+ | Not listed | Not listed |
| SnappPay (2024) | BNPL fintech | Airflow, Spark, Kafka, ClickHouse, K8s | Senior | Not listed | Not listed |
| Digipay (2024) | Payments | Kafka, ClickHouse, Python, Airflow | Mid, 3+ | Not listed | Not listed |
| Fanavin | Enterprise AI | Python, DMBOK data quality, LLMs | Mid, 3-6 | Negotiable | Insurance, lunch, loans, flexible hours, sports, military help |

Employer rankings (Iran Talent, general employees, not DE-specific): Cafe Bazaar is 1st for learning, work-life balance, promotion and culture; no tech company is in the top salary-and-benefits list. Companies that publish pay or perks (Zibal, Adin, Fanavin, Algorithm, Booran) tend to be more transparent.

## Hiring rules

- Military service: completion/exemption usually required; Zibal and Fanavin offer amrieh placement.
- Gender: mostly "not important"; some ads restrict (Fanavin listed men only). Read the ad first.
- Education: bachelor's in CS/IT is the floor. English: medium, but docs are English.
- Digikala example: 2-3 interviews plus technical and behavioural assessments.

## Action plan

1. Build a portfolio pipeline in the local stack: PostgreSQL, Debezium, Kafka, ClickHouse, Airflow, Docker Compose, data-quality checks, Metabase. Publish on GitHub.
2. Master Python, SQL, Airflow, Kafka first; add Spark and ClickHouse (covers about 80% of postings read).
3. For a first job also search ETL developer, BI developer, data analyst and internships.
4. Practise on this site: question bank, mock interview, SQL playground, design drill.

## Sources

- Jobinja Data Engineer search: https://jobinja.ir/jobs?filters%5Bkeywords%5D%5B0%5D=data+engineer
- JobVision Algorithm posting: https://jobvision.ir/jobs/1514295
- Iran Talent DS & AI salary 1405: https://salary.irantalent.com/salary-report/data-science-ai-expert-salary/
- Iran Talent best employers (Digiato): https://digiato.com/iran-technology-news/iran-talent-report-of-the-best-employers-in-iran
- Fatehi School salaries 1404: https://fatehischool.ir/programing-salary-iran/
- Digikala hiring process: https://www.digikala.com/mag/digikala-hiring-process-hr-report-2/
- Individual postings: Zibal (tE5k), Telewebion (tNEn), Adin (t7db), Booran (tERF), Bitpin (tmX1), Azki (tjA7), Digipay (AOYq), SnappPay (Awmm), Fanavin (tUhK), Irancell Labs (jobinja.ir/1506430)
