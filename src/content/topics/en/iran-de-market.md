---
order: 1
tid: "iran-de-market"
track: "0"
section: null
num: "0.2"
title: "The Iranian Data Engineering Market"
type: "guide"
level: "beginner"
priority: "P0"
est_hours: 2
summary: "Iranian companies fall into three broad groups: large product companies (e-commerce, ride-hailing, classifieds, app stores, fintech), telcos/banks/banking-software vendors, and startups. Almost everything is on-prem because public cloud is effectively unavailable. Common stacks: Kafka, Spark, ClickHouse, Airflow, PostgreSQL, and Hadoop/Hive plus Oracle in banks and telcos. Analyze job postings of your target companies."
covers: ["company segments","typical stacks","on-prem implications","job posting analysis method","where to find postings (Jobinja, Jobvision, Quera, LinkedIn)","salary ranges research"]
interview_qs: []
extras: [{"label":"practice","items":["Collect 15 senior DE job postings → tag technologies → build a frequency table → adjust priorities in this roadmap"]}]
prerequisites: []
related: ["de-role-overview", "onprem-iran", "hadoop-ecosystem", "kafka-architecture", "spark-architecture", "encoding-persian-data", "negotiation-iran"]
---

## 🎯 TL;DR

> The Iranian data engineering market is mostly **on-prem**, mixes **modern open-source stacks** (Kafka, Spark, ClickHouse, Airflow, PostgreSQL) with **older enterprise stacks** (Hadoop/Hive, Oracle), and is segmented by company type. Your target segment decides which tools you must know.

- Three company groups: **product companies**, **telcos, banks and banking-software vendors**, and **startups**.
- Public cloud is largely not an option for many teams, so skills in self-managed clusters, capacity planning and upgrades count more than they would in a cloud-first market.
- The reliable way to know what a company wants is to **read its job postings**, not to rely on blog posts or rumours.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A city has different kinds of workplaces: a fast-moving shopping mall, a government office with old filing cabinets, and a new coworking space. The same job title ("data engineer") means different daily work in each one. Before you prepare, find out which building you want to work in.

The three segments, in plain words:

| Segment | Examples of business | What the data looks like | What they usually run |
|---|---|---|---|
| Product companies | e-commerce, ride-hailing, classifieds, app stores, fintech | User events, orders, payments, search logs, very high volume | Kafka, Spark, ClickHouse, Airflow, PostgreSQL |
| Telcos, banks, banking software | mobile operators, banks, vendors that build banking systems | Call records, transactions, regulated reports, strict audit | Hadoop/Hive, Oracle, Spark, batch-heavy ETL |
| Startups | early-stage products of any kind | Small, changing, often one team does everything | PostgreSQL, a managed or simple orchestrator, a few Spark or Python jobs |

These are broad patterns, not rules. Verify each target company on its own.

## 🔧 How it really works (Mid)

### Why on-prem changes the job

Global public cloud providers do not offer their services to customers in Iran in the normal way, so most companies run their own data centres or private infrastructure. For a data engineer this means:

- You manage **clusters** yourself: Kafka brokers, Spark on YARN or Kubernetes, ClickHouse replicas, Airflow workers.
- Storage is a **capacity decision**, not a bill you can scale with one click. Disk, network and hardware refresh cycles matter.
- Upgrades, backups and disaster recovery are your work. A "managed service" answer is not enough in an interview.
- Some tools that are normal in the West (for example some SaaS ingestion connectors or hosted warehouses) may be unavailable or hard to justify. Interviewers often ask how you would build the same thing self-hosted.

### A repeatable method: analyse job postings

Do this before you spend months on a topic list.

1. Pick 15 to 20 recent postings for senior data engineer roles at companies you like. Sources include Jobinja, Jobvision, Quera and LinkedIn.
2. Copy each posting's requirements into a plain text file.
3. Tag every technology mentioned (Kafka, Spark, Airflow, Hive, Oracle, dbt, ...) and every soft requirement (on-call, mentoring, ownership).
4. Build a frequency table with a short script:

```python
from collections import Counter

# one set of tags per posting, written by hand after reading it
postings = [
    {"kafka", "spark", "airflow", "clickhouse"},
    {"spark", "hive", "oracle", "airflow"},
    {"kafka", "flink", "postgresql"},
    {"spark", "hive", "hdfs", "yarn"},
]

counts = Counter(tag for tags in postings for tag in tags)
for tag, n in counts.most_common():
    print(f"{tag:12} {n}/{len(postings)}")
```

5. Compare the top tags with your roadmap. If your target segment asks for Hive and HDFS and your plan has none of it, change the plan.

> **⚠️ Watch out:** Job postings describe what a company *asks for*, not what it *runs*. Use them to prioritise topics, then confirm in interviews.

### Salaries and market information

Salary numbers are hard to verify and change quickly. Use several sources (job boards, community reports, the offers your peers actually received), write down the date and the role level, and never quote a single number as fact. In an interview, asking about the team's stack and scale is more useful than asking about money.

## 🧠 Senior depth & trade-offs

Senior candidates are expected to understand the constraints of this market, not just list tools.

- **Scale in telcos and banks:** call detail records and card transactions can reach billions of rows per day. A design that works for millions is not enough; talk about partitioning, retention and batch windows.
- **Hadoop is still alive in some places.** If the job asks for HDFS, YARN and Hive, know the small-files problem, partition layout and why batch windows are long. Also know when moving to a lakehouse table format is a good argument and when it is not realistic in the company's situation.
- **Persian data is a real engineering problem.** Mixed Arabic and Persian characters (ی/ي, ک/ك), Persian and Arabic digits, the zero-width non-joiner, Jalali dates and timezone policy changes can break joins and reports silently. A candidate who has handled this stands out. See [[encoding-persian-data|Persian encoding and normalisation]].
- **Capacity and cost:** on fixed hardware, the question is not "how do I scale up" but "what will this cluster run at peak, and what gets delayed when it is full?" Prioritisation and quotas are design topics here.
- **Security and compliance:** banks and telcos have strict rules about who can read personal data. Column-level masking and audit logs are design requirements.
- **When NOT to copy a Western design:** a five-service streaming platform is a poor fit for a startup with two engineers. Say what you would build first and what you would postpone.

## 🧪 Try it

1. Collect 15 senior data engineer postings from two job boards and one company career page.
2. Tag the technologies with the script above (extend the list as you go).
3. Write three sentences: the top three tools, the one tool you do not yet know, and the change you will make to your study plan.

<details><summary>What a good result looks like</summary>

You will usually see Kafka, Spark, Airflow and SQL near the top, with Hive, Oracle or ClickHouse depending on the segment. The useful output is not the list itself but the decision: "I will study Kafka exactly-once before Flink, because 9 of 15 postings ask for Kafka and 2 ask for Flink."

</details>

## 💬 Interview corner

**Question:** "Which stack would you expect to use in a company like ours, and how would you run it without a public cloud?"

> **🎤 Say this in the interview:** "I would first ask whether the team is product, telco or bank, because the stack and the batch-versus-stream mix differ. For a product team I would expect Kafka and Spark with ClickHouse for serving; for a bank I would expect Hive and batch windows with strict audit. In either case I would plan the hardware, backups and upgrades myself, because a managed service is not the default here."

- **Junior answer:** names popular tools without asking about the company.
- **Mid answer:** knows the difference between on-prem and cloud operations and can name the operational tasks.
- **Senior answer:** connects the segment to the stack, to scale and to the cost of running it, and states what they would build first.

## ⚠️ Common mistakes

- **Preparing only for cloud tools.** Many local roles expect self-managed clusters. Study the operations, not just the APIs.
- **Trusting one forum post about salaries or stacks.** Check several sources and the posting dates.
- **Ignoring Persian-specific data issues.** Interviewers in Iran often ask about them because they are common and easy to get wrong.
- **Studying every tool equally.** Use the frequency table to decide the order.

## ✅ Check yourself (quiz)

1. Which segment most often runs Hadoop/Hive and Oracle?
   - A) Early-stage startups
   - B) Telcos, banks and banking-software vendors
   - C) Pure research labs
   - D) Freelance agencies

<details><summary>Answer</summary>

**B** — telcos and banks have long-lived enterprise systems and batch-heavy reporting, which is where these tools remain common. Startups often choose newer, smaller stacks.

</details>

2. You tagged 15 postings and "Kafka" appears in 12 of them, while "Hive" appears in 2. Your target is a product company. What is the best use of this result?
   - A) Ignore it; postings are unreliable
   - B) Prioritise Kafka and its operations (consumer lag, exactly-once) and treat Hive as secondary
   - C) Study Hive first because it is older
   - D) Stop applying to product companies

<details><summary>Answer</summary>

**B** — postings show demand. The frequency table should set your study order. Hive still matters if your target segment uses it, which is why the segment question comes first.

</details>

3. **(Senior)** A bank asks you to design a daily batch of 2 billion card transactions on self-managed Spark and Hive. Which concern should you raise first?
   - A) Whether the bank will adopt a serverless warehouse
   - B) Partitioning and the batch window: how the data is laid out, how long the job runs at peak, and what happens when it misses its window
   - C) Whether to use Python instead of Scala
   - D) Whether to remove all indexes

<details><summary>Answer</summary>

**B** — at this scale the design problem is data layout (partitions, file sizes), the time budget of the batch and recovery when it overruns. Tool preference is secondary.

</details>

4. **(Senior)** Why can a mixed Arabic/Persian text field break a join silently?
   - A) Persian uses a different SQL dialect
   - B) The same visible letter can have different code points (for example Arabic ya and Persian ya), so equal-looking strings do not match unless normalised
   - C) Joins cannot run on text columns
   - D) Persian digits are stored as floats

<details><summary>Answer</summary>

**B** — visually identical strings can be different bytes. Normalise both sides (one canonical character set and digit set) before joining or deduplicating, and test with real Persian data.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| The three company segments in Iran | Product companies, telcos/banks/banking software, startups |
| Typical stack of a product company | Kafka, Spark, ClickHouse, Airflow, PostgreSQL (verify per company) |
| Typical stack of a bank or telco | Hadoop/Hive, Oracle, batch-heavy Spark ETL |
| Why on-prem changes the job | You own cluster capacity, upgrades, backups and disaster recovery |
| Best source for market signal | Recent job postings for the target role and level |
| Why job postings are not enough | They show what is requested, not what is running; confirm in interviews |
| Common Persian data trap | Arabic and Persian letter variants and digits look the same but differ in code points |

## 🔗 Go further

- **de-role-overview** explains the role before you compare segments.
- **onprem-iran** covers the operational side of self-managed infrastructure.
- **hadoop-ecosystem** and **kafka-architecture** are good next steps for the most common enterprise and product stacks.
- **encoding-persian-data** goes deeper into normalisation and Jalali dates.
- **negotiation-iran** covers offers and compensation once your market research is done.
