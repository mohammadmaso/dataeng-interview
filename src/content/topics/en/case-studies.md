---
order: 182
tid: "case-studies"
track: "14"
section: "14D"
num: "14D"
title: "In the Wild: Engineering Case Studies"
type: "concept"
level: "advanced"
priority: "P1"
est_hours: 8
summary: "Reading engineering write-ups from large companies to have real examples in interviews. Each case study will get a Persian summary and key lessons on the site."
covers: []
interview_qs: []
extras: [{"label":"candidates","items":["Uber (Hudi origin, real-time platform)","Netflix (Iceberg origin, data platform)","LinkedIn (Kafka origin)","Airbnb (Minerva metrics layer)","Spotify (event delivery)","Cloudflare (ClickHouse for analytics)","Discord (data storage evolution)","Iranian engineering blogs (Virgool, company tech blogs) on Kafka/ClickHouse/Spark experiences"]}]
prerequisites: ["kafka-architecture", "lakehouse-table-formats", "realtime-olap"]
related: ["how-to-prepare", "interview-loop", "pattern-cdc-analytics", "design-cdc-lake", "incidents-postmortems", "design-platform-from-scratch"]
---

## 🎯 TL;DR

> An engineering case study is a **story of one team solving one problem under one set of constraints**. Its value in an interview is not the tool names. It is the reasoning: what was tried, what was rejected, and why.

- Read a write-up in this order: **context and scale, problem, constraints, options, decision, results, regrets**.
- Extract **transferable principles** ("keep an immutable log so consumers can replay"), not copyable architectures.
- Numbers only count with their conditions: hardware, data volume, workload, and the date of the post.
- In an interview, say "Company X faced a similar problem and chose Y because Z", then state the trade-off in your own words.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A case study is like a restaurant review written by the chef. It is useful: it tells you why the kitchen made certain choices. It is not a recipe you can copy, because your kitchen, suppliers and customers are different.

Suppose a blog post says: "We moved our event pipeline to a log-based broker and our consumers can now replay a week of data after a bug." A junior reader might write down "use Kafka". A better reader writes down the principle:

- **Principle:** an append-only log lets consumers re-read history, so a bug fix can be followed by a replay instead of a manual repair.
- **Condition:** the team needed replay more than it needed low latency per message.
- **Question for your own design:** do my consumers need to replay, and for how long?

That three-line note is what you bring to an interview. The tool name is the least important part.

## 🔧 How it really works (Mid)

A repeatable reading method turns any long post into notes you can use. Ask these questions in order.

| Step | Question to answer | What to write down |
|---|---|---|
| 1. Context | Who is the company, what product, what scale, what year? | Scale numbers with units and date |
| 2. Problem | What broke or what could not be done? | One sentence, in the team's words |
| 3. Constraints | Latency, cost, team size, existing systems, regulation? | The hard limits |
| 4. Options | What else did they consider? | At least two rejected options |
| 5. Decision | What did they choose, and why that and not the others? | The deciding constraint |
| 6. Results | What improved, by how much, measured how? | The metric and the baseline |
| 7. Regrets | What did they say they would change? | The honest lessons |

The result is a short note, not a summary of the whole post. This note is a hypothetical example to show the shape, not a real post:

```text
Post (hypothetical): "Moving the event pipeline to a log-based broker"
Scale: placeholders, fill in from the real post (events per day, consumer teams)
Problem: no replay after a consumer bug; batch re-extracts took days
Constraint: existing batch consumers must keep working during the migration
Options: keep batch extracts; a new broker with a dual-write period; CDC from the source
Decision: log-based broker with dual-write, because it removed the extracts without a flag day
Result: a measured improvement, with its baseline (fill in from the real post)
Lesson: migrate by running both paths, and define a cutover criterion before you start
```

Each line of this note maps to a question an interviewer may ask. "Why not just batch?" is answered by the constraint line, and "how did you migrate?" by the decision line.

## 🧠 Senior depth & trade-offs

Reading well means judging the source as well as the content.

- **Survivorship and marketing.** A post is written after success. It rarely shows the failed attempts, the team that struggled, or the cost of the new system. Ask what is missing.
- **Scale does not transfer.** A design that handles hundreds of thousands of events per second may be wrong for a bank with 2 million events per day, where a single well-indexed database may be simpler and cheaper.
- **Version and date.** Tools change fast. A 2016 post about a stream processor may describe defaults that no longer apply. Check the date, then check the current docs.
- **Attribution.** Companies often open-sourced a tool that grew out of an internal need. Saying "X was built at Y to solve Z" is a useful fact, but verify the origin against the project's own history before repeating it.
- **Principles over products.** The durable lessons are about trade-offs: log versus table, batch versus stream, exact versus approximate, build versus buy. A good answer names the trade-off first and the product second.

### Candidate write-ups: what to take from each

The points below are widely known public facts. They are summaries of origins and themes, not details. For any number or design claim, read the original post.

- **LinkedIn and Kafka.** Kafka was built at LinkedIn for high-volume activity and event data and later open-sourced. Lesson: a durable, partitioned log can be the backbone that many systems read from. Read the original posts for throughput figures and their own trade-offs.
- **Uber and Hudi.** Hudi (Hadoop Upserts Deletes and Incrementals) came from Uber's need for record-level upserts and incremental pulls on data lake tables. Lesson: lake tables are not only append-only. Hudi discusses copy-on-write versus merge-on-read trade-offs. Verify the current options in the project docs.
- **Netflix and Iceberg.** Apache Iceberg was created at Netflix to address problems with directory-based Hive tables, such as slow listing and fragile schema and partition changes. Lesson: a table format is a metadata layer, and that layer solves problems the storage layer cannot. See [[iceberg]].
- **Airbnb and Minerva.** Minerva is described as a central layer for metric definitions, so that the same metric is computed the same way everywhere. Lesson: many analytics disagreements are definition disagreements. Read the post for its architecture and how adoption was managed.
- **Spotify and event delivery.** Spotify has written about event delivery at very large scale. I do not have reliable details to summarise here, so read the original posts and extract the constraints and decisions yourself.
- **Cloudflare and ClickHouse.** Cloudflare has written publicly about using ClickHouse for analytics over very large volumes of request and log data. Lesson: an [[olap]] engine can serve high-volume analytics, but the schema and ingestion design carry most of the outcome. Verify all figures in the original posts.
- **Discord and data storage.** Discord has described how its message store moved between database systems as its data grew. Lesson: storage choices have a lifetime, and migrations are planned around a growing data set. Read the posts for the reasons and the migration approach.
- **Iranian engineering blogs.** Blogs from Iranian companies, for example on Kafka, ClickHouse or Spark, are closer to the constraints you will meet locally (hardware, cloud access, mirrors). Read them with the same method. If a specific claim is central to your answer, confirm it with the author or the original post.

## 🧪 Try it

Choose one engineering write-up that you have not read before. It can be from the list above or from any company blog. Fill in the table from the Mid section, then answer:

1. Which constraint in the post would change your decision if it were different?
2. Which claim in the post could be wrong for your own company, and why?
3. Write a 3-sentence interview answer that uses one principle from the post, without quoting any number.

<details><summary>Show a worked example</summary>

**Worked example (Kafka origin story, from widely known public facts):**

- **Context:** a large social network with high-volume activity events and many consuming teams.
- **Problem:** point-to-point pipelines between systems were hard to build and operate.
- **Constraint:** many consumers needed the same stream, at different speeds.
- **Options:** point-to-point pipelines, a message queue per consumer, a shared log.
- **Decision:** a shared, partitioned, durable log that consumers read at their own offset.
- **Result:** check the original post for measured figures.
- **Lesson:** a log decouples producers from consumers and makes replay possible.

**Answers:**

1. If consumers needed no replay and only one consumer existed, a simple queue could replace the log. That constraint would change the decision.
2. A company with a few thousand events per day could be worse off with a cluster to operate. Scale and team skill matter.
3. "The principle I take from log-based designs is that a durable log lets many consumers read at their own pace and replay after a bug. Before I pick a broker, I ask who needs replay and for how long, because that decides whether the log's operating cost is worth it."

</details>

## 💬 Interview corner

**Question: "Tell me about a time you learned from an engineering blog post and applied it."**

- **Mid answer:** "I read about how a big company used a data lake table format and we adopted a similar format."
- **Senior answer:** "The post described a table format solving listing and schema-change problems at large scale. Our scale is smaller and our bottleneck was a different one, so I checked the constraints first. We adopted the principle, that table metadata should be separate from file layout, and we measured our own listing time before choosing."

**Question: "How would you justify a technology choice?"**

Use the same structure as the case study: context, constraint, options, decision, and the trade-off you accept. Interviewers look for the trade-off, not the brand.

> **🎤 Say this in the interview:** "A company that faced a similar problem chose a log-based design so that consumers could replay. Their scale and team were different from ours, so I'd take the principle, that a durable log decouples producers and consumers, and check our own replay requirements before committing."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Quoting a number from a blog post without its conditions. "Company X handles a million events per second" means nothing without hardware, message size and the year.

- Treating a blog post as a specification. It is an account of one team's choices.
- Copying the architecture without the constraints that produced it.
- Repeating an origin story without checking it against the project's history.
- Summarising only the success. Ask what failed and what the authors regret.
- Presenting a write-up you have only skimmed as if you know it well. Say what you read and when.
- Using a case study as an argument against a simpler tool. Many teams are better served by a database than by a distributed system.

## ✅ Check yourself (quiz)

1. Which section of a write-up tells you the most about whether its decision applies to you?

   - A) The title
   - B) The constraints and the scale, with their dates
   - C) The list of authors
   - D) The closing sentence

<details><summary>Answer</summary>

**B** — the decision follows from the constraints. If your constraints differ, the decision may not transfer, even when the problem sounds the same.

</details>

2. A post says "we reduced latency by 10×". What is the first thing to ask?

   - A) Which programming language they used
   - B) What the baseline was, measured how, on what workload and hardware
   - C) Whether the author is famous
   - D) Whether they open-sourced the tool

<details><summary>Answer</summary>

**B** — a 10× claim is meaningless without its baseline and workload. A change from a badly tuned setup to a well-tuned one can produce the same figure.

</details>

3. Which lesson from a case study is most likely to transfer to your own design?

   - A) "Use this exact product"
   - B) "Consumers need replay, so keep an append-only, replayable log"
   - C) "Our cluster has 400 nodes"
   - D) "Deploy on a Friday"

<details><summary>Answer</summary>

**B** — a principle tied to a constraint transfers. Product names and cluster sizes depend on context you may not share.

</details>

4. **(Senior)** A post describes a migration that succeeded after a dual-write period. Your team wants to copy the approach. What do you check first?

   - A) Whether the post used the newest version of the tool
   - B) Whether your team can run and reconcile both paths, and what your cutover criterion is
   - C) Whether the post has a diagram
   - D) Whether the authors work in your country

<details><summary>Answer</summary>

**B** — dual-write works only if you can keep both paths consistent and you know when to stop the old one. The operating cost and the cutover rule decide whether the approach fits.

</details>

5. **(Senior)** Why is "we chose X because a big company uses it" a weak interview answer?

   - A) Because interviewers never ask about companies
   - B) Because it names no constraint, no alternative and no trade-off, so it does not show your reasoning
   - C) Because big companies always pick the wrong tools
   - D) Because it is too short

<details><summary>Answer</summary>

**B** — the answer relies on reputation. A strong answer names the constraint, the rejected options and the trade-off you accept, which is what the interviewer is testing.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What to extract from a write-up | Context and scale, problem, constraints, options, decision, results, regrets |
| Why numbers need conditions | Hardware, data volume, workload and date decide whether a number means anything |
| Principle vs product | A principle ties a trade-off to a constraint; a product is one implementation of it |
| Survivorship bias in case studies | Posts describe successes; the failed attempts are rarely shown |
| Dual-write migration risk | Two paths must stay consistent, and you need a clear cutover criterion |
| Why Iceberg exists (summary) | To fix problems with directory-based Hive tables: listing, schema and partition changes |
| What a metrics layer solves (Minerva-style) | Disagreement caused by different definitions of the same metric |
| Strongest interview answer structure | Constraint, options, decision, and the trade-off accepted |

## 🔗 Go further

- **kafka-architecture** and **lakehouse-table-formats** give the mechanics behind the Kafka and Iceberg case studies.
- **realtime-olap** covers the analytics-engine trade-offs that the ClickHouse write-ups discuss.
- **pattern-cdc-analytics** and **design-cdc-lake** show how CDC pipelines are designed, a theme in many company posts.
- **incidents-postmortems** teaches the same reading method applied to failures, which are often the most useful posts.
- **how-to-prepare** and **interview-loop** explain how to turn these notes into stories for a real interview.
- For each write-up you read, keep the seven-line note from the Mid section. After ten notes, you will see the same trade-offs repeat.
