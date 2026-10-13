---
order: 63
tid: "modeling-methodologies"
track: "6"
section: null
num: "6.8"
title: "Inmon, Kimball & Data Vault 2.0"
type: "concept"
level: "advanced"
priority: "P1"
est_hours: 4
summary: "Comparing methodologies: Inmon (normalized enterprise warehouse), Kimball (dimensional marts), Data Vault 2.0 (hubs, links, satellites), popular in banks for auditability and history."
covers: []
interview_qs: []
extras: []
prerequisites: ["modeling-levels", "dimensional-modeling"]
related: ["normalization", "scd", "medallion-architecture", "data-lineage", "star-snowflake-obt"]
---
## 🎯 TL;DR

> Three well-known methods answer one question: **how do you structure a whole data warehouse**, not one table. **Inmon** builds a normalized enterprise warehouse first and derives marts from it. **Kimball** builds dimensional marts (star schemas) and connects them with shared, conformed dimensions. **Data Vault 2.0** stores history in insert-only hubs, links and satellites, and builds star-shaped marts on top.

- **Inmon (top-down):** one integrated, normalized (3NF) warehouse; marts are views or copies of it.
- **Kimball (bottom-up):** dimensional marts that are easy to query; conformed dimensions keep them consistent.
- **Data Vault 2.0 (hybrid, auditable):** business keys, relationships and descriptive history are separated; loads are parallel and insert-only.
- None is "best". They differ in who they serve, how fast you can deliver, and how well they handle audit and change.

## 🌱 Start simple (Junior)

> **💡 Analogy:** Three ways to organize a big city's records. **Inmon** keeps one central archive, perfectly filed, and makes a small reading room for each department from it. **Kimball** opens a reading room for each department first, but makes sure all the rooms use the same index cards for "customer" and "date". **Data Vault** files every fact in three kinds of folders: one for *who* or *what* (the business key), one for *how things are connected*, and one for *what we knew, and when* (the history). New facts are only ever added, never overwritten.

For a junior, the practical question is: "Where does my data go, and what does a dashboard read?" Take a bank with customers and accounts:

- **Inmon:** the customer and account data go into normalized tables in the central warehouse. A "customer balance" report reads a mart built from those tables.
- **Kimball:** you build `fact_account_balance` with `dim_customer` and `dim_date` right away. The report reads that star directly.
- **Data Vault:** the customer key goes into a *hub*, the customer-account relationship goes into a *link*, and the changing name and address go into a *satellite* with a timestamp. The report reads a star built on top of the vault.

> **⚠️ Watch out:** These are not three different *schemas* for the same table. They are three different *architectures* for the whole warehouse. A Kimball star can sit on top of an Inmon warehouse, and a Data Vault is usually paired with stars for reporting.

## 🔧 How it really works (Mid)

### Inmon: the corporate information factory

Inmon, the "father of data warehousing", defines a **subject-oriented, integrated, time-variant, non-volatile** warehouse. In practice:

1. Source systems are integrated into one **normalized (3NF)** enterprise model, with a single definition of "customer" and "account".
2. **Dependent data marts** are derived from the enterprise warehouse for each department. A mart is often a denormalized copy or a view.
3. History is kept by design, because the warehouse is time-variant.

The strength is a single source of truth. The cost is time: designing a full enterprise model before the first dashboard can take months.

### Kimball: dimensional bus architecture

Kimball starts with a business process: "orders", "payments", "shipments". For each process you build a **dimensional model** (fact table at a declared grain, dimensions around it). The key idea is the **bus matrix**: a table of processes against dimensions. Dimensions that are used by several processes are **conformed**: the same `dim_customer` with the same keys and attributes.

With conformed dimensions, marts built at different times still agree. The cost is discipline: someone must own the conformed dimensions, or each mart drifts.

### Data Vault 2.0: hubs, links and satellites

Data Vault (Dan Linstedt) splits every source table into three concept types:

| Structure | Holds | Example | Key property |
|---|---|---|---|
| **Hub** | A business key and load metadata | `hub_customer` (customer number) | One row per business key, ever |
| **Link** | A relationship between hubs | `link_customer_order` | Many-to-many by design; one row per combination |
| **Satellite** | Descriptive attributes and history | `sat_customer_details` (name, email, country) | A new row when attributes change (`load_dts`) |

Version 2.0 adds **hash keys**. Each hub, link and satellite key is a deterministic hash (for example MD5 or SHA-256) of the normalized business key. Hash keys let different loaders run in parallel without looking up sequences, and they make the vault easier to move between systems.

```sql
-- Data Vault 2.0 sketch (Postgres syntax; hash keys as hex text)
CREATE TABLE hub_customer (
  customer_hk   char(32) PRIMARY KEY,   -- hash of the normalized business key
  customer_bk   text NOT NULL UNIQUE,   -- for example the CRM customer number
  load_dts      timestamptz NOT NULL,
  record_source text NOT NULL
);

CREATE TABLE hub_order (
  order_hk      char(32) PRIMARY KEY,
  order_bk      text NOT NULL UNIQUE,
  load_dts      timestamptz NOT NULL,
  record_source text NOT NULL
);

CREATE TABLE link_customer_order (
  link_hk       char(32) PRIMARY KEY,   -- hash of the combined business keys
  customer_hk   char(32) NOT NULL REFERENCES hub_customer (customer_hk),
  order_hk      char(32) NOT NULL REFERENCES hub_order (order_hk),
  load_dts      timestamptz NOT NULL,
  record_source text NOT NULL,
  UNIQUE (customer_hk, order_hk)
);

CREATE TABLE sat_customer_details (
  customer_hk   char(32) NOT NULL REFERENCES hub_customer (customer_hk),
  load_dts      timestamptz NOT NULL,
  hash_diff     char(32) NOT NULL,      -- hash of the descriptive columns
  name          text,
  email         text,
  country       char(2),
  record_source text NOT NULL,
  PRIMARY KEY (customer_hk, load_dts)
);
```

Notice what is missing: no updates, no deletes. A change in the customer's email creates a new satellite row whose `hash_diff` differs from the previous one. The history is the table.

### Where Data Vault stops and marts begin

Raw Vault stores data as it arrived, with no business rules. A **business vault** adds derived rules. **Information marts** (usually stars) are built for reporting, often with **PIT** (point-in-time) and **bridge** tables that make joins across satellites efficient.

```mermaid
flowchart LR
    SRC[Source systems] --> STG[Staging]
    STG --> RV[Raw Vault: hubs, links, satellites]
    RV --> BV[Business Vault: derived rules]
    BV --> MART[Information marts: star schemas]
    MART --> BI[BI tools]
```

### Side by side

| Question | Inmon | Kimball | Data Vault 2.0 |
|---|---|---|---|
| Direction | Top-down (enterprise first) | Bottom-up (process by process) | Hybrid: integrated raw store, then marts |
| Core model | Normalized 3NF | Star schemas | Hubs, links, satellites |
| Time to first mart | Slow | Fast | Medium (vault plus marts) |
| History and audit | Good | Depends on SCD choices | Strongest (insert-only, load timestamps, record source) |
| Query speed for BI | Via marts | Best (designed for it) | Via marts; joins are heavier in the vault |
| Parallel loading | Harder | Moderate | Easy (hash keys, no sequences) |
| Change to a source | Impact on the enterprise model | Impact on affected marts | Add satellites; existing structures stay |
| Typical fit | Large organization with a strong central team | Analytics teams that need dashboards quickly | Regulated industries that need audit trails |

## 🧠 Senior depth & trade-offs

- **Auditability is the main reason banks choose Data Vault.** Every row carries `load_dts` and `record_source`, nothing is overwritten, and you can rebuild any past state. Regulators ask "what did you know on that day?", and a vault answers it directly.
- **The cost is joins and layers.** A question about a customer's current address may join a hub, a satellite and a PIT table, then a mart. Automate the layers (dbt, code generators) and keep the business vault small. Many teams treat the vault as storage and serve analysts from stars.
- **Kimball is not "no history".** Kimball handles history through SCD types on dimensions. The difference is where the full history lives: in the dimension (Kimball) or in satellites (Data Vault).
- **Inmon's enterprise model is the hardest to get right.** It requires agreement across departments before delivery. In a large organization with a strong data governance function it works; without one, it stalls.
- **Hash keys:** collisions are rare with a good hash, but you must normalize the input (trim, case, null handling, separators) or the same business key gets different hashes. Also decide the hash function once and never change it, because the keys are stored everywhere.
- **Hybrid reality:** most real warehouses mix methods. A Kimball star on top of a lakehouse medallion layout, or a Data Vault raw layer with Kimball marts, is common. Name the layers and the rule for each one, instead of arguing for a pure method.
- **When NOT to use Data Vault:** for a small team with a few sources and simple reporting, a Kimball star with SCD2 is faster to deliver and easier to hire for.

## 🧪 Try it

A retail company has sources for `customers` (CRM), `orders` (e-commerce) and `payments` (bank). Management wants a monthly revenue dashboard and an audit trail of every change to a customer's address.

1. Which structure would hold the customer's address history in Data Vault, and what columns does it need?
2. Which dimension would a Kimball star use for the same address history, and which SCD type gives you the full history?
3. Name one reason to choose Data Vault here and one reason to choose Kimball.
4. Why must the loader normalize a business key before hashing it?

<details><summary>Your guess?</summary>

1. A satellite on `hub_customer`, for example `sat_customer_address`, with `customer_hk`, `load_dts`, `hash_diff`, the address columns and `record_source`. Each change adds a row.
2. A `dim_customer` with address attributes tracked as SCD Type 2 (valid_from, valid_to, is_current). That keeps the full history in the dimension.
3. Data Vault is strong for audit and parallel loading of several sources. Kimball is faster to deliver the revenue dashboard and simpler for analysts.
4. The same business key written as `" C-001"`, `"c-001"` or `"C-001 "` produces different hashes, so the same customer would become several hubs. Normalizing (trim, upper or lower case, consistent null handling, fixed separators) makes the hash deterministic.

</details>

## 💬 Interview corner

**Question: "Inmon or Kimball, which would you use?"** Mid answer: "Kimball for fast dashboards, Inmon for a single source of truth." Senior answer: "It depends on the organization. I'd ask who consumes the warehouse, how fast the first business value must arrive, and how strict the audit requirements are. In practice I'd build conformed dimensions Kimball-style, and if the sources are many and volatile, I'd put a historized layer underneath, which is where Inmon or Data Vault ideas help."

**Question: "What is Data Vault and why do banks like it?"** Mid: "It splits data into hubs for business keys, links for relationships and satellites for history. Banks like it for audit." Senior: "Every row is insert-only and timestamped with its source, so I can reproduce any past state and prove what was loaded when. Loads run in parallel because the hash keys do not need sequence lookups. The trade-off is more joins, so I'd put star marts on top for reporting."

**Question: "What is a conformed dimension?"** It is a dimension shared across fact tables with the same keys and meaning, which lets marts combine consistently. Kimball's bus matrix is the planning tool for finding them.

> **🎤 Say this in the interview:** "I pick the method by the constraints. Kimball gives fast, query-friendly marts with conformed dimensions. Inmon gives one integrated enterprise model. Data Vault gives auditable, insert-only history and parallel loads, with star marts on top. Most real warehouses combine them, and I'd name the layer and rule for each."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Calling Data Vault "a star schema with hubs". It is a different model for storage and history. Stars are for queries; the vault is for integration and audit.

> **⚠️ Watch out:** Querying the raw vault directly from BI. The joins are many and the result is hard to read. Build marts for consumers.

- Treating the three methods as mutually exclusive. Hybrid architectures are the norm.
- Building a full Inmon enterprise model before delivering any business value.
- Making a hub from a business key that is not stable (for example, a status code) instead of an identifier.
- Storing descriptive attributes in the hub or in the link. They belong in satellites.
- Hashing business keys without normalization, then wondering why duplicates appear.
- Overwriting satellite rows, which removes the audit trail that the method exists to provide.

## ✅ Check yourself (quiz)

1. Which method builds a normalized enterprise warehouse first and derives marts from it?
   - A) Kimball
   - B) Inmon
   - C) Data Vault 2.0
   - D) Medallion

<details><summary>Answer</summary>

**B** — Inmon's top-down approach creates an integrated 3NF enterprise warehouse. Dependent marts are derived from it.

</details>

2. In Data Vault, where do descriptive attributes such as an email address and their history go?
   - A) Hub
   - B) Link
   - C) Satellite
   - D) Bridge

<details><summary>Answer</summary>

**C** — satellites hold descriptive attributes and their history, with a load timestamp. Hubs store only the business key and metadata; links store relationships.

</details>

3. Which Kimball concept lets several marts agree on the meaning of "customer"?

   - A) A junk dimension
   - B) A conformed dimension
   - C) A degenerate dimension
   - D) A mini-dimension

<details><summary>Answer</summary>

**B** — a conformed dimension has the same keys and definitions across fact tables, so marts built at different times combine consistently.

</details>

4. **(Senior)** An auditor asks what the customer's address was on a date two years ago, and the data has been loaded with Data Vault for a year. What makes this answerable, and what is the cost of answering it?

   - A) Nothing; Data Vault does not keep history
   - B) Satellite rows are insert-only and timestamped with `load_dts`, so you select the row valid at that time; the cost is extra joins, which a PIT table or mart can hide
   - C) You must restore a database backup
   - D) Hubs store the address by year

<details><summary>Answer</summary>

**B** — insert-only satellites with load timestamps let you reconstruct any past state. The trade-off is more joins; PIT tables and marts reduce the cost for common queries.

</details>

5. **(Senior)** A team hashes business keys to build hub keys but gets duplicate customers after a source starts sending keys in lowercase with trailing spaces. What is the root cause and the fix?

   - A) The hash function is broken; switch to a sequence
   - B) The business key is not normalized before hashing, so equal keys produce different hashes; normalize (trim, case, null handling, separators) consistently and never change the function
   - C) Hubs cannot hold text keys
   - D) Satellites are causing duplicates in the hub

<details><summary>Answer</summary>

**B** — the same logical key produces two hashes. Normalize before hashing, and keep the hash definition fixed, because the keys are stored across the vault.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Inmon | Top-down: integrated 3NF enterprise warehouse, marts derived from it |
| Kimball | Bottom-up: dimensional marts with conformed dimensions, planned with a bus matrix |
| Data Vault hub | One row per business key plus load metadata |
| Data Vault link | Relationship between hubs; one row per combination |
| Data Vault satellite | Descriptive attributes and history, insert-only, with load timestamp |
| Hash key (DV 2.0) | Deterministic hash of the normalized business key; enables parallel loads |
| Raw vault vs business vault | Raw stores data as received; business vault adds derived rules |
| Strongest auditability | Data Vault: insert-only, `load_dts`, `record_source` on every row |

## 🔗 Go further

- **dimensional-modeling** covers the star schema and conformed dimensions that Kimball and most marts rely on.
- **scd** explains how Kimball keeps history inside dimensions, to compare with satellites.
- **medallion-architecture** shows the bronze, silver and gold layers, which often sit next to a vault in lakehouse designs.
- **data-lineage** is what makes a vault's `record_source` useful in practice.
- **star-snowflake-obt** compares the shapes of the marts that you build on top of any of these methods.
