---
order: 134
tid: "data-security"
track: "12"
section: null
num: "12.7"
title: "Security, Privacy & Access Control"
type: "concept"
level: "advanced"
priority: "P1"
est_hours: 3
summary: "RBAC and ABAC, Apache Ranger, row- and column-level security, masking and tokenizing sensitive data (national ID, card numbers, phone numbers), encryption at rest and in transit, and auditing. Critical for banks and telcos."
covers: ["Kerberos in Hadoop","Keycloak/OIDC integration","PII discovery","right-to-delete in immutable lakes"]
interview_qs: []
extras: []
prerequisites: ["dwh-fundamentals"]
related: ["data-retention", "data-catalogs", "data-lineage", "governance-contracts-mesh", "hadoop-ecosystem", "networking-essentials"]
---
## 🎯 TL;DR

> **Data security** answers three questions for every dataset: **who** may touch it, **which parts** of it they may see, and **how** it is protected while stored, moving and being audited.

- **Authentication** proves who you are. **Authorization** decides what you may do. Keep both separate.
- **RBAC** (role-based) grants permissions to roles. **ABAC** (attribute-based) decides from attributes such as department, purpose or data classification.
- **Row-level** and **column-level** security hide parts of a table. **Masking** and **tokenization** replace sensitive values with safe stand-ins.
- Encrypt **at rest** and **in transit**, and keep an **audit** trail of who read what.
- In a lake, the hardest requirement is **deleting one person's data** from immutable files. Plan for it from day one.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A bank branch. The teller can see your account balance but not the vault. The manager can open the vault, but every opening is written in a logbook. The customer's national ID on the form is covered by a sticker, so the teller can confirm the record without reading the number.

Every data platform has the same four controls:

| Control | Question it answers | Example |
|---|---|---|
| Authentication | Is this really Ali? | Password, SSO login, Kerberos ticket |
| Authorization | May Ali read `customers`? | `GRANT SELECT ON customers TO analyst` |
| Protection | If someone copies the file, can they read it? | Encrypted disk, encrypted bucket |
| Audit | Who read the data yesterday, and when? | Access log, query history |

A junior-level mistake is to give every analyst the `admin` role because it is faster. A junior-level good habit is to create a role per job function and grant the minimum.

Here is a minimal, portable example of a role and a grant:

```sql
CREATE ROLE analyst;
GRANT SELECT ON sales.orders TO analyst;

-- Analyst can read orders, but not the customers table with personal data.
REVOKE ALL ON sales.customers FROM analyst;
```

Principle of least privilege: give a user **only** what the job needs, for **only** as long as the job needs it.

## 🔧 How it really works (Mid)

### RBAC, ABAC and where the rules live

- **RBAC** maps users to roles and roles to permissions. It is simple and fits a stable org chart: `analyst`, `engineer`, `finance_reader`.
- **ABAC** evaluates a policy with attributes: `user.department = 'fraud' AND table.classification <> 'secret' AND request.purpose = 'investigation'`. It scales better when rules depend on data, but it is harder to explain and to test.

In practice, a data platform uses a central policy engine. In the Hadoop world that is **Apache Ranger**: one place defines policies for HDFS, Hive, Spark SQL, Kafka and others, and each engine asks Ranger (through a plugin) whether an action is allowed. Authentication in Hadoop often uses **Kerberos**: the user gets a ticket from a key distribution center and presents it to services. In web-facing tools, **OIDC** (OpenID Connect, on top of OAuth 2.0) with a broker such as **Keycloak** handles login and group claims that feed RBAC.

### Row-level and column-level security

```sql
-- Column masking with a view: only members of 'pii_readers' see full national IDs.
CREATE VIEW customers_safe AS
SELECT
  customer_id,
  city,
  CASE
    WHEN EXISTS (
      SELECT 1 FROM pii_grants g
      WHERE g.user_name = current_user AND g.purpose = 'fraud'
    )
    THEN national_id
    ELSE CONCAT(REPEAT('*', 6), RIGHT(national_id, 4))
  END AS national_id
FROM customers;
```

Row-level security works the same way with a filter instead of a `CASE`: a policy adds `WHERE branch_id IN (the user's branches)` to every query. The engine adds the filter, so users cannot forget it.

### Masking vs tokenization vs encryption

| Technique | What it does | Reversible? | Typical use |
|---|---|---|---|
| Masking | Shows a partial or fake value (`******1234`) | No | Support screens, analyst dashboards |
| Tokenization | Replaces the value with a random or keyed token, keeps the mapping in a vault | Yes, only through the vault | Joins across systems without raw IDs |
| Keyed hash (pseudonym) | Same input and key give the same token | No (without the key, it is one-way) | Joining pseudonymous users |
| Encryption | Scrambles data with a key | Yes, with the key | Storage and transport |

A keyed hash keeps joins working without exposing the value:

```python
import hashlib
import hmac
import os

# The key must come from a secret store (Vault, a cloud KMS, or an HSM), never from the code.
KEY = os.environ["PSEUDONYM_KEY"].encode()

def pseudonymize(national_id: str) -> str:
    """Same ID + same key -> same token, so joins still work."""
    return hmac.new(KEY, national_id.encode(), hashlib.sha256).hexdigest()

print(pseudonymize("0012345678"))
```

If the key leaks, an attacker can test candidate IDs. For low-entropy values such as phone numbers or national IDs, a keyed hash is still **pseudonymization**, not anonymization: keep the key separate from the data and rotate it under a documented process.

### Encryption in transit and at rest

- **In transit:** TLS between clients, Kafka brokers, Spark executors (where enabled), JDBC/ODBC drivers and object storage.
- **At rest:** disk or volume encryption, and server-side or client-side encryption on object storage. Keys live in a KMS or HSM with access logs, and key rotation is an operational task, not a one-time setup.
- Encryption does **not** stop a user with valid credentials from reading data. It protects against stolen disks, misconfigured buckets and backups.

### Audit

Log at least: who authenticated, which query or job ran, which tables and columns were read or written, and which policies allowed it. Audit logs should go to a store that the data engineers cannot edit. Without this, a privacy incident cannot be investigated.

## 🧠 Senior depth & trade-offs

- **Discovery comes first.** You cannot protect PII you do not know about. Run PII classification (pattern checks such as Iranian national ID format plus column-name heuristics, and sampling) and store the tag in the [[data-catalog]], so policies can reference tags instead of hand-written column lists.
- **Tags beat per-table rules.** Attach `classification=pii` to a column and let ABAC policies apply masking everywhere. Hand-written grants drift; tags propagate.
- **Right to delete in immutable storage.** Parquet files in a lake are immutable, and time travel or snapshots keep old versions. Options:
  - **Rewrite affected files** (delete rows, then expire old snapshots and vacuum). Expensive at scale; schedule it and measure it.
  - **Crypto-shredding:** store each data subject's PII encrypted with a per-subject key. "Delete" means destroying the key. The bytes stay, but they are unreadable. This only works if the non-sensitive columns carry no identity.
  - **Keep PII out of the lake** in the first place: store it in a small OLTP table and keep only tokens in the analytical tables.
- **Performance cost.** Row filters and masking run inside the engine. Complex predicates can block pushdown, and a policy on a wide table can add a scan. Measure with `EXPLAIN` before shipping a policy.
- **Trust boundaries.** The policy engine is a single point of failure. Define what happens when it is down (fail closed for PII, with a break-glass path that is audited).
- **Least privilege for pipelines.** Service accounts for Airflow or Spark jobs should have their own roles. A shared "etl" user with admin rights is the most common finding in a security review.
- **Regulation.** Banks and telcos face strict rules on who may see customer data and where it is stored. Work with legal and compliance: the engineering controls above are necessary, not a legal opinion.

> **⚠️ Watch out:** A masked view is not a security boundary if the analyst can still query the base table. Remove direct grants on the base table, or the view protects nothing.

## 🧪 Try it

Use any local PostgreSQL 14+ instance.

1. Create a table `customers(id int, national_id text, city text)` with two rows. Create a role `analyst` and a role `pii_reader`. Grant `SELECT` on the table to `analyst` only through a view that masks `national_id` with `CONCAT(REPEAT('*', 6), RIGHT(national_id, 4))`. Explain why `analyst` must not have a direct grant on the base table.
2. Run the Python pseudonymization function on the same ID twice, then with a different key. Explain which property makes joins possible and which one breaks when the key is lost.
3. Write down the steps to honour one "delete my data" request for a customer whose rows are spread across 200 Parquet files in an Iceberg table. Name the parts that take time.

<details><summary>Show solutions</summary>

**1.** Create the view with `SELECT id, CASE WHEN ... END` as above and grant `SELECT` on the view. If `analyst` also has `SELECT` on the base table, the query `SELECT national_id FROM customers` bypasses the view entirely. The view is only a boundary when the base table is not reachable.

**2.** The same ID and key give the same token, so joins on the token work. A different key gives a different token, so the old tokens no longer match anything. If the key is lost, you cannot re-create the mapping, and existing tokens remain as opaque values.

**3.** Find all rows for the subject (through the catalog, the tag and the identity mapping). Rewrite or delete the affected data files, which is the slow step on 200 files and grows with table size. Then expire old snapshots and delete the old files, because time travel would otherwise keep the data. Record the request and the completion date for the audit trail.

</details>

## 💬 Interview corner

**Question: "How would you give analysts access to customer data without exposing personal information?"**

- **Mid answer:** "I'd create a masked view for analysts and remove their direct access to the base table. Engineers and fraud analysts get a role with full access, and access is logged."
- **Senior answer:** "I'd start with classification: tag PII columns in the catalog, then apply ABAC policies on tags. Analysts get masked or tokenized columns, joins run on keyed pseudonyms, and the key sits in a KMS. Break-glass access is time-limited and audited. I'd also check the right-to-delete path, because the masking design decides how hard deletions are later."

> **🎤 Say this in the interview:** "I separate authentication, authorization and audit, and I prefer policies on tags over hand-written grants. For PII in an immutable lake, I'd keep identity out of the analytical tables, so deletion means touching a few rows or destroying a key."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Hard-coding passwords or keys in Airflow DAGs, notebooks or Git history. Use a secret backend and rotate anything that has been committed.

- Treating encryption at rest as access control. It does not stop authorized-but-curious users.
- Shipping a masking view while the base table is still readable.
- Using an unsalted hash of a national ID as a "anonymous" key. It is reversible by enumeration.
- Sharing one service account across all pipelines, which makes audit useless.
- Logging raw PII in application logs or in error messages sent to monitoring.
- Forgetting backups and snapshots when deleting data.

## ✅ Check yourself (quiz)

1. Which control answers the question "what did this user read last week?"
   - A) Encryption at rest
   - B) Authorization
   - C) Audit logging
   - D) Tokenization

<details><summary>Answer</summary>

**C** — audit logging records access events. Encryption and tokenization protect data but do not record who read it. Authorization decides who may read; it does not keep a history.

</details>

2. A column holds national IDs. Analysts need to count distinct customers per city. Which technique keeps that working?
   - A) Delete the column before the analysts get access
   - B) Replace the value with a keyed pseudonym; distinct counts still work
   - C) Mask the value with `******` for all rows
   - D) Encrypt the column with a key every analyst has

<details><summary>Answer</summary>

**B** — a deterministic keyed pseudonym keeps equal values equal, so distinct counts are correct, and the raw ID is not exposed. Full masking (C) destroys the distinct count. Option D gives analysts the key, so the protection is gone.

</details>

3. Why is a view with `CASE` masking not enough if analysts can still `SELECT` from the base table?
   - A) Views cannot contain `CASE`
   - B) The base table is a separate path that bypasses the policy
   - C) Views are always slower than tables
   - D) Masking requires a Kerberos ticket

<details><summary>Answer</summary>

**B** — the policy only applies when the query goes through the view. Remove the base-table grant, or the masking is decoration.

</details>

4. **(Senior)** A customer requests deletion. Their data is in an Iceberg table with years of snapshots. What is the trade-off between rewriting files and crypto-shredding?

   - A) There is no trade-off; deletion always means dropping the table
   - B) Rewriting removes the bytes but is expensive at scale and must also expire snapshots; crypto-shredding is cheap but only works if the PII was encrypted per subject and the remaining columns cannot identify the person
   - C) Crypto-shredding is cheaper because it deletes the whole table
   - D) Rewriting is illegal in every jurisdiction

<details><summary>Answer</summary>

**B** — rewriting physically removes data but costs compute and touches many files, and old snapshots keep the data until expired. Crypto-shredding is fast, but only sound when the stored data is unreadable without the key and no other column re-identifies the subject.

</details>

5. **(Senior)** A pipeline service account has admin rights on the warehouse "because it was easier". What should you propose?

   - A) Nothing; service accounts are exempt from least privilege
   - B) Create a dedicated role per pipeline with only the schemas and operations it needs, store credentials in a secret backend, and log its actions separately
   - C) Share the admin password with the analytics team so they can debug
   - D) Disable audit logs for service accounts to reduce noise

<details><summary>Answer</summary>

**B** — least privilege applies to machines as much as people. Dedicated roles limit blast radius, and separate logs make a compromised job visible.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Authentication vs authorization | Authentication proves identity; authorization decides what that identity may do |
| RBAC vs ABAC | RBAC grants by role; ABAC decides from attributes (department, purpose, classification) |
| Masking vs tokenization | Masking hides parts of a value, usually one-way; tokenization replaces it with a token that can be mapped back only through a vault |
| Keyed hash (pseudonym) | Same input and key give the same output; keeps joins working; the key must be protected |
| Why keep the key out of the data | If the key leaks with the data, the pseudonyms can be reversed by enumeration |
| Encryption at rest vs in transit | Protects stored bytes (disks, buckets) vs data moving across the network (TLS) |
| Crypto-shredding | Delete a subject's key so their encrypted data becomes unreadable |
| Why a masked view alone is not enough | Users with direct grants on the base table bypass it |

## 🔗 Go further

- **[[data-retention]]** covers how long data may be kept, and how deletion and archive policies fit with security.
- **[[data-catalogs]]** and **[[data-lineage]]** show how tags and lineage make policies and deletion requests manageable.
- **[[governance-contracts-mesh]]** places access control inside ownership and data contracts.
- **[[design-user-deletion]]** works through a full "delete a user" design with the constraints in this lesson.
- Read the security documentation of your engine (Spark, Trino, ClickHouse, your warehouse) for the exact syntax of row policies and masking functions; the concepts transfer, the syntax does not.

