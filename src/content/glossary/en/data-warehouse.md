---
term: "Data warehouse"
aliases: ["data warehouses", "dwh", "edw", "enterprise data warehouse"]
category: modeling
short: "A central, modeled, governed store of integrated historical data optimized for analytics and reporting (schema-on-write)."
related: ["olap", "etl-elt", "star-schema", "data-lake"]
---
## In plain words (Junior)
A data warehouse is the company's well-organized library of facts: data from many systems is cleaned, matched up and stored in a consistent way, so that anyone asking "what were our sales last quarter?" gets the same trustworthy answer regardless of which system the data came from.

## How it works (Mid)
Defined by Inmon as subject-oriented, integrated, time-variant and non-volatile. Data is extracted from operational systems, transformed to a common model (dimensional/star or normalized or Data Vault) and loaded with schema-on-write, so quality and structure are enforced at load time. It's served through SQL on columnar MPP engines (Snowflake, BigQuery, Redshift, Synapse) that separate or couple storage and compute.

```
OLTP DBs, SaaS, logs
      │  ETL / ELT / CDC
      ▼
 staging ─► core (integrated, history) ─► marts (star schemas) ─► BI tools
                  ▲
          conformed dimensions, business definitions, access control
```

Layers: staging (raw copy), integration/core (cleaned, historized), marts (subject-specific stars). Conformed dimensions and a semantic layer give one definition for each metric. Design approaches: Kimball (dimensional marts, bottom-up), Inmon (normalized EDW first, top-down), Data Vault (hubs, links, satellites for auditable integration).

## Senior angle & interview tips
Warehouses excel at governed SQL analytics with strong performance and concurrency, but classical ones are costly for raw/unstructured data, tightly coupled to a vendor format, and slower to change because modeling is up-front. Modern cloud warehouses decouple storage and compute and support semi-structured data and ELT, narrowing the gap with lakehouses. Key concerns: cost control (query pruning, clustering, workload isolation), slowly changing history, metric consistency, and PII access policies. Reverse ETL pushes warehouse data back into operational tools.

Interview question: "When do you still choose a warehouse over a lake?" Model answer: when the main workload is governed SQL/BI with many concurrent users and strict consistency of metrics, and a managed engine's performance and simplicity outweigh format openness; otherwise a lakehouse for multi-engine and ML.
