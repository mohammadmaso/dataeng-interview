---
term: "Data catalog"
aliases: ["metadata catalog", "hive metastore", "glue catalog", "unity catalog", "iceberg catalog", "data discovery"]
category: quality
short: "A searchable inventory of datasets with schemas, owners, descriptions, lineage and access policies; table catalogs also map names to table metadata."
related: ["lineage", "data-contract", "data-mesh"]
---
## In plain words (Junior)
A library's catalog tells you which books exist, where they are, who wrote them and whether you may borrow them. A data catalog does this for datasets: it lets people find the right table, understand what it means and know who to ask, instead of guessing.

## How it works (Mid)
Technical catalogs are part of the runtime: they map table names to storage locations and schemas, and for table formats like Iceberg the catalog holds the pointer to the current metadata file, so commits are atomic via compare-and-swap. Using one shared catalog lets Spark, Trino and Flink see the same tables.

```
Two meanings, often mixed up:

1. Technical catalog / metastore (runtime):  db.table -> location + schema + table metadata pointer
   Hive Metastore, AWS Glue, Unity Catalog, Iceberg REST/Nessie catalog
   Engines (Spark, Trino, Flink) call it to resolve and atomically commit tables.

2. Business / discovery catalog (humans):  search, glossary, owners, tags, PII classification, lineage
   DataHub, Amundsen, OpenMetadata, Collibra, Atlan
```

Discovery catalogs ingest metadata automatically (schemas, query logs, lineage, usage popularity) and let humans add descriptions, tags, ownership, glossary terms and classifications such as PII. They underpin governance (access policies, retention), impact analysis and self-service in a data mesh.

## Senior angle & interview tips
Catalog quality = metadata freshness and ownership. Stale descriptions and unowned tables make a catalog useless, so automate ingestion, tie ownership to teams, surface usage, and make documentation part of the contract or CI (dbt docs). Multi-engine environments need an engine-neutral catalog (REST catalog) to avoid lock-in and inconsistent views. Security: column-level policies, tag-based access and audit logs should be enforced by the platform, not just documented. Without a catalog, a lake becomes a swamp.

Interview question: "What does the catalog do in an Iceberg setup?" Model answer: it maps table identifiers to the current metadata file and performs the atomic pointer swap on commit, so all engines agree on table state; a separate discovery catalog adds search, ownership and lineage for humans.
