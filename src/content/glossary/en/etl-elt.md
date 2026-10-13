---
term: "ETL vs ELT"
aliases: ["etl", "elt", "extract transform load", "extract load transform"]
category: ingestion
short: "ETL transforms data before loading it into the target; ELT loads raw data first and transforms inside the warehouse or lakehouse."
related: ["data-warehouse", "lakehouse", "dag"]
---
## In plain words (Junior)
Cooking analogy: in ETL you chop and cook the ingredients in a separate prep kitchen and only deliver the finished dish to the restaurant. In ELT you deliver raw ingredients straight into the big restaurant kitchen and cook there. The second is possible when the main kitchen is powerful and cheap to scale.

## How it works (Mid)
ETL emerged when warehouses were expensive and compute scarce, so data was cleaned in an external engine (Informatica, SSIS, Spark) and only curated data was loaded. ELT became popular with scalable cloud warehouses and lakehouses: tools like Fivetran or Airbyte copy raw data, and SQL transformations (dbt) run inside the warehouse, close to the data.

```
ETL:  source ─► [transform on separate server/Spark] ─► warehouse
ELT:  source ─► raw tables in warehouse ─► [SQL transforms (dbt)] ─► models

-- ELT transform, run inside the warehouse
CREATE TABLE marts.daily_revenue AS
SELECT order_date, SUM(amount) AS revenue
FROM raw.orders WHERE status = 'PAID' GROUP BY 1;
```

ELT advantages: raw data is preserved (replay and re-derive), transformations are SQL that analysts can own, scale comes from the warehouse engine, and development is faster. ETL still fits: sensitive data that must be masked before landing, heavy non-SQL processing (parsing, ML, images), streaming enrichment, and strict schema validation up front.

## Senior angle & interview tips
In practice most modern stacks are "ELT with a bit of ETL": light cleaning or PII handling at ingestion, bulk modeling in-warehouse. Trade-offs: ELT can produce sprawling SQL and cost surprises from repeated full-table transforms (use incremental models), raw data landing may violate privacy rules, and raw layers need governance. Data contracts and quality tests belong between layers in both styles. Do not frame it as ETL being obsolete; choose per transformation.

Interview question: "ETL or ELT for a new platform?" Model answer: ELT by default: land raw data in object storage/warehouse, transform with SQL in layers with dbt, keep raw immutable for replay; use ETL-style processing before landing only for PII masking, heavy non-SQL transforms or streaming needs.
