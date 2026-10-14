---
term: "Reverse ETL"
aliases: ["reverse etl","operational analytics","data activation"]
category: ingestion
short: "Syncing modeled warehouse data back into operational tools like CRM and ad platforms."
related: ["etl-elt","data-warehouse","data-pipeline"]
---
## In plain words (Junior)
Instead of only looking at reports, you push the insights back into the tools people use daily, such as sending a "high-value customer" flag into the sales app.

## How it works (Mid)
A sync job reads tables or models from the warehouse and upserts them into SaaS and operational systems (Salesforce, HubSpot, ads, email) via their APIs, tracking diffs and handling rate limits. Tools: Census, Hightouch, or custom jobs.

## Senior angle & interview tips
Issues: API rate limits, idempotent upserts by external id, deletes, schema mapping, PII governance and who owns data quality at the destination. It makes the warehouse a source of truth for operations, so freshness SLAs matter.

Interview question: "How to sync a churn-risk score to the CRM?" Answer: compute in warehouse, diff against last synced state, upsert changed rows by account id with retries and rate limiting.
