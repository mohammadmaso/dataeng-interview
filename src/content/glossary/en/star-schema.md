---
term: "Star schema"
aliases: ["star schemas", "dimensional model", "dimensional modeling", "snowflake schema"]
category: modeling
short: "A dimensional model with a central fact table joined directly to denormalized dimension tables, optimized for analytic queries."
related: ["fact-table", "dimension-table", "grain", "surrogate-key"]
---
## In plain words (Junior)
Imagine a ledger of sales in the center of a table, surrounded by small reference cards: one for products, one for customers, one for dates, one for stores. Each sale line points to its cards. Drawn on paper it looks like a star. This layout makes it easy to answer questions like "sales by month and product category".

## How it works (Mid)
The center is a fact table (measurements at a declared grain: quantities, amounts) with foreign keys (usually surrogate keys) to dimension tables (descriptive context: who, what, where, when). Dimensions are denormalized (a product row carries its category and brand), so queries need one join per dimension and no long join chains. A snowflake schema normalizes dimensions into sub-tables, saving some space at the cost of more joins.

```
                dim_date
                   │
dim_customer ── fct_sales ── dim_product
                   │
                dim_store

SELECT d.month, p.category, SUM(f.net_amount)
FROM fct_sales f
JOIN dim_date d    ON f.date_sk = d.date_sk
JOIN dim_product p ON f.product_sk = p.product_sk
GROUP BY d.month, p.category;
```

Design process (Kimball): pick the business process, declare the grain, identify dimensions, identify facts. Conformed dimensions (the same `dim_customer` across several fact tables) enable drill-across. Degenerate dimensions (order number in the fact), junk dimensions and role-playing dimensions (date as order date and ship date) are common patterns.

## Senior angle & interview tips
Stars shine for BI: simple SQL, predictable joins, great fit for columnar engines and broadcast joins of small dimensions. Trade-offs: ETL effort to build and maintain, handling history with SCDs, rigid compared with schema-on-read, and denormalization duplicates attributes. In modern columnar systems, some teams use wide denormalized "one big table" for speed and simplicity; still, a star model gives governed metrics and reuse. Be ready to compare Kimball (bottom-up marts), Inmon (normalized enterprise warehouse) and Data Vault.

Interview question: "Design a model for e-commerce analytics." Model answer: a sales fact at order-line grain with surrogate keys to date, product, customer and store dimensions, measures such as quantity and net amount, SCD2 on customer and product where history matters, and a conformed date dimension.
