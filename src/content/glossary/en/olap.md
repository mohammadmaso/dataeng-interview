---
term: "OLAP (online analytical processing)"
aliases: ["olap", "analytical database", "analytical workload", "olap cube"]
category: storage
short: "Workload of complex aggregations and scans over large volumes of historical data, optimized for throughput and read speed."
related: ["oltp", "columnar-storage", "star-schema"]
---
## In plain words (Junior)
If OLTP is the cash register, OLAP is the accountant analyzing the whole year of receipts: which products sold best, how sales changed by region and month. Fewer people ask, but each question looks at huge amounts of data and may take seconds instead of milliseconds.

## How it works (Mid)
Characteristics: columnar storage, denormalized dimensional models (star schema), large scans with aggregations, batch or micro-batch ingestion, historical data, and relatively few concurrent but heavy queries. Systems: Snowflake, BigQuery, Redshift, ClickHouse, StarRocks, Druid, Pinot, DuckDB and lakehouse engines over Parquet.

```
SELECT d.month, c.region, p.category,
       SUM(f.net_amount) AS revenue, COUNT(DISTINCT f.customer_sk) AS buyers
FROM fct_sales f
JOIN dim_date d ON ... JOIN dim_customer c ON ... JOIN dim_product p ON ...
WHERE d.year = 2024
GROUP BY d.month, c.region, p.category;     -- scans millions/billions of rows
```

Operations known from OLAP cubes: slice, dice, drill-down, roll-up, pivot. Modern engines achieve speed with vectorized execution, compression, partition/data skipping, materialized views and pre-aggregation, and MPP parallelism across nodes. Real-time OLAP (ClickHouse, Druid, Pinot, StarRocks) adds low-latency ingestion and sub-second queries for user-facing analytics.

## Senior angle & interview tips
The OLTP vs OLAP table is a classic interview question; add the nuance. Data flows OLTP to OLAP by ETL/ELT or CDC; OLAP stores are poor at single-row updates and high-rate point writes, so mutations are handled via append plus merge (ReplacingMergeTree, upserts in StarRocks/Iceberg). Choose by latency target and concurrency: BI dashboards with few users suit a warehouse; user-facing high-concurrency analytics suit a real-time OLAP engine with pre-aggregation. HTAP and "zero-ETL" claim to blur the line; know their limits.

Interview question: "OLTP vs OLAP differences?" Model answer: OLTP is row-oriented, normalized, short ACID transactions on current data with high concurrency; OLAP is columnar, denormalized, large aggregate scans over history with few heavy queries, and is typically fed from OLTP through ETL or CDC.
