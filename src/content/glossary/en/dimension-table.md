---
term: "Dimension table"
aliases: ["dimension tables", "dimensions", "conformed dimension", "degenerate dimension"]
category: modeling
short: "A dimensional-model table with descriptive attributes (who, what, where, when) used to filter, group and label facts."
related: ["fact-table", "star-schema", "scd", "surrogate-key"]
---
## In plain words (Junior)
If the fact table is the diary of events, dimension tables are the reference cards explaining the words in it: what is this product, who is this customer, what day of the week was that date. You use them to filter and group the numbers, for example "total sales for category Electronics in Spring".

## How it works (Mid)
Dimensions have a surrogate primary key, the natural/business key, and many descriptive columns, often hierarchies flattened into one row (product, brand, category). They are comparatively small and wide, and are denormalized to avoid extra joins. They give the labels and the filter and group-by attributes used in reports.

```
CREATE TABLE dim_product (
  product_sk   BIGINT PRIMARY KEY,
  product_id   VARCHAR,        -- natural key from source
  name         VARCHAR,
  category     VARCHAR,        -- denormalized hierarchy
  brand        VARCHAR,
  valid_from   DATE, valid_to DATE, is_current BOOLEAN   -- SCD2
);
-- special rows: product_sk = -1 -> 'Unknown'
```

Notable kinds: the date dimension (pre-generated, with fiscal periods, holidays, weekday flags), conformed dimensions shared by several facts, role-playing dimensions (one date table used as order date and ship date), junk dimensions (combine low-cardinality flags), degenerate dimensions (identifiers kept in the fact), and mini-dimensions for fast-changing attributes. Slowly changing attributes are handled with SCD types.

## Senior angle & interview tips
Quality of dimensions drives analytics quality: stable keys, unique natural keys per version, no overlapping validity ranges, and clear handling of unknown or late-arriving members (insert an inferred stub row, fix later). Wide dimensions are fine in columnar stores. Avoid putting rapidly changing numeric measures into dimensions (they belong in facts) and avoid snowflaking unless there is a strong reason. In distributed engines, small dimensions are broadcast to join with large facts; huge dimensions (customers with hundreds of millions of rows) need co-partitioning.

Interview question: "Fact or dimension: customer's credit score?" Model answer: it depends on how it is used and how it changes; a slowly changing descriptive attribute can be in the dimension (SCD), while a frequently changing numeric value used for aggregation is better as a measure in a snapshot fact or a mini-dimension band.
