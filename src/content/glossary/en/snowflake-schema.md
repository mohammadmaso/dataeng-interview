---
term: "Snowflake schema"
aliases: ["snowflaking"]
category: modeling
short: "A star schema whose dimensions are further normalized into sub-tables."
related: ["star-schema","dimension-table","normalization"]
---
## In plain words (Junior)
A star where each point branches into smaller points: product links to category, which links to department.

## How it works (Mid)
Dimension tables are split into hierarchies (product → subcategory → category → department) to remove redundancy. It saves storage and keeps attribute updates in one place, at the cost of more joins and a more complex model.

## Senior angle & interview tips
Cheap storage and fast columnar joins mean star schemas usually win for BI usability; snowflaking is justified for very large dimensions with shared sub-dimensions or strict governance. It is unrelated to the Snowflake database product, which is a classic confusion in interviews.

Interview question: "Star or snowflake for a BI mart?" Answer: star for simplicity and speed, snowflake only when dimension redundancy is truly costly.
