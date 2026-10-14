---
term: "Conformed dimension"
aliases: ["shared dimension","master dimension"]
category: modeling
short: "A dimension with identical meaning and keys shared across multiple fact tables."
related: ["dimension-table","star-schema","fact-table"]
---
## In plain words (Junior)
One official customer list used by sales, support and marketing, so everyone counts the same customers the same way.

## How it works (Mid)
A conformed dimension (date, customer, product) has one definition, one set of keys and attribute names, reused by many facts. This enables drill-across: combining metrics from different fact tables by shared dimension attributes. Kimball’s bus matrix lists which facts use which conformed dimensions.

## Senior angle & interview tips
Without conformance you get data silos with conflicting definitions of "customer" or "active". Achieving it is mostly organizational: agreeing on definitions, owners and keys. It underpins data mesh interoperability too.

Interview question: "How do you compare revenue and support tickets per customer?" Answer: both facts join to the same conformed customer dimension, then aggregate each separately and join the results.
