---
term: "Data mesh"
aliases: ["data products", "domain-oriented data", "federated governance"]
category: general
short: "A decentralized approach where domain teams own and publish data as products, supported by a self-serve platform and federated governance."
related: ["data-contract", "lineage", "data-warehouse"]
---
## In plain words (Junior)
Instead of one central kitchen cooking for the whole company, each department runs its own kitchen and sells well-labeled dishes with quality guarantees to others, using shared appliances and a common health code. Data mesh gives data ownership to the teams who know it best.

## How it works (Mid)
In a centralized model, one data team ingests, models and serves everything, becoming a bottleneck far from the business meaning of the data. Data mesh moves ownership to domain teams (checkout, logistics) who publish data products: well-defined datasets or streams with an owner, documentation, schema and quality contracts, access policies and SLAs, discoverable via a catalog.

```
Four principles (Dehghani):
1. Domain-oriented ownership      - orders team owns "orders" data end-to-end
2. Data as a product              - discoverable, documented, SLA'd, versioned, contract
3. Self-serve data platform       - shared infra so domains don't each reinvent pipelines
4. Federated computational governance - global policies enforced as code, local autonomy
```

A self-serve platform provides paved roads (storage, pipelines, CI, catalog, observability) so domain teams don't need deep infra skills. Federated governance sets global rules (identity, PII, interoperability, naming) enforced automatically (policy as code), while allowing domain autonomy.

## Senior angle & interview tips
Mesh is an organizational pattern more than a technology; it fails without product thinking, funding for domain data engineers, and a strong platform team. Risks: duplicated effort, inconsistent definitions and quality across domains, hard cross-domain joins, and tool sprawl. It suits large organizations with many domains and a central-team bottleneck, not small companies. Data contracts, catalog, lineage and quality SLOs are the enabling mechanisms. It pairs with domain-driven design and event-driven architectures; a hybrid with a central core and domain-owned products is common.

Interview question: "When would you NOT adopt data mesh?" Model answer: in a small or mid-sized org with few domains and a team that isn't a bottleneck; the coordination and platform investment outweighs benefits, and a well-run central platform with clear ownership works better.
