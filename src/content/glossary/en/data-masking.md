---
term: "PII masking and anonymization"
aliases: ["pii", "data masking", "pseudonymization", "tokenization", "anonymization", "gdpr", "right to be forgotten"]
category: quality
short: "Protecting personal data by masking, hashing, tokenizing or generalizing it, and supporting deletion requests across the data platform."
related: ["data-catalog", "data-lake", "data-quality"]
---
## In plain words (Junior)
When sharing a class list, you might black out names or replace them with student numbers so people can still analyze results without knowing who is who. Masking personal data does that: analysts can work with the data while the sensitive identity stays protected.

## How it works (Mid)
PII (names, emails, phones, IDs, location, IPs) is regulated by laws like GDPR. Techniques: redaction/masking (hide part), hashing (one-way; use a keyed hash or HMAC with secret, since plain hashes of low-entropy values like phone numbers are reversible by brute force), tokenization (replace with a token, mapping in a secured vault), encryption (field-level or at-rest), generalization/k-anonymity (reduce precision), and differential privacy for aggregates. Pseudonymized data is still personal data; truly anonymous data cannot be re-identified.

```
-- Pseudonymize: stable token, joins still work, raw value is not exposed
SELECT sha256(concat(email, :secret_salt)) AS email_token FROM users;     -- keyed hash / HMAC
-- Mask for analysts
SELECT regexp_replace(phone, '\d(?=\d{4})', '*') AS phone_masked FROM users;
-- Generalize
SELECT date_trunc('year', birth_date) AS birth_year, left(zip, 3) AS zip3 FROM users;
-- Column policy (warehouse): analysts see masked, privileged roles see raw
```

Platform controls: classify and tag PII in the catalog, column-level access policies and dynamic masking by role, encryption in transit and at rest, audit logs, and keeping raw PII in a restricted zone, with downstream layers receiving tokens.

## Senior angle & interview tips
Right-to-erasure is the hard data-engineering part: data is copied across raw, silver, gold, backups and ML features, in immutable files and Kafka logs. Strategies: keep PII in a separate small table keyed by a surrogate id so deleting one row cuts the link (crypto-shredding: delete the encryption key for a subject), run delete jobs via Iceberg/Delta `DELETE` plus compaction and snapshot expiry to physically remove old files, and track subject-id lineage. Joins on tokens must be consistent across systems (same salt/key), while rotating keys needs planning. Logging and debugging output are common leak points.

Interview question: "How do you handle a GDPR deletion request in a lakehouse?" Model answer: find the subject through its stable id, delete or tombstone rows in all tables (ideally PII isolated in one table or crypto-shredded), then compact and expire snapshots so old files containing the data are physically removed, and verify across backups and downstream copies.
