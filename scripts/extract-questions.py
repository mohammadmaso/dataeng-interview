#!/usr/bin/env python3
"""Extract the Q&A question bank from epub/*.epub into src/data/questions/*.json (+ index.json).

Source material: OBenner/data-engineering-interview-questions (credited on the site).
Re-run after `node scripts/parse-roadmap.mjs` so topic mappings resolve.
"""
import json, re, zipfile, html
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "src/data/questions"
OUT.mkdir(parents=True, exist_ok=True)

VOLS = {
    "vol1-sql-and-foundations": 1, "vol2-databases-and-warehouses": 2,
    "vol3-big-data-frameworks": 3, "vol4-cloud-modern-stack-bi": 4,
}
# chapter slug -> topic selectors: "track:N" | "id:foo" | "prefix:bar-"
MAP = {
    "sql": ["track:2"], "data-structure": ["id:dsa-for-de", "id:probabilistic-ds"], "python": ["track:3"],
    "system-design": ["track:14"],
    "dwha": ["id:dwh-fundamentals", "id:mpp-architecture", "id:oltp-vs-olap", "id:columnar-storage", "id:data-lake-object-storage"],
    "data-modeling": ["track:6"],
    "redshift": ["id:cloud-warehouses"], "bigquery": ["id:cloud-warehouses"], "greenplum": ["id:mpp-architecture"],
    "hive": ["id:hadoop-ecosystem"], "cassandra": ["id:nosql-families", "id:nosql-modeling", "id:storage-engines"],
    "hbase": ["id:nosql-families", "id:nosql-modeling", "id:hadoop-ecosystem"], "bigtable": ["id:nosql-families"],
    "mongo": ["id:nosql-families"],
    "hadoop": ["id:hadoop-ecosystem", "id:mapreduce", "id:small-files"],
    "spark": ["prefix:spark-", "id:pyspark-api", "id:small-files"],
    "flink": ["id:flink", "id:stream-concepts", "id:streaming-state", "id:stream-joins"],
    "kafka": ["prefix:kafka-", "id:queues-vs-logs", "id:schema-registry"],
    "impala": ["id:hadoop-ecosystem", "id:trino"], "airflow": ["id:airflow", "id:orchestration-concepts"],
    "nifi": ["id:ingestion-tools", "id:file-ingestion"], "flume": ["id:ingestion-tools", "id:file-ingestion"],
    "avro": ["id:data-formats", "id:schema-registry", "id:schema-evolution"],
    "iceberg": ["id:lakehouse-table-formats", "id:catalogs"], "hudi": ["id:lakehouse-table-formats"],
    "aws": ["id:cloud-warehouses"], "gcp": ["id:cloud-warehouses"], "azure": ["id:cloud-warehouses"],
    "kubernetes": ["id:kubernetes-for-data", "id:spark-on-kubernetes", "id:docker"],
    "dbt": ["id:dbt"], "cdc": ["id:cdc"], "data-quality": ["id:data-quality"],
    "observability": ["id:data-observability", "id:pipeline-monitoring"],
    "data-governance": ["id:governance-contracts-mesh", "id:data-security", "id:data-catalogs"],
    "cost-optimization": ["id:capacity-planning"],
    "tableau": ["id:bi-analytics-engineering"], "looker": ["id:bi-analytics-engineering", "id:semantic-layer"],
    "superset": ["id:bi-analytics-engineering"],
}

# topics available
topics = []
for f in (ROOT / "src/content/topics/en").glob("*.md"):
    fm = f.read_text()
    topics.append((re.search(r'^tid: "(.*?)"', fm, re.M).group(1), re.search(r'^track: "(.*?)"', fm, re.M).group(1)))

def resolve(sels):
    out = []
    for s in sels:
        kind, v = s.split(":", 1)
        for tid, tr in topics:
            if (kind == "track" and tr == v) or (kind == "id" and tid == v) or (kind == "prefix" and tid.startswith(v)):
                if tid not in out: out.append(tid)
    return out

def clean(h: str) -> str:
    h = re.sub(r'<p><a href="[^"]*" class="back">.*?</a></p>\s*', '', h, flags=re.S)
    h = re.sub(r'<(/?)h[1-6]', r'<\1h4', h)  # keep headings below the question title
    return h.strip()

index = []
for epub, vol in VOLS.items():
    z = zipfile.ZipFile(ROOT / "epub" / f"{epub}.epub")
    for name in sorted(n for n in z.namelist() if re.search(r"text/ch\d+\.xhtml$", n)):
        s = z.read(name).decode("utf8")
        m = re.search(r'<section id="ch-([^"]+)"', s)
        if not m: continue
        slug = m.group(1)
        title = re.sub(r"^\d+\.\s*", "", html.unescape(re.search(r"<h1>(.*?)</h1>", s, re.S).group(1)).strip())
        qs = []
        for qm in re.finditer(rf'<section id="({re.escape(slug)}-q\d+)" class="level2">\s*<h2>(.*?)</h2>(.*?)</section>', s, re.S):
            qid, q, body = qm.groups()
            q = re.sub(r"^\d+\.\s*", "", html.unescape(re.sub(r"<[^>]+>", "", q)).strip())
            qs.append({"id": qid, "q": q, "a": clean(body)})
        (OUT / f"{slug}.json").write_text(json.dumps({"slug": slug, "title": title, "vol": vol, "questions": qs}, ensure_ascii=False))
        index.append({"slug": slug, "title": title, "vol": vol, "count": len(qs), "topics": resolve(MAP.get(slug, []))})
        print(f"{slug:20} {len(qs):4}  topics={len(index[-1]['topics'])}")

(ROOT / "src/data/question-index.json").write_text(json.dumps(index, ensure_ascii=False, indent=1))
print("total questions:", sum(c["count"] for c in index), "chapters:", len(index))
