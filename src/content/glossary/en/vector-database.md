---
term: "Vector database"
aliases: ["vector store","ann","hnsw","similarity search"]
category: ml
short: "A store optimized for finding the nearest vectors to a query vector."
related: ["embedding","database-index","sharding"]
---
## In plain words (Junior)
A library that finds the books most similar in meaning to a sentence, not the ones that match exact words.

## How it works (Mid)
It indexes embeddings with approximate nearest neighbor structures (HNSW graphs, IVF with product quantization) and supports metadata filtering. Examples: pgvector, Pinecone, Milvus, Qdrant, OpenSearch.

## Senior angle & interview tips
ANN trades recall for speed; tune ef/nprobe and measure recall. Filtering combined with ANN can hurt recall or latency. Memory footprint is large, so quantization and sharding matter. Often pgvector is enough at moderate scale.

Interview question: "Exact vs approximate nearest neighbor?" Answer: exact scans everything (O(N)); ANN indexes give sub-linear search with a small recall loss.
