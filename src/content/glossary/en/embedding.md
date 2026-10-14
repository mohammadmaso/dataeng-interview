---
term: "Embedding"
aliases: ["vector embedding","vector representation"]
category: ml
short: "A list of numbers representing the meaning of text, an image or an item so similar things are close together."
related: ["vector-database","feature-store"]
---
## In plain words (Junior)
Turning words into coordinates on a map where "cat" lands near "kitten" and far from "invoice".

## How it works (Mid)
A model maps an input to a dense vector (hundreds to thousands of dimensions). Similarity is measured with cosine similarity or dot product. Used for semantic search, recommendations, clustering and RAG.

## Senior angle & interview tips
Data engineering concerns: keeping embeddings in sync with source data (re-embed on change), model/version management (vectors from different models are incompatible), batch vs streaming generation, cost, and storing them with metadata in a vector store.

Interview question: "The embedding model changed; what happens to the index?" Answer: all vectors must be recomputed and the index rebuilt, since spaces are not comparable.
