---
term: "Lambda architecture"
aliases: ["lambda"]
category: batch
short: "Run a batch layer and a speed layer in parallel and merge results at query time."
related: ["kappa-architecture","batch-processing","stream-processing"]
---
## In plain words (Junior)
Two kitchens: a slow one that cooks the perfect, fully checked meal later, and a fast one that serves a quick approximate plate now. You get speed immediately and accuracy eventually.

## How it works (Mid)
Raw data goes to both a batch layer (recomputes accurate views from all history) and a speed layer (streaming, low-latency incremental views). A serving layer merges them at query time. The batch result eventually overwrites the speed layer output for the same period.

## Senior angle & interview tips
Main drawback: two code paths computing the same logic, which drift apart and double maintenance. Modern lakehouse engines with unified batch/stream APIs, or Kappa, reduce the need for it. It still makes sense when streaming results are approximate by design (sketches) and a precise nightly recompute is required.

Interview question: "Why did people move away from Lambda?" Answer: duplicated logic and reconciliation pain; replayable logs and unified engines made one pipeline enough.
