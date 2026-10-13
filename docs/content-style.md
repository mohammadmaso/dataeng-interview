# Lesson Style Guide (read fully before writing)

Audience: Persian-speaking engineers preparing for Data Engineering interviews. One page must serve a **junior, a mid and a senior**. Lessons are *learning material first, answer-key second*.

## Learning-science rules baked into every lesson
1. **Progressive disclosure (simple → advanced)** — each lesson climbs three layers; the reader can stop at the layer they need.
2. **Concrete before abstract** — start with an everyday analogy or a tiny worked example, then name the concept.
3. **Worked examples** — show a full small example (SQL / Python / config / diagram) before asking the reader to generalize.
4. **Predict-then-reveal** — pose a short question, then give the answer inside `<details>`; this forces retrieval, which is the strongest memory technique.
5. **Retrieval practice** — end with a quiz and flashcards (no re-reading).
6. **Misconceptions first-class** — a "Common mistakes" section for each lesson.
7. **Interview framing** — how the topic is asked, what a mid vs senior answer sounds like, and a model answer in 3–5 sentences.
8. **Connect ideas** — link prerequisites/related topics and glossary terms.

## File format
Path: `src/content/topics/{en,fa}/<tid>.md` — **keep the existing frontmatter exactly** (do not change `tid`, `order`, `track`, ...); only (a) replace the line `<!-- stub -->` with the lesson body and (b) you MAY fill `prerequisites` and `related` with existing topic ids (JSON arrays, ids must exist in `src/content/topics/en/`). Do not leave `<!-- stub -->` in a finished lesson. Keep `summary` and `covers` as they are.

Body uses plain Markdown (GFM). Headings start at `##` (the page title is already `#`).

### Required section order (EN heading → FA heading)
```
## 🎯 TL;DR                         → ## 🎯 خلاصه
## 🌱 Start simple (Junior)         → ## 🌱 از ساده شروع کنیم (مبتدی)
## 🔧 How it really works (Mid)     → ## 🔧 واقعاً چطور کار می‌کند (متوسط)
## 🧠 Senior depth & trade-offs     → ## 🧠 عمق و trade-offها (سنیور)
## 🧪 Try it                        → ## 🧪 امتحان کن
## 💬 Interview corner              → ## 💬 گوشه‌ی مصاحبه
## ⚠️ Common mistakes               → ## ⚠️ اشتباهات رایج
## ✅ Check yourself (quiz)         → ## ✅ خودت را بسنج
## 🗂️ Flashcards                   → ## 🗂️ فلش‌کارت
## 🔗 Go further                    → ## 🔗 ادامه
```
Type-specific adjustments: `problem` lessons replace "Start simple/How it works" with the 11-step problem template (statement, functional reqs, non-functional reqs, estimation, data model, high-level design, deep dives, expectations per level Mid/Senior/Staff, follow-ups, mistakes, quiz). `guide` lessons may drop "Try it" and flashcards. `practice` lessons must contain real exercises with solutions in `<details>`.

### Component conventions (plain markdown/HTML, no MDX)
- **Callouts**: blockquote with bold label: `> **💡 Analogy:** …`, `> **⚠️ Watch out:** …`, `> **🎤 Say this in the interview:** …`.
- **Reveal**: `<details><summary>Your guess?</summary>\n\nanswer…\n\n</details>` (blank lines inside are required).
- **Quiz**: 4–5 questions. Each: question, options as a list `- A) …`, then `<details><summary>Answer</summary>\n\n**B** — why…\n\n</details>`. Mix recall + scenario questions; at least one senior-level "why/trade-off" question.
- **Flashcards**: a table `| Front | Back |` with 5–8 rows.
- **Diagrams**: fenced ```mermaid blocks (flowchart, sequenceDiagram, etc.), 1–3 per lesson where structure/flow matters. Keep labels short; always Latin text inside mermaid even in FA lessons.
- **Code**: fenced with language (`sql`, `python`, `yaml`, `bash`, `text`). Code is identical in EN and FA; only comments may be translated. Make examples runnable and small.
- **Glossary links**: the build auto-links the first occurrence of known terms — don't hand-link. To force one: `[[slug]]` / `[[slug|label]]` where slug exists in `src/content/glossary/en/`. `[[topic-id]]` / `[[topic-id|label]]` links to another lesson (id must exist in `src/content/topics/en/`). Never invent slugs or ids.

## Voice and depth
- Plain words first; define every acronym once. Short paragraphs (≤4 lines), bullets for lists, tables for comparisons.
- Junior layer: no jargon without an analogy; ~150–250 words + one example.
- Mid layer: mechanisms, a concrete example with numbers/code, when to use it.
- Senior layer: failure modes, scale numbers, trade-offs, how it behaves in Kafka/Spark/Flink/ClickHouse/StarRocks/Iceberg/Airflow in practice, what you would monitor, when NOT to use it.
- Length target: 1,500–2,600 words per EN lesson including code (problems up to 3,200). FA lesson is a faithful, natural Persian rendering (not word-for-word), same structure, same code, same quiz answers.
- Persian: natural, friendly "تو" tone consistent with the site; keep established English tech names in Latin (Kafka, Spark, Parquet, exactly-once, watermark, shuffle); use «» or "" for emphasis; add the English term in parentheses on first use of a Persian rendering, e.g. «خودتوانی (idempotency)». Persian digits are NOT required inside code or tables with numbers.
- Iranian context where it helps (on-prem stacks, Jalali dates, telco/bank scale) — only when true and relevant.

## Accuracy rules (non-negotiable)
- Be precise on semantics (exactly-once, watermarks, CAP, isolation levels, join algorithms). If unsure, say less rather than guess.
- Do not state version-specific facts, benchmark numbers or config defaults unless you are certain or verified them with Context7 (library docs) — prefer principles + "check the docs for your version".
- Numbers in estimations must be internally consistent (show the arithmetic).
- Every code example must be correct for the stated engine/dialect.

## Self-check before finishing a lesson
All sections present in order · stub marker removed · frontmatter untouched · EN and FA structurally identical · quiz answers correct · mermaid syntax valid · no invented glossary slugs · no claims you cannot back up.
