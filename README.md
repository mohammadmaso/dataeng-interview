# DE Interview Prep · آمادگی مصاحبه مهندسی داده

Bilingual (English / فارسی) Data Engineering interview preparation site — **https://dataeng.mohammadmaso.ir**

- **Roadmap**: 17 tracks, 172 topics + 22 design problems (generated from `content-src/roadmap.md`)
- **Glossary**: every term explained for juniors, mids and seniors; terms are auto-linked (with hover tooltips) everywhere
- **Question bank**: ~2,800 interview questions with answers, by technology
- Built with [Astro](https://astro.build) + Tailwind, search by Pagefind, hosted on GitHub Pages

### Study tools (all client-side; progress lives in the browser's `localStorage`)

| Feature | Where | Notes |
|---|---|---|
| Spaced repetition | `/review/` | Leitner/SM-2-style boxes over question-bank items **and** lesson flashcards; keys `1/2/3` grade, `Space` reveals |
| Question practice | `/questions/<chapter>/` | Per-question self-grading, ★ save, status/difficulty filters, EN⇄FA side-by-side view |
| Interactive lessons | `/topics/<id>/` | Clickable quizzes, flip-card flashcards wired to review, self-check list, runnable SQL blocks |
| Mock interview | `/mock/` | Pick chapters, level, count and a per-question timer; results feed the review schedule |
| Design drill | `/drill/` | Timed design problem with phase checklist, notes and a coverage check |
| SQL playground | `/playground/` | SQLite via `sql.js` (wasm), sample tables and 10 interview patterns; shareable `#q=` links |
| Glossary quiz | `/glossary/quiz/` | Term ⇄ definition multiple choice |
| Progress | `/progress/` | Streak, activity heatmap, weak areas (from review accuracy), backup/restore |
| Study paths | `/paths/` | Curated routes defined in `src/data/paths.ts` (validated at build) |
| Cheat sheets | `/cheatsheets/<track>/` | Printable summaries + flashcards per track |
| Offline | `sw.js`, `manifest.webmanifest` | Visited pages and data stay available offline; installable |

Question difficulty is **estimated** at build time (`src/lib/difficulty.ts`) from answer depth and vocabulary, and labelled as such in the UI.
Lesson flashcards are read from the `## 🗂️` table of each lesson at build time (`src/lib/flashcards.ts`), so no extra authoring is needed.

## Develop

```bash
nvm use            # Node >= 22.12 (CI uses 24)
npm install
npm run dev        # http://localhost:4321
npm run build      # static site in dist/ + search index
```

## Content workflow

| Task | How |
|---|---|
| Regenerate topic stubs from the roadmap | `npm run parse` (never overwrites files that no longer contain `<!-- stub -->`) |
| Write a lesson | Replace the stub marker in `src/content/topics/{en,fa}/<id>.md` with the lesson body |
| Add a glossary term | Add `src/content/glossary/{en,fa}/<slug>.md` (see existing files for frontmatter) |
| Force a glossary link | `[[slug]]` or `[[slug\|label]]` in markdown; first occurrence of any term/alias is linked automatically |
| Re-extract the question bank | `python3 scripts/extract-questions.py` (needs the source EPUBs in `epub/`) |

## Credits

The question bank is derived from [OBenner/data-engineering-interview-questions](https://github.com/OBenner/data-engineering-interview-questions).
