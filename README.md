# DE Interview Prep · آمادگی مصاحبه مهندسی داده

Bilingual (English / فارسی) Data Engineering interview preparation site — **https://dataeng.mohammadmaso.ir**

- **Roadmap**: 17 tracks, 172 topics + 22 design problems (generated from `content-src/roadmap.md`)
- **Glossary**: every term explained for juniors, mids and seniors; terms are auto-linked (with hover tooltips) everywhere
- **Question bank**: ~2,800 interview questions with answers, by technology
- Built with [Astro](https://astro.build) + Tailwind, search by Pagefind, hosted on GitHub Pages

## Develop

```bash
nvm use            # Node >= 22.12
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
