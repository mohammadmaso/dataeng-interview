// Validates written lessons (files without the stub marker). Usage: node scripts/validate-lessons.mjs [--strict]
import fs from 'node:fs';
import path from 'node:path';

const ROOT = path.resolve(import.meta.dirname, '..');
const T = path.join(ROOT, 'src/content/topics');
const G = path.join(ROOT, 'src/content/glossary/en');
const glossary = new Set(fs.existsSync(G) ? fs.readdirSync(G).map((f) => f.replace(/\.md$/, '')) : []);
const ids = new Set(fs.readdirSync(path.join(T, 'en')).map((f) => f.replace(/\.md$/, '')));

const SECTIONS = {
  en: ['TL;DR', 'Start simple', 'How it really works', 'Senior depth', 'Try it', 'Interview corner', 'Common mistakes', 'Check yourself', 'Flashcards', 'Go further'],
  fa: ['خلاصه', 'از ساده شروع کنیم', 'واقعاً چطور کار می‌کند', 'عمق و trade', 'امتحان کن', 'گوشه‌ی مصاحبه', 'اشتباهات رایج', 'خودت را بسنج', 'فلش‌کارت', 'ادامه'],
};
const OPTIONAL = { guide: [4, 8], problem: [1, 2, 4], practice: [] }; // indexes that may be missing per type

const only = process.argv.find((a) => a.startsWith('--only='))?.slice(7).split(',');
let errors = 0, done = 0, stubs = 0;
const err = (f, m) => { errors++; console.error(`✗ ${f}: ${m}`); };

for (const lang of ['en', 'fa']) {
  for (const f of fs.readdirSync(path.join(T, lang))) {
    if (only && !only.includes(f.replace(/\.md$/, ''))) continue;
    const file = path.join(T, lang, f);
    const raw = fs.readFileSync(file, 'utf8');
    if (raw.includes('<!-- stub -->')) { stubs++; continue; }
    done++;
    const rel = `${lang}/${f}`;
    const m = raw.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
    if (!m) { err(rel, 'bad frontmatter'); continue; }
    const type = m[1].match(/^type: "(.*?)"/m)?.[1];
    const body = m[2];
    const heads = [...body.matchAll(/^## (.+)$/gm)].map((x) => x[1]);
    let pos = -1;
    SECTIONS[lang].forEach((s, i) => {
      const at = heads.findIndex((h) => h.includes(s));
      if (at < 0) { if (!(OPTIONAL[type] ?? []).includes(i)) err(rel, `missing section "${s}"`); return; }
      if (at < pos) err(rel, `section out of order: "${s}"`);
      pos = Math.max(pos, at);
    });
    for (const g of body.matchAll(/\[\[([a-z0-9-]+)(?:\|[^\]]+)?\]\]/g)) if (!glossary.has(g[1]) && !ids.has(g[1])) err(rel, `unknown [[${g[1]}]] (not a glossary slug or topic id)`);
    for (const k of ['prerequisites', 'related']) {
      const arr = JSON.parse(m[1].match(new RegExp(`^${k}: (.*)$`, 'm'))?.[1] ?? '[]');
      for (const id of arr) if (!ids.has(id)) err(rel, `${k}: unknown topic id "${id}"`);
    }
    const fences = (body.match(/^```/gm) ?? []).length;
    if (fences % 2) err(rel, 'unbalanced code fences');
    if ((body.match(/<details>/g) ?? []).length !== (body.match(/<\/details>/g) ?? []).length) err(rel, 'unbalanced <details>');
    if (!/<details>/.test(body) && type !== 'guide') err(rel, 'no <details> reveal/quiz answers');
    const words = body.split(/\s+/).length;
    if (words < (lang === 'fa' ? 500 : 600)) err(rel, `too short (${words} words)`);
    // EN/FA parity
    if (lang === 'fa') {
      const en = path.join(T, 'en', f);
      if (!fs.existsSync(en) || fs.readFileSync(en, 'utf8').includes('<!-- stub -->')) err(rel, 'EN lesson missing/stub');
      else {
        const eb = fs.readFileSync(en, 'utf8');
        const c = (s, re) => (s.match(re) ?? []).length;
        if (c(eb, /^```/gm) !== c(body, /^```/gm)) err(rel, 'code-fence count differs from EN');
        if (c(eb, /<details>/g) !== c(body, /<details>/g)) err(rel, '<details> count differs from EN');
      }
    }
  }
}
console.log(`lessons written: ${done / 2} topics (${done} files) · stubs left: ${stubs} · errors: ${errors}`);
process.exit(errors ? 1 : 0);
