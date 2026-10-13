// Build-time glossary matcher shared by the remark plugin and page templates.
import fs from 'node:fs';
import path from 'node:path';

const DIR = path.resolve(process.cwd(), 'src/content/glossary');
const cache = new Map();

const unquote = (s) => s.trim().replace(/^["']|["']$/g, '');

function parseFront(raw) {
  const m = raw.match(/^---\n([\s\S]*?)\n---/);
  if (!m) return null;
  const out = {};
  for (const line of m[1].split('\n')) {
    const kv = line.match(/^(\w+):\s*(.*?)(?:\s+#.*)?$/);
    if (!kv) continue;
    const [, k, v] = kv;
    if (v.startsWith('[')) {
      try { out[k] = JSON.parse(v); } catch { out[k] = [...v.matchAll(/["']([^"']+)["']/g)].map((x) => x[1]); }
    } else out[k] = unquote(v);
  }
  return out;
}

export function loadGlossary(lang) {
  if (cache.has(lang)) return cache.get(lang);
  const dir = path.join(DIR, lang);
  const entries = [];
  if (fs.existsSync(dir)) {
    for (const f of fs.readdirSync(dir)) {
      if (!f.endsWith('.md')) continue;
      const fm = parseFront(fs.readFileSync(path.join(dir, f), 'utf8'));
      if (!fm?.term) continue;
      const slug = f.replace(/\.md$/, '');
      const keys = new Set();
      const base = fm.term.replace(/\s*\(.*\)\s*$/, '');
      keys.add(base);
      if (fm.term_en) keys.add(fm.term_en);
      const paren = fm.term.match(/\((.+)\)\s*$/);
      if (paren) keys.add(paren[1]);
      for (const a of fm.aliases ?? []) keys.add(a);
      entries.push({ slug, term: fm.term, short: fm.short ?? '', keys: [...keys].filter((k) => k.length > 1) });
    }
  }
  const bySlug = new Map(entries.map((e) => [e.slug, e]));
  // longest keys first so "data skew" wins over "skew"
  const flat = entries.flatMap((e) => e.keys.map((k) => ({ key: k, entry: e }))).sort((a, b) => b.key.length - a.key.length);
  const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regexes = flat.map(({ key, entry }) => ({
    entry,
    re: new RegExp(`(?<![\\p{L}\\p{N}_-])${esc(key)}(?![\\p{L}\\p{N}_-])`, 'iu'),
  }));
  const val = { entries, bySlug, regexes };
  cache.set(lang, val);
  return val;
}

/** Find the earliest-matching glossary term in `text`, ignoring slugs already used. */
export function findNext(text, lang, used) {
  const { regexes } = loadGlossary(lang);
  let best = null;
  for (const { entry, re } of regexes) {
    if (used.has(entry.slug)) continue;
    const m = re.exec(text);
    if (m && (!best || m.index < best.index || (m.index === best.index && m[0].length > best.text.length))) {
      best = { index: m.index, text: m[0], entry };
    }
  }
  return best;
}

const escHtml = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

/** Plain text -> HTML string with first-occurrence glossary links. */
export function linkText(text, lang, used = new Set()) {
  let rest = text;
  let html = '';
  for (;;) {
    const hit = findNext(rest, lang, used);
    if (!hit) { html += escHtml(rest); break; }
    html += escHtml(rest.slice(0, hit.index));
    used.add(hit.entry.slug);
    html += `<a class="gloss" href="/${lang}/glossary/${hit.entry.slug}/" data-tip="${escHtml(hit.entry.short)}">${escHtml(hit.text)}</a>`;
    rest = rest.slice(hit.index + hit.text.length);
  }
  return html;
}

/** Slugs of glossary terms mentioned in text. */
export function mentioned(text, lang) {
  const out = new Set();
  for (const { entry, re } of loadGlossary(lang).regexes) if (re.test(text)) out.add(entry.slug);
  return out;
}
