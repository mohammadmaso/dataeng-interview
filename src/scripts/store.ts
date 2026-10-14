// Client-side learner state. Everything lives in localStorage; no server involved.
// Keys are namespaced `de:*` and every read/write is wrapped so private-mode browsers keep working.

export type Grade = 0 | 1 | 2; // 0 = missed, 1 = unsure, 2 = got it
export interface Card { b: number; d: number; r: number; l: number; t: number; g?: string } // box, due day, reps, lapses, last-seen day, group
export type Srs = Record<string, Card>;
export interface Session { ts: number; kind: 'mock' | 'review' | 'quiz' | 'glossary' | 'drill'; total: number; correct: number; label?: string; secs?: number }

const K = {
  read: 'de:read', known: 'de:known', srs: 'de:srs', bm: 'de:bm', act: 'de:act', last: 'de:last',
  quiz: 'de:quiz', sessions: 'de:sessions', checks: 'de:checks', goal: 'de:goal',
};
export const KEYS = K;

// Box → days until next review. Box 0 means "again today".
export const LADDER = [0, 1, 3, 7, 16, 35, 75, 150];

function load<T>(k: string, fallback: T): T {
  try { const v = localStorage.getItem(k); return v ? (JSON.parse(v) as T) : fallback; } catch { return fallback; }
}
function save(k: string, v: unknown) {
  try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* storage full or blocked */ }
  document.dispatchEvent(new CustomEvent('de:change', { detail: { key: k } }));
}

/** Days since epoch in the learner's local timezone. */
export const today = () => {
  const d = new Date();
  return Math.floor((d.getTime() - d.getTimezoneOffset() * 60000) / 86400000);
};
export const dayToDate = (day: number) => new Date(day * 86400000 + new Date().getTimezoneOffset() * 60000);

// ---------- SRS ----------
export const getSrs = () => load<Srs>(K.srs, {});
export function grade(id: string, g: Grade, group?: string): Card {
  const all = getSrs();
  const c: Card = all[id] ?? { b: 0, d: today(), r: 0, l: 0, t: today(), g: group };
  c.r++; c.t = today(); if (group) c.g = group;
  if (g === 0) { c.b = 0; c.l++; c.d = today(); }
  else if (g === 1) { c.b = Math.max(1, c.b); c.d = today() + Math.max(1, Math.round(LADDER[c.b] * 0.5)); }
  else { c.b = Math.min(LADDER.length - 1, c.b + 1); c.d = today() + LADDER[c.b]; }
  all[id] = c; save(K.srs, all); bump();
  return c;
}
/** Add cards to the review schedule without grading them (due immediately). Returns how many were new. */
export function enroll(ids: string[], group?: string): number {
  const all = getSrs(); let n = 0;
  for (const id of ids) if (!all[id]) { all[id] = { b: 0, d: today(), r: 0, l: 0, t: today(), g: group }; n++; }
  if (n) save(K.srs, all);
  return n;
}
export const isDue = (c: Card, day = today()) => c.d <= day;
export function dueIds(filter?: (id: string, c: Card) => boolean): string[] {
  const day = today();
  return Object.entries(getSrs()).filter(([id, c]) => isDue(c, day) && (!filter || filter(id, c)))
    .sort((a, b) => a[1].d - b[1].d).map(([id]) => id);
}
export const srsStats = () => {
  const all = Object.values(getSrs()); const day = today();
  return {
    total: all.length,
    due: all.filter((c) => c.d <= day).length,
    mature: all.filter((c) => c.b >= 4).length,
    learning: all.filter((c) => c.b < 4).length,
  };
};
export const resetCard = (id: string) => { const a = getSrs(); delete a[id]; save(K.srs, a); };

// ---------- Topic progress ----------
export const getList = (k: 'read' | 'known'): string[] => load<string[]>(K[k], []);
export function toggleList(k: 'read' | 'known', id: string) {
  const a = getList(k); const i = a.indexOf(id);
  if (i > -1) a.splice(i, 1); else { a.push(id); bump(); }
  save(K[k], a);
}

// ---------- Bookmarks (t:<tid>, q:<id>, g:<slug>) ----------
export const getBm = () => load<string[]>(K.bm, []);
export const hasBm = (id: string) => getBm().includes(id);
export function toggleBm(id: string): boolean {
  const a = getBm(); const i = a.indexOf(id);
  if (i > -1) a.splice(i, 1); else a.push(id);
  save(K.bm, a); return i < 0;
}

// ---------- Activity / streak ----------
export const getAct = () => load<Record<string, number>>(K.act, {});
export function bump(n = 1) {
  const a = getAct(); const d = String(today());
  a[d] = (a[d] ?? 0) + n;
  // keep one year
  const cut = today() - 400;
  for (const k of Object.keys(a)) if (+k < cut) delete a[k];
  try { localStorage.setItem(K.act, JSON.stringify(a)); } catch { /* ignore */ }
  document.dispatchEvent(new CustomEvent('de:change', { detail: { key: K.act } }));
}
export function streak(): { current: number; best: number; activeToday: boolean } {
  const a = getAct(); const t = today();
  const days = Object.keys(a).map(Number).sort((x, y) => x - y);
  let best = 0, run = 0, prev = -999;
  for (const d of days) { run = d === prev + 1 ? run + 1 : 1; best = Math.max(best, run); prev = d; }
  let cur = 0; let d = a[String(t)] ? t : t - 1;
  while (a[String(d)]) { cur++; d--; }
  return { current: cur, best, activeToday: !!a[String(t)] };
}

// ---------- Last visited ----------
export const getLast = () => load<{ tid: string; title: string; lang: string } | null>(K.last, null);
export const setLast = (v: { tid: string; title: string; lang: string }) => { try { localStorage.setItem(K.last, JSON.stringify(v)); } catch { /* ignore */ } };

// ---------- Quiz scores per topic ----------
export type QuizRes = Record<string, { s: number; n: number; t: number }>;
export const getQuiz = () => load<QuizRes>(K.quiz, {});
export function setQuiz(tid: string, s: number, n: number) {
  const q = getQuiz(); q[tid] = { s, n, t: Date.now() }; save(K.quiz, q); bump();
}

// ---------- Sessions (mock / review history) ----------
export const getSessions = () => load<Session[]>(K.sessions, []);
export function addSession(s: Omit<Session, 'ts'>) {
  const a = getSessions(); a.push({ ...s, ts: Date.now() });
  save(K.sessions, a.slice(-200)); bump(Math.max(1, s.total));
}

// ---------- Self-check checklists (key -> ticked indexes) ----------
export const getChecks = () => load<Record<string, number[]>>(K.checks, {});
export function setChecks(key: string, v: number[]) { const c = getChecks(); if (v.length) c[key] = v; else delete c[key]; save(K.checks, c); }

// ---------- Daily goal ----------
export const getGoal = () => load<number>(K.goal, 10);
export const setGoal = (n: number) => save(K.goal, n);
export const doneToday = () => getAct()[String(today())] ?? 0;

// ---------- Backup ----------
export function exportAll(): string {
  const out: Record<string, unknown> = { v: 1, at: new Date().toISOString() };
  for (const [name, key] of Object.entries(K)) { try { const v = localStorage.getItem(key); if (v) out[name] = JSON.parse(v); } catch { /* skip */ } }
  return JSON.stringify(out, null, 2);
}
export function importAll(text: string): number {
  const data = JSON.parse(text); let n = 0;
  if (typeof data !== 'object' || !data) throw new Error('bad file');
  for (const [name, key] of Object.entries(K)) {
    if (name in data) { localStorage.setItem(key, JSON.stringify(data[name])); n++; }
  }
  document.dispatchEvent(new CustomEvent('de:change', { detail: { key: '*' } }));
  return n;
}
export function resetAll() {
  for (const key of Object.values(K)) { try { localStorage.removeItem(key); } catch { /* ignore */ } }
  document.dispatchEvent(new CustomEvent('de:change', { detail: { key: '*' } }));
}

// ---------- Shared helpers ----------
export const ui = (): Record<string, any> => {
  try { return JSON.parse(document.getElementById('ui-json')?.textContent ?? '{}'); } catch { return {}; }
};
export const lang = () => document.documentElement.lang as 'en' | 'fa';
export const digits = (n: number | string) => lang() === 'fa' ? String(n).replace(/\d/g, (d) => '۰۱۲۳۴۵۶۷۸۹'[+d]) : String(n);
export const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));
export const shuffle = <T,>(a: T[]): T[] => { const b = a.slice(); for (let i = b.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [b[i], b[j]] = [b[j], b[i]]; } return b; };

/** Question id → chapter slug. Ids look like `<slug>-q<number>` and slugs may contain hyphens. */
export const chapterOf = (qid: string) => qid.slice(0, qid.lastIndexOf('-q'));

const cache = new Map<string, Promise<any>>();
export function fetchJson<T = any>(url: string): Promise<T> {
  if (!cache.has(url)) cache.set(url, fetch(url).then((r) => { if (!r.ok) throw new Error(url + ' ' + r.status); return r.json(); }));
  return cache.get(url)!;
}
export interface QItem { id: string; q: string; a: string; d: 0 | 1 | 2 }
export const loadChapter = (l: string, slug: string) => fetchJson<QItem[]>(`/${l}/data/questions/${slug}.json`);
export const loadMeta = (l: string) => fetchJson<Meta>(`/${l}/data/meta.json`);
export interface Meta {
  chapters: { slug: string; title: string; vol: number; count: number; topics: string[] }[];
  topics: { tid: string; title: string; track: string; level: string; priority: string; type: string; hours: number; covers: string[]; summary: string }[];
  tracks: { id: string; title: string }[];
}

/** Tiny safe renderer for the inline markdown used in flashcards (already HTML-escaped at build time). */
export const html = (s: string) => s;
