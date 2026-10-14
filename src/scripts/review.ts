import { today, getSrs, dueIds, srsStats, getBm, chapterOf, loadChapter, loadMeta, fetchJson, lang, ui, esc, digits, shuffle, getGoal, doneToday, streak } from './store';
import { runSession, type Item } from './runner';

const root = document.getElementById('review-app')!;
const L = lang(); const a = ui().app; const BATCH = 25;

type Flash = Record<string, { t: string; k: string; c: string[][] }>;
const loadFlash = () => fetchJson<Flash>(`/${L}/data/flashcards.json`);

/** Resolve a list of SRS ids into displayable items. Unknown ids are skipped. */
async function toItems(ids: string[]): Promise<Item[]> {
  const meta = await loadMeta(L);
  const titles = new Map(meta.chapters.map((c) => [c.slug, c.title]));
  const flash = ids.some((i) => i.startsWith('fc:')) ? await loadFlash() : {};
  const chapters = [...new Set(ids.filter((i) => !i.startsWith('fc:')).map(chapterOf))];
  const loaded = new Map<string, Map<string, { q: string; a: string; d: 0 | 1 | 2 }>>();
  await Promise.all(chapters.map(async (c) => {
    try { loaded.set(c, new Map((await loadChapter(L, c)).map((x) => [x.id, x]))); } catch { /* chapter missing */ }
  }));
  const out: Item[] = [];
  for (const id of ids) {
    if (id.startsWith('fc:')) {
      const [, tid, idx] = id.split(':'); const card = flash[tid]?.c[+idx]; if (!card) continue;
      out.push({ id, front: card[0], back: card[1], label: flash[tid].t, group: 'lesson:' + tid, href: `/${L}/topics/${tid}/` });
    } else {
      const ch = chapterOf(id); const q = loaded.get(ch)?.get(id); if (!q) continue;
      out.push({ id, front: esc(q.q), back: q.a, label: titles.get(ch) ?? ch, group: ch, href: `/${L}/questions/${ch}/#${id}`, d: q.d });
    }
  }
  return out;
}

async function chapterDeck(slug: string, n = 15): Promise<Item[]> {
  const rows = await loadChapter(L, slug); const srs = getSrs(); const day = today();
  const due = rows.filter((r) => srs[r.id] && srs[r.id].d <= day + 1);
  const fresh = shuffle(rows.filter((r) => !srs[r.id]));
  const rest = shuffle(rows.filter((r) => srs[r.id] && srs[r.id].d > day + 1));
  return toItems([...due, ...fresh, ...rest].slice(0, n).map((r) => r.id));
}
async function topicDeck(tid: string): Promise<Item[]> {
  const f = (await loadFlash())[tid]; if (!f) return [];
  return toItems(f.c.map((_, i) => `fc:${tid}:${i}`));
}

function start(items: Item[], label: string, kind: 'review' = 'review') {
  if (!items.length) { root.innerHTML = `<p class="card p-6 text-center" style="color:var(--fg-muted)">${a.nothingToReview}</p><div class="mt-4 text-center"><button class="rounded-xl border px-4 py-2 font-semibold" style="border-color:var(--border)" id="back">${a.backToPractice}</button></div>`; root.querySelector('#back')!.addEventListener('click', () => setup()); return; }
  runSession({ root, items, kind, label, onExit: () => { history.replaceState(null, '', location.pathname); setup(); }, onRetryMissed: (m) => start(m, label, kind) });
  root.scrollIntoView({ block: 'start' });
}

async function setup() {
  const meta = await loadMeta(L); const st = srsStats(); const sg = streak(); const goal = getGoal(); const done = doneToday();
  const flash = await loadFlash().catch(() => ({} as Flash));
  const chOpts = meta.chapters.map((c) => `<option value="${c.slug}">${esc(c.title)} (${digits(c.count)})</option>`).join('');
  const lessonOpts = Object.entries(flash).map(([tid, f]) => `<option value="${tid}">${esc(f.t)}</option>`).join('');
  const stat = (v: number | string, l: string) => `<div class="card p-4 text-center"><div class="text-2xl font-extrabold" style="color:var(--accent)">${digits(v)}</div><div class="text-xs" style="color:var(--fg-muted)">${l}</div></div>`;
  root.innerHTML = `
    <div class="grid grid-cols-2 gap-3 sm:grid-cols-5">
      ${stat(st.due, a.dueToday)}${stat(st.learning, a.learning)}${stat(st.mature, a.mature)}${stat(st.total, a.total)}${stat(sg.current, ui().app.streakDays)}
    </div>
    <div class="card mt-5 p-5">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div><div class="font-semibold">${a.todayGoal}: ${digits(Math.min(done, goal))} ${a.goalOf} ${digits(goal)} ${a.cardsGoal}</div>
          <div class="mt-2 h-2 w-64 max-w-full overflow-hidden rounded-full" style="background:var(--bg-soft)"><div class="h-full rounded-full" style="background:var(--accent);width:${Math.min(100, Math.round((done / goal) * 100))}%"></div></div></div>
        ${st.due ? `<button id="go-due" class="rounded-xl px-6 py-3 font-semibold text-white" style="background:var(--accent)">${a.startReview} (${digits(Math.min(st.due, BATCH))})</button>` : `<p style="color:var(--fg-muted)">${a.allCaughtUp}</p>`}
      </div>
    </div>
    <div class="mt-6 grid gap-4 md:grid-cols-2">
      <div class="card p-5"><h2 class="font-bold">${a.practiceChapter}</h2>
        <select id="sel-ch" class="mt-3 w-full rounded-lg border px-3 py-2 text-sm" style="border-color:var(--border);background:var(--bg-soft);color:var(--fg)"><option value="">${a.pickChapter}</option>${chOpts}</select>
        <button id="go-ch" class="mt-3 rounded-lg border px-4 py-2 text-sm font-semibold" style="border-color:var(--border)">${a.start}</button></div>
      <div class="card p-5"><h2 class="font-bold">${a.practiceTopic}</h2>
        <select id="sel-tp" class="mt-3 w-full rounded-lg border px-3 py-2 text-sm" style="border-color:var(--border);background:var(--bg-soft);color:var(--fg)"><option value="">${a.pickLesson}</option>${lessonOpts}</select>
        <button id="go-tp" class="mt-3 rounded-lg border px-4 py-2 text-sm font-semibold" style="border-color:var(--border)">${a.start}</button></div>
    </div>
    ${getBm().some((x) => x.startsWith('q:')) ? `<div class="mt-4"><button id="go-bm" class="rounded-lg border px-4 py-2 text-sm font-semibold" style="border-color:var(--border)">★ ${a.reviewBookmarks}</button></div>` : ''}`;
  const $ = <T extends HTMLElement>(s: string) => root.querySelector(s) as T | null;
  $('#go-due')?.addEventListener('click', async () => start(await toItems(dueIds().slice(0, BATCH)), a.review));
  $('#go-ch')?.addEventListener('click', async () => { const v = ($('#sel-ch') as HTMLSelectElement).value; if (v) start(await chapterDeck(v), meta.chapters.find((c) => c.slug === v)!.title); });
  $('#go-tp')?.addEventListener('click', async () => { const v = ($('#sel-tp') as HTMLSelectElement).value; if (v) start(await topicDeck(v), flash[v].t); });
  $('#go-bm')?.addEventListener('click', async () => start(await toItems(shuffle(getBm().filter((x) => x.startsWith('q:')).map((x) => x.slice(2))).slice(0, 30)), '★'));
}

async function boot() {
  root.innerHTML = '';
  const p = new URLSearchParams(location.search);
  const meta = await loadMeta(L);
  if (p.get('chapter')) { const c = meta.chapters.find((x) => x.slug === p.get('chapter')); if (c) return start(await chapterDeck(c.slug), c.title); }
  if (p.get('topic')) { const t = p.get('topic')!; const f = (await loadFlash())[t]; return start(await topicDeck(t), f?.t ?? t); }
  if (p.has('bm')) return start(await toItems(shuffle(getBm().filter((x) => x.startsWith('q:')).map((x) => x.slice(2))).slice(0, 30)), '★');
  if (p.has('due')) return start(await toItems(dueIds().slice(0, BATCH)), a.review);
  setup();
}
boot().catch((e) => { root.innerHTML = `<p class="card p-6" style="color:#dc2626">${esc(String(e))}</p>`; });
