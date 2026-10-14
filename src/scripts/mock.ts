import { loadChapter, loadMeta, lang, ui, esc, digits, shuffle } from './store';
import { runSession, type Item } from './runner';

const root = document.getElementById('mock-app')!;
const L = lang(); const a = ui().app;
const PRESETS: [string, string[]][] = [
  [a.mockSqlPreset, ['sql']],
  [a.mockStreamPreset, ['kafka', 'flink', 'cdc']],
  [a.mockModelPreset, ['data-modeling', 'dwha']],
  [a.mockMixed, ['sql', 'python', 'spark', 'kafka', 'data-modeling', 'system-design']],
];

async function setup(prev?: { sel: string[]; level: string; count: string; secs: string }) {
  const meta = await loadMeta(L);
  const sel = new Set(prev?.sel ?? new URLSearchParams(location.search).get('chapters')?.split(',') ?? ['sql', 'python', 'spark']);
  const chip = (c: { slug: string; title: string; count: number }) =>
    `<button type="button" data-ch="${c.slug}" class="chip ch-chip" aria-pressed="${sel.has(c.slug)}">${esc(c.title)} · ${digits(c.count)}</button>`;
  const opt = (v: string, label: string, cur: string) => `<option value="${v}" ${v === cur ? 'selected' : ''}>${label}</option>`;
  root.innerHTML = `
    <div class="card p-5">
      <div class="flex flex-wrap items-center justify-between gap-2"><h2 class="font-bold">${a.mockTracks}</h2>
        <div class="flex flex-wrap gap-1.5 text-xs"><span style="color:var(--fg-muted)">${a.mockPreset}:</span>
          ${PRESETS.map(([n], i) => `<button type="button" data-preset="${i}" class="chip hover:border-[var(--accent)]">${n}</button>`).join('')}
          <button type="button" id="m-all" class="chip">${a.mockSelectAll}</button><button type="button" id="m-none" class="chip">${a.mockClear}</button></div></div>
      <div class="mt-3 flex flex-wrap gap-1.5" id="m-chips">${meta.chapters.map(chip).join('')}</div>
      <div class="mt-5 grid gap-4 sm:grid-cols-3">
        <label class="text-sm font-semibold">${a.mockCount}
          <select id="m-count" class="mt-1 w-full rounded-lg border px-3 py-2 font-normal" style="border-color:var(--border);background:var(--bg-soft);color:var(--fg)">${['5', '10', '15', '20', '30'].map((v) => opt(v, digits(v), prev?.count ?? '10')).join('')}</select></label>
        <label class="text-sm font-semibold">${a.mockTime}
          <select id="m-secs" class="mt-1 w-full rounded-lg border px-3 py-2 font-normal" style="border-color:var(--border);background:var(--bg-soft);color:var(--fg)">${opt('0', a.mockNoLimit, prev?.secs ?? '120')}${['60', '120', '180', '300'].map((v) => opt(v, `${digits(+v / 60)} ${a.mockMinutes}`, prev?.secs ?? '120')).join('')}</select></label>
        <label class="text-sm font-semibold">${a.mockLevel}
          <select id="m-level" class="mt-1 w-full rounded-lg border px-3 py-2 font-normal" style="border-color:var(--border);background:var(--bg-soft);color:var(--fg)">${opt('any', a.qAny, prev?.level ?? 'any')}${opt('0', a.qEasy, prev?.level ?? 'any')}${opt('1', a.qMid, prev?.level ?? 'any')}${opt('2', a.qHard, prev?.level ?? 'any')}</select></label>
      </div>
      <p class="mt-2 text-xs" style="color:var(--fg-muted)">${a.qEstimated}</p>
      <p id="m-err" class="mt-3 hidden text-sm font-semibold" style="color:#dc2626">${a.mockNeedChapter}</p>
      <button id="m-go" class="mt-4 rounded-xl px-6 py-3 font-semibold text-white" style="background:var(--accent)">${a.mockStart}</button>
    </div>`;
  const $ = (s: string) => root.querySelector(s) as HTMLElement;
  const paint = () => root.querySelectorAll<HTMLElement>('.ch-chip').forEach((b) => {
    const on = sel.has(b.dataset.ch!); b.setAttribute('aria-pressed', String(on)); b.classList.toggle('chip-P0', on);
  });
  paint();
  $('#m-chips').addEventListener('click', (e) => { const b = (e.target as HTMLElement).closest<HTMLElement>('.ch-chip'); if (!b) return; const s = b.dataset.ch!; sel.has(s) ? sel.delete(s) : sel.add(s); paint(); });
  root.querySelectorAll<HTMLElement>('[data-preset]').forEach((b) => b.addEventListener('click', () => { sel.clear(); PRESETS[+b.dataset.preset!][1].forEach((s) => sel.add(s)); paint(); }));
  $('#m-all').addEventListener('click', () => { meta.chapters.forEach((c) => sel.add(c.slug)); paint(); });
  $('#m-none').addEventListener('click', () => { sel.clear(); paint(); });
  $('#m-go').addEventListener('click', async () => {
    if (!sel.size) { $('#m-err').classList.remove('hidden'); return; }
    const cfg = { sel: [...sel], level: ($('#m-level') as HTMLSelectElement).value, count: ($('#m-count') as HTMLSelectElement).value, secs: ($('#m-secs') as HTMLSelectElement).value };
    const titles = new Map(meta.chapters.map((c) => [c.slug, c.title]));
    const pools = await Promise.all(cfg.sel.map(async (slug) => (await loadChapter(L, slug)).map((q) => ({ ...q, slug }))));
    let pool = pools.flat();
    if (cfg.level !== 'any') { const f = pool.filter((q) => q.d === +cfg.level); if (f.length >= 3) pool = f; }
    // round-robin across chapters so a big chapter does not dominate
    const by = new Map<string, typeof pool>(); shuffle(pool).forEach((q) => by.set(q.slug, [...(by.get(q.slug) ?? []), q]));
    const picked: typeof pool = []; const lists = [...by.values()];
    while (picked.length < +cfg.count && lists.some((l) => l.length)) for (const l of lists) { const q = l.pop(); if (q && picked.length < +cfg.count) picked.push(q); }
    const items: Item[] = shuffle(picked).map((q) => ({ id: q.id, front: esc(q.q), back: q.a, label: titles.get(q.slug), group: q.slug, href: `/${L}/questions/${q.slug}/#${q.id}`, d: q.d }));
    const run = (list: Item[]) => runSession({ root, items: list, kind: 'mock', label: a.mockTitle, secsPerItem: +cfg.secs || 0, requeueMissed: false, onExit: () => setup(cfg), onRetryMissed: run });
    run(items);
  });
}
setup().catch((e) => { root.innerHTML = `<p class="card p-6" style="color:#dc2626">${esc(String(e))}</p>`; });
