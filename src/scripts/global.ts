// Site-wide behaviour: header badges, bookmarks, print, random picks, shortcuts, home dashboard, offline support.
import {
  hasBm, toggleBm, streak, srsStats, getGoal, doneToday, getLast, getSrs, chapterOf,
  loadMeta, loadChapter, fetchJson, lang, ui, digits, esc,
} from './store';

const L = lang(); const a = ui().app;

// ---------- header badges ----------
function badges() {
  const sg = streak(); const st = srsStats();
  const s = document.getElementById('hdr-streak');
  if (s) { s.textContent = `🔥 ${digits(sg.current)}`; s.title = `${sg.current} ${a.streakDays}`; s.classList.toggle('hidden', sg.current === 0); }
  const d = document.getElementById('hdr-due');
  if (d) { d.textContent = digits(st.due); d.style.display = st.due === 0 ? 'none' : 'grid'; d.title = `${st.due} ${a.cardsDue}`; }
}

// ---------- bookmarks ----------
function paintBm() {
  document.querySelectorAll<HTMLElement>('[data-bm]').forEach((b) => {
    const on = hasBm(b.dataset.bm!);
    b.setAttribute('aria-pressed', String(on));
    if (b.dataset.on) b.textContent = on ? b.dataset.on! : b.dataset.off!;
    b.style.color = on ? 'var(--accent)' : '';
    b.style.borderColor = on ? 'var(--accent)' : '';
  });
}
document.addEventListener('click', (e) => {
  const t = e.target as HTMLElement;
  const bm = t.closest<HTMLElement>('[data-bm]');
  if (bm) { e.preventDefault(); toggleBm(bm.dataset.bm!); paintBm(); return; }
  if (t.closest('[data-print]')) { window.print(); return; }
  const r = t.closest<HTMLElement>('[data-random]');
  if (r) { e.preventDefault(); randomPick(r.dataset.random!); }
});

async function randomPick(kind: string) {
  try {
    if (kind === 'term') {
      const g = await fetchJson<{ s: string }[]>(`/${L}/data/glossary.json`);
      location.href = `/${L}/glossary/${g[Math.floor(Math.random() * g.length)].s}/`;
    } else {
      const meta = await loadMeta(L);
      const total = meta.chapters.reduce((x, c) => x + c.count, 0); let n = Math.random() * total;
      const ch = meta.chapters.find((c) => (n -= c.count) < 0) ?? meta.chapters[0];
      const rows = await loadChapter(L, ch.slug);
      location.href = `/${L}/questions/${ch.slug}/#${rows[Math.floor(Math.random() * rows.length)].id}`;
    }
  } catch { /* offline and uncached: ignore */ }
}

// ---------- keyboard: Ctrl/Cmd+K opens search ----------
document.addEventListener('keydown', (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); (document.getElementById('open-search') as HTMLElement | null)?.click(); }
});

// ---------- home dashboard ----------
function dash() {
  const el = document.getElementById('home-dash'); if (!el) return;
  const sg = streak(); const st = srsStats(); const goal = getGoal(); const done = doneToday(); const last = getLast();
  const used = sg.current > 0 || st.total > 0 || !!last;
  if (!used) { el.classList.add('hidden'); return; }
  el.classList.remove('hidden');
  el.innerHTML = `
    <div class="flex flex-wrap items-center gap-x-8 gap-y-3">
      <div><div class="text-2xl font-extrabold" style="color:var(--accent)">🔥 ${digits(sg.current)}</div><div class="text-xs" style="color:var(--fg-muted)">${a.streakDays}</div></div>
      <div class="min-w-[10rem] flex-1"><div class="text-sm font-semibold">${a.todayGoal}: ${digits(Math.min(done, goal))} ${a.goalOf} ${digits(goal)}</div>
        <div class="mt-1.5 h-2 overflow-hidden rounded-full" style="background:var(--bg-soft)"><div class="h-full rounded-full" style="background:var(--accent);width:${Math.min(100, Math.round((done / goal) * 100))}%"></div></div></div>
      ${st.due ? `<a href="/${L}/review/?due=1" class="rounded-xl px-4 py-2 text-sm font-semibold text-white" style="background:var(--accent)">${a.startReview} · ${digits(st.due)} ${a.cardsDue}</a>` : `<span class="text-sm" style="color:var(--fg-muted)">${a.allCaughtUp}</span>`}
      ${last ? `<a href="/${last.lang}/topics/${last.tid}/" class="text-sm underline">${a.continueLearning}: <strong>${esc(last.title)}</strong></a>` : ''}
    </div>`;
}

// ---------- question index: per-chapter progress ----------
function qprog() {
  const els = document.querySelectorAll<HTMLElement>('[data-qprog]'); if (!els.length) return;
  const seen = new Map<string, number>();
  for (const id of Object.keys(getSrs())) if (!id.startsWith('fc:')) { const c = chapterOf(id); seen.set(c, (seen.get(c) ?? 0) + 1); }
  els.forEach((el) => {
    const [slug, total] = el.dataset.qprog!.split(':'); const n = seen.get(slug) ?? 0;
    const bar = el.querySelector<HTMLElement>('[data-bar]'); if (bar) bar.style.width = Math.min(100, Math.round((n / +total) * 100)) + '%';
    const lbl = el.querySelector<HTMLElement>('[data-lbl]'); if (lbl) lbl.textContent = n ? `${digits(n)}/${digits(total)}` : '';
  });
}

function all() { badges(); paintBm(); dash(); qprog(); }
document.addEventListener('de:change', all);
addEventListener('storage', all);
all();

// ---------- print: show every answer ----------
addEventListener('beforeprint', () => document.querySelectorAll<HTMLDetailsElement>('details:not([open])').forEach((d) => { d.open = true; d.dataset.wasClosed = '1'; }));
addEventListener('afterprint', () => document.querySelectorAll<HTMLDetailsElement>('details[data-was-closed]').forEach((d) => { d.open = false; delete d.dataset.wasClosed; }));

// ---------- offline support ----------
if (import.meta.env.PROD && 'serviceWorker' in navigator) {
  addEventListener('load', () => { navigator.serviceWorker.register('/sw.js').catch(() => { /* unsupported context */ }); });
}
