import { getBm, toggleBm, chapterOf, loadChapter, loadMeta, fetchJson, lang, ui, esc } from './store';

const root = document.getElementById('bm-app')!;
const L = lang(); const a = ui().app;

async function render() {
  const bm = getBm();
  if (!bm.length) { root.innerHTML = `<p class="card p-8 text-center" style="color:var(--fg-muted)">${a.bmEmpty}</p>`; return; }
  const meta = await loadMeta(L);
  const topics = new Map(meta.topics.map((t) => [t.tid, t]));
  const titles = new Map(meta.chapters.map((c) => [c.slug, c.title]));
  const tids = bm.filter((x) => x.startsWith('t:')).map((x) => x.slice(2));
  const qids = bm.filter((x) => x.startsWith('q:')).map((x) => x.slice(2));
  const gids = bm.filter((x) => x.startsWith('g:')).map((x) => x.slice(2));
  const chs = [...new Set(qids.map(chapterOf))];
  const qmap = new Map<string, { q: string; a: string }>();
  await Promise.all(chs.map(async (c) => { try { (await loadChapter(L, c)).forEach((x) => qmap.set(x.id, x)); } catch { /* skip */ } }));
  const glossary = gids.length ? await fetchJson<{ s: string; t: string; d: string }[]>(`/${L}/data/glossary.json`) : [];
  const gmap = new Map(glossary.map((g) => [g.s, g]));
  const rm = (id: string) => `<button class="rounded-lg border px-2.5 py-1 text-xs" style="border-color:var(--border)" data-rm="${esc(id)}">✕ ${a.bmRemove}</button>`;
  const section = (title: string, body: string) => body ? `<section class="mt-8"><h2 class="mb-3 text-xl font-bold">${title}</h2><div class="grid gap-2">${body}</div></section>` : '';

  root.innerHTML =
    (qids.length ? `<a class="inline-block rounded-xl px-5 py-2.5 font-semibold text-white" style="background:var(--accent)" href="/${L}/review/?bm=1">${a.bmPractice} (${qids.length})</a>` : '') +
    section(a.bmLessons, tids.map((id) => { const t = topics.get(id); return t ? `<div class="card flex items-center justify-between gap-3 p-3"><a class="min-w-0 flex-1" href="/${L}/topics/${id}/"><div class="font-semibold">${esc(t.title)}</div><div class="truncate text-sm" style="color:var(--fg-muted)">${esc(t.summary)}</div></a>${rm('t:' + id)}</div>` : ''; }).join('')) +
    section(a.bmQuestions, qids.map((id) => { const q = qmap.get(id); if (!q) return ''; return `<details class="card p-3"><summary class="flex cursor-pointer list-none items-start justify-between gap-3"><span class="min-w-0 flex-1 font-medium" dir="auto"><span class="chip me-2">${esc(titles.get(chapterOf(id)) ?? '')}</span>${esc(q.q)}</span>${rm('q:' + id)}</summary><div class="prose mt-3 max-w-none border-t pt-3 dark:prose-invert" dir="auto" style="border-color:var(--border)">${q.a}</div><a class="mt-2 inline-block text-sm underline" href="/${L}/questions/${chapterOf(id)}/#${id}">↗</a></details>`; }).join('')) +
    section(a.bmTerms, gids.map((id) => { const g = gmap.get(id); return g ? `<div class="card flex items-center justify-between gap-3 p-3"><a class="min-w-0 flex-1" href="/${L}/glossary/${id}/"><div class="font-semibold">${esc(g.t)}</div><div class="text-sm" style="color:var(--fg-muted)">${esc(g.d)}</div></a>${rm('g:' + id)}</div>` : ''; }).join(''));
  root.querySelectorAll<HTMLElement>('[data-rm]').forEach((b) => b.addEventListener('click', (e) => { e.preventDefault(); e.stopPropagation(); toggleBm(b.dataset.rm!); render(); }));
}
render().catch((e) => { root.innerHTML = `<p class="card p-6" style="color:#dc2626">${esc(String(e))}</p>`; });
