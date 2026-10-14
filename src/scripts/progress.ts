import {
  getSrs, srsStats, streak, getAct, getList, getQuiz, getSessions, getGoal, setGoal, getLast, today, dayToDate,
  exportAll, importAll, resetAll, loadMeta, lang, ui, esc, digits,
} from './store';

const root = document.getElementById('progress-app')!;
const L = lang(); const a = ui().app; const S = ui();

async function render() {
  const meta = await loadMeta(L);
  const read = new Set([...getList('read'), ...getList('known')]);
  const topics = meta.topics; const st = srsStats(); const sg = streak(); const act = getAct();
  const titles = new Map(meta.chapters.map((c) => [c.slug, c]));
  const tmap = new Map(topics.map((t) => [t.tid, t]));
  const done = topics.filter((t) => read.has(t.tid)).length;

  const tile = (v: string | number, l: string, sub = '') => `<div class="card p-4"><div class="text-3xl font-extrabold" style="color:var(--accent)">${v}</div><div class="text-sm font-semibold">${l}</div>${sub ? `<div class="text-xs" style="color:var(--fg-muted)">${sub}</div>` : ''}</div>`;

  // next lesson: first unread P0, else first unread
  const next = topics.find((t) => !read.has(t.tid) && t.priority === 'P0') ?? topics.find((t) => !read.has(t.tid));
  const last = getLast();

  // heatmap: 12 weeks ending today, columns = weeks
  const t0 = today(); const weekday = dayToDate(t0).getDay(); // 0 = Sunday
  const startDay = t0 - weekday - 11 * 7;
  let cells = '';
  for (let w = 0; w < 12; w++) {
    cells += '<div class="grid gap-1">';
    for (let d = 0; d < 7; d++) {
      const day = startDay + w * 7 + d; const n = act[String(day)] ?? 0;
      const lvl = day > t0 ? -1 : n === 0 ? 0 : n < 5 ? 1 : n < 15 ? 2 : n < 30 ? 3 : 4;
      const bg = lvl < 0 ? 'transparent' : lvl === 0 ? 'var(--bg-soft)' : `color-mix(in srgb, var(--accent) ${[0, 30, 55, 80, 100][lvl]}%, var(--bg-soft))`;
      cells += `<div class="h-3.5 w-3.5 rounded-[3px]" style="background:${bg}" title="${dayToDate(day).toLocaleDateString(L)}: ${n}"></div>`;
    }
    cells += '</div>';
  }

  // tracks
  const trackRows = meta.tracks.map((tr) => {
    const list = topics.filter((t) => t.track === tr.id); const n = list.filter((t) => read.has(t.tid)).length;
    const pct = list.length ? Math.round((n / list.length) * 100) : 0;
    return `<a href="/${L}/tracks/${tr.id}/" class="flex items-center gap-3 rounded-lg px-2 py-1.5 text-sm hover:bg-[var(--bg-soft)]"><span class="w-6 text-xs font-bold" style="color:var(--accent)">${digits(tr.id)}</span><span class="min-w-0 flex-1 truncate">${esc(tr.title)}</span><span class="h-2 w-28 shrink-0 overflow-hidden rounded-full" style="background:var(--bg-soft)"><span class="block h-full" style="width:${pct}%;background:var(--accent)"></span></span><span class="w-12 text-end text-xs tabular-nums" style="color:var(--fg-muted)">${digits(n)}/${digits(list.length)}</span></a>`;
  }).join('');

  // weak areas from SRS
  const groups = new Map<string, { reps: number; lapses: number }>();
  for (const c of Object.values(getSrs())) { if (!c.g) continue; const g = groups.get(c.g) ?? { reps: 0, lapses: 0 }; g.reps += c.r; g.lapses += c.l; groups.set(c.g, g); }
  const weak = [...groups].filter(([, v]) => v.reps >= 3).map(([g, v]) => ({ g, acc: Math.round((1 - v.lapses / v.reps) * 100), reps: v.reps }))
    .sort((x, y) => x.acc - y.acc || y.reps - x.reps).filter((w) => w.acc < 90).slice(0, 8);
  const weakHtml = weak.length ? weak.map((w) => {
    const isLesson = w.g.startsWith('lesson:'); const tid = w.g.slice(7);
    const ch = isLesson ? null : titles.get(w.g);
    const name = isLesson ? tmap.get(tid)?.title ?? tid : ch?.title ?? w.g;
    const sugg = ch ? ch.topics.map((x) => tmap.get(x)).find((t) => t && !read.has(t.tid)) ?? tmap.get(ch.topics[0]) : isLesson ? tmap.get(tid) : undefined;
    const practice = isLesson ? `/${L}/review/?topic=${tid}` : `/${L}/review/?chapter=${w.g}`;
    return `<div class="card flex flex-wrap items-center gap-3 p-3 text-sm"><div class="min-w-0 flex-1"><div class="font-semibold">${esc(name)}</div><div class="text-xs" style="color:var(--fg-muted)">${digits(w.acc)}% ${a.pgrAccuracy} · ${digits(w.reps)} ${a.pgrAttempts}${sugg ? ` · ${a.pgrSuggested}: <a class="underline" href="/${L}/topics/${sugg.tid}/">${esc(sugg.title)}</a>` : ''}</div></div><div class="h-2 w-24 overflow-hidden rounded-full" style="background:var(--bg-soft)"><div class="h-full" style="width:${w.acc}%;background:${w.acc < 60 ? '#dc2626' : '#d97706'}"></div></div><a class="rounded-lg border px-3 py-1.5 font-semibold" style="border-color:var(--border)" href="${practice}">${a.pgrPracticeNow}</a></div>`;
  }).join('') : `<p class="card p-4 text-sm" style="color:var(--fg-muted)">${a.pgrNoWeak}</p>`;

  const quiz = Object.entries(getQuiz()).sort((x, y) => y[1].t - x[1].t).slice(0, 8);
  const sessions = getSessions().slice(-8).reverse();
  const kinds = a.sessionKind as Record<string, string>;

  root.innerHTML = `
    <div class="grid grid-cols-2 gap-3 md:grid-cols-4">
      ${tile(digits(sg.current), a.pgrStreak, S.app.streakDays)}${tile(digits(sg.best), a.pgrBest)}
      ${tile(`${digits(done)}/${digits(topics.length)}`, a.pgrLessons)}${tile(digits(st.total), a.pgrCards, `${digits(st.due)} ${a.dueNow}`)}
    </div>
    <div class="mt-5 grid gap-4 md:grid-cols-2">
      <div class="card p-5"><div class="text-xs font-semibold uppercase" style="color:var(--accent)">${a.pgrNextLesson}</div>
        ${next ? `<a class="mt-1 block text-lg font-bold hover:underline" href="/${L}/topics/${next.tid}/">${esc(next.title)}</a><p class="mt-1 text-sm" style="color:var(--fg-muted)">${esc(next.summary)}</p>` : '<p>🎉</p>'}
        ${last ? `<div class="mt-3 text-sm"><span style="color:var(--fg-muted)">${a.continueLearning}:</span> <a class="underline" href="/${last.lang}/topics/${last.tid}/">${esc(last.title)}</a></div>` : ''}
        ${st.due ? `<a class="mt-4 inline-block rounded-xl px-4 py-2 text-sm font-semibold text-white" style="background:var(--accent)" href="/${L}/review/?due=1">${a.startReview} (${digits(st.due)})</a>` : ''}</div>
      <div class="card p-5"><div class="flex items-center justify-between"><div class="text-xs font-semibold uppercase" style="color:var(--accent)">${a.pgrActivity}</div>
        <label class="text-xs" style="color:var(--fg-muted)">${a.pgrSetGoal} <input id="goal" type="number" min="1" max="200" value="${getGoal()}" class="ms-1 w-16 rounded border px-1.5 py-0.5 text-center" style="border-color:var(--border);background:var(--bg-soft);color:var(--fg)"></label></div>
        <div class="mt-3 flex justify-center gap-1" dir="ltr">${cells}</div></div>
    </div>
    <section class="mt-8"><h2 class="mb-2 text-xl font-bold">${a.pgrWeak}</h2><p class="mb-3 text-sm" style="color:var(--fg-muted)">${a.pgrWeakSub}</p><div class="grid gap-2">${weakHtml}</div></section>
    <section class="mt-8"><h2 class="mb-2 text-xl font-bold">${a.pgrTracks}</h2><div class="card p-2">${trackRows}</div></section>
    <div class="mt-8 grid gap-6 md:grid-cols-2">
      <section><h2 class="mb-2 text-xl font-bold">${a.pgrQuizzes}</h2><div class="grid gap-1.5 text-sm">${quiz.length ? quiz.map(([tid, r]) => `<a class="card flex justify-between p-3 hover:border-[var(--accent)]" href="/${L}/topics/${tid}/"><span class="truncate">${esc(tmap.get(tid)?.title ?? tid)}</span><span class="font-bold tabular-nums" style="color:var(--accent)">${digits(r.s)}/${digits(r.n)}</span></a>`).join('') : `<p style="color:var(--fg-muted)">${a.pgrNoSessions}</p>`}</div></section>
      <section><h2 class="mb-2 text-xl font-bold">${a.pgrRecent}</h2><div class="grid gap-1.5 text-sm">${sessions.length ? sessions.map((s) => `<div class="card flex justify-between gap-2 p-3"><span class="truncate">${esc(kinds[s.kind] ?? s.kind)}${s.label ? ' · ' + esc(s.label) : ''}</span><span class="shrink-0 tabular-nums" style="color:var(--fg-muted)">${digits(s.correct)}/${digits(s.total)} · ${new Date(s.ts).toLocaleDateString(L)}</span></div>`).join('') : `<p style="color:var(--fg-muted)">${a.pgrNoSessions}</p>`}</div></section>
    </div>
    <section class="mt-10 card p-5"><h2 class="text-xl font-bold">${a.pgrBackup}</h2><p class="mt-1 text-sm" style="color:var(--fg-muted)">${a.pgrBackupSub}</p>
      <div class="mt-3 flex flex-wrap gap-2">
        <button id="b-exp" class="rounded-lg border px-4 py-2 text-sm font-semibold" style="border-color:var(--border)">⬇ ${a.pgrExport}</button>
        <label class="cursor-pointer rounded-lg border px-4 py-2 text-sm font-semibold" style="border-color:var(--border)">⬆ ${a.pgrImport}<input id="b-imp" type="file" accept="application/json,.json" class="hidden"></label>
        <button id="b-rst" class="rounded-lg border px-4 py-2 text-sm font-semibold" style="border-color:#dc2626;color:#dc2626">${a.pgrReset}</button>
        <span id="b-msg" class="self-center text-sm font-semibold"></span></div></section>`;

  const $ = (s: string) => root.querySelector(s) as HTMLElement;
  ($('#goal') as HTMLInputElement).addEventListener('change', (e) => setGoal(Math.max(1, Math.min(200, +(e.target as HTMLInputElement).value || 10))));
  $('#b-exp').addEventListener('click', () => {
    const url = URL.createObjectURL(new Blob([exportAll()], { type: 'application/json' }));
    const el = document.createElement('a'); el.href = url; el.download = `de-interview-progress-${new Date().toISOString().slice(0, 10)}.json`; el.click(); URL.revokeObjectURL(url);
  });
  $('#b-imp').addEventListener('change', async (e) => {
    const f = (e.target as HTMLInputElement).files?.[0]; if (!f) return;
    try { const n = importAll(await f.text()); render().then(() => { const m = document.getElementById('b-msg'); if (m) m.textContent = `${a.pgrImported} (${n})`; }); }
    catch { $('#b-msg').textContent = a.pgrBadFile; $('#b-msg').style.color = '#dc2626'; }
  });
  $('#b-rst').addEventListener('click', () => { if (confirm(a.pgrConfirmReset)) { resetAll(); render(); } });
}
render().catch((e) => { root.innerHTML = `<p class="card p-6" style="color:#dc2626">${esc(String(e))}</p>`; });
