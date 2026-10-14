import { loadMeta, lang, ui, esc, digits, today, addSession, bump, type Meta } from './store';

const root = document.getElementById('drill-app')!;
const L = lang(); const a = ui().app as Record<string, any>;
const phases: [string, string][] = a.phases;
const noteKey = (tid: string) => `de:drill:${tid}`;
const rd = (k: string) => { try { return localStorage.getItem(k) ?? ''; } catch { return ''; } };
const wr = (k: string, v: string) => { try { localStorage.setItem(k, v); } catch { /* ignore */ } };
const fmt = (s: number) => `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;

let problems: Meta['topics'] = [];

function pick(tid?: string) {
  const dayIdx = today() % problems.length;
  const p = problems.find((x) => x.tid === tid) ?? problems[dayIdx];
  return p;
}

function chooser(cur: Meta['topics'][number]) {
  const todayP = pick();
  root.innerHTML = `
    <div class="card p-5">
      <div class="text-xs font-semibold uppercase" style="color:var(--accent)">${cur.tid === todayP.tid ? a.drillToday : a.drillPick}</div>
      <h2 class="mt-1 text-xl font-bold">${esc(cur.title)}</h2>
      <p class="mt-1 text-sm" style="color:var(--fg-muted)">${esc(cur.summary)}</p>
      <div class="mt-4 flex flex-wrap items-end gap-3">
        <label class="text-sm font-semibold">${a.drillMinutes}
          <select id="d-min" class="mt-1 block rounded-lg border px-3 py-2 font-normal" style="border-color:var(--border);background:var(--bg-soft);color:var(--fg)">${[30, 45, 60].map((m) => `<option value="${m}" ${m === 45 ? 'selected' : ''}>${digits(m)}</option>`).join('')}</select></label>
        <button id="d-go" class="rounded-xl px-6 py-2.5 font-semibold text-white" style="background:var(--accent)">${a.drillStart}</button>
        <button id="d-rand" class="rounded-xl border px-4 py-2.5 font-semibold" style="border-color:var(--border)">🎲 ${a.drillRandom}</button>
      </div>
      <label class="mt-5 block text-sm font-semibold">${a.drillPick}
        <select id="d-pick" class="mt-1 block w-full rounded-lg border px-3 py-2 font-normal" style="border-color:var(--border);background:var(--bg-soft);color:var(--fg)">${problems.map((p) => `<option value="${p.tid}" ${p.tid === cur.tid ? 'selected' : ''}>${esc(p.title)}</option>`).join('')}</select></label>
    </div>
    <div class="mt-6 card p-5"><h3 class="font-bold">${a.drillPhases}</h3>
      <ol class="mt-3 grid gap-2 text-sm">${phases.map(([t, d], i) => `<li class="flex gap-3"><span class="grid h-6 w-6 shrink-0 place-items-center rounded-full text-xs font-bold" style="background:var(--accent-soft);color:var(--accent)">${digits(i + 1)}</span><span><strong>${t}</strong><br><span style="color:var(--fg-muted)">${d}</span></span></li>`).join('')}</ol></div>`;
  const $ = (s: string) => root.querySelector(s) as HTMLElement;
  $('#d-pick').addEventListener('change', (e) => chooser(pick((e.target as HTMLSelectElement).value)));
  $('#d-rand').addEventListener('click', () => chooser(problems[Math.floor(Math.random() * problems.length)]));
  $('#d-go').addEventListener('click', () => run(cur, +($('#d-min') as HTMLSelectElement).value * 60));
}

function run(p: Meta['topics'][number], totalSecs: number) {
  let left = totalSecs, paused = false, over = false; const t0 = Date.now();
  // phase budget: proportional minutes
  const weights = [10, 8, 12, 25, 25, 12, 8]; const wsum = weights.reduce((x, y) => x + y, 0);
  root.innerHTML = `
    <div class="sticky top-14 z-20 -mx-4 flex items-center justify-between gap-3 border-b px-4 py-3 backdrop-blur" style="border-color:var(--border);background:color-mix(in srgb,var(--bg) 88%,transparent)">
      <div class="min-w-0"><div class="truncate font-bold">${esc(p.title)}</div><div id="d-over" class="hidden text-xs font-semibold" style="color:#dc2626">${a.drillOver}</div></div>
      <div class="flex items-center gap-2"><span id="d-time" class="text-2xl font-extrabold tabular-nums" dir="ltr">${fmt(left)}</span>
        <button id="d-pause" class="rounded-lg border px-3 py-1.5 text-sm font-semibold" style="border-color:var(--border)">${a.drillPause}</button>
        <button id="d-fin" class="rounded-lg px-3 py-1.5 text-sm font-semibold text-white" style="background:var(--accent)">${a.drillFinish}</button></div>
    </div>
    <div class="mt-5 grid gap-5 md:grid-cols-5">
      <ol class="grid content-start gap-2 md:col-span-2">${phases.map(([t, d], i) => `
        <li><label class="card flex cursor-pointer gap-3 p-3 text-sm"><input type="checkbox" class="mt-1 h-4 w-4 accent-[var(--accent)]" data-ph="${i}">
          <span><strong>${digits(i + 1)}. ${t}</strong> <span class="chip ms-1">${digits(Math.round((totalSecs / 60) * weights[i] / wsum))} ${a.mockMinutes}</span><br><span style="color:var(--fg-muted)">${d}</span></span></label></li>`).join('')}</ol>
      <div class="md:col-span-3"><label class="text-sm font-semibold">${a.drillNotes}</label>
        <textarea id="d-notes" rows="18" class="mt-1 w-full rounded-xl border p-3 font-mono text-sm" dir="auto" style="border-color:var(--border);background:var(--bg-soft);color:var(--fg)" placeholder="${esc(a.drillNotesPh)}">${esc(rd(noteKey(p.tid)))}</textarea></div>
    </div>`;
  const $ = (s: string) => root.querySelector(s) as HTMLElement;
  const notes = $('#d-notes') as HTMLTextAreaElement;
  notes.addEventListener('input', () => wr(noteKey(p.tid), notes.value));
  const tick = window.setInterval(() => {
    if (paused) return; left--; $('#d-time').textContent = fmt(Math.max(0, left));
    if (left <= 0 && !over) { over = true; $('#d-over').classList.remove('hidden'); $('#d-time').style.color = '#dc2626'; }
    if (left < -3600) clearInterval(tick);
  }, 1000);
  $('#d-pause').addEventListener('click', (e) => { paused = !paused; (e.target as HTMLElement).textContent = paused ? a.drillResume : a.drillPause; });
  $('#d-fin').addEventListener('click', () => { clearInterval(tick); check(p, Math.round((Date.now() - t0) / 1000)); });
}

function check(p: Meta['topics'][number], secs: number) {
  const items = [...p.covers, ...(a.drillGeneric ?? [])];
  root.innerHTML = `
    <div class="card p-5"><h2 class="text-xl font-bold">${a.drillCoverage}</h2>
      <p class="mt-1 text-sm" style="color:var(--fg-muted)">${a.selfCheckDesign} · ⏱ ${fmt(secs)}</p>
      <ul class="mt-4 grid gap-2">${items.map((c, i) => `<li><label class="flex cursor-pointer gap-3 rounded-lg border p-3 text-sm" style="border-color:var(--border)"><input type="checkbox" class="mt-0.5 h-4 w-4 accent-[var(--accent)]" data-c="${i}"><span dir="auto">${esc(c)}</span></label></li>`).join('')}</ul>
      <div class="mt-4 flex items-center gap-3"><div class="h-2 flex-1 overflow-hidden rounded-full" style="background:var(--bg-soft)"><div id="d-bar" class="h-full rounded-full" style="background:var(--accent);width:0"></div></div><span id="d-score" class="text-sm font-bold tabular-nums"></span></div>
      <div class="mt-5 flex flex-wrap gap-2">
        <a class="rounded-xl px-5 py-2.5 font-semibold text-white" style="background:var(--accent)" href="/${L}/topics/${p.tid}/">${a.drillSolution}</a>
        <button id="d-save" class="rounded-xl border px-5 py-2.5 font-semibold" style="border-color:var(--border)">${a.drillAgain}</button>
      </div>
    </div>`;
  const boxes = [...root.querySelectorAll<HTMLInputElement>('input[data-c]')];
  const upd = () => { const n = boxes.filter((b) => b.checked).length; (root.querySelector('#d-bar') as HTMLElement).style.width = Math.round((n / boxes.length) * 100) + '%'; (root.querySelector('#d-score') as HTMLElement).textContent = `${a.drillScoreLine} ${digits(n)} / ${digits(boxes.length)}`; };
  boxes.forEach((b) => b.addEventListener('change', upd)); upd();
  root.querySelector('#d-save')!.addEventListener('click', () => {
    addSession({ kind: 'drill', total: boxes.length, correct: boxes.filter((b) => b.checked).length, label: p.title, secs });
    bump(5); chooser(problems[Math.floor(Math.random() * problems.length)]);
  });
  // record even if the learner just navigates away to the solution
  root.querySelector('a')!.addEventListener('click', () => addSession({ kind: 'drill', total: boxes.length, correct: boxes.filter((b) => b.checked).length, label: p.title, secs }));
}

loadMeta(L).then((m) => {
  problems = m.topics.filter((t) => t.type === 'problem' && t.tid.startsWith('design-'));
  const q = new URLSearchParams(location.search).get('problem');
  chooser(pick(q ?? undefined));
}).catch((e) => { root.innerHTML = `<p class="card p-6" style="color:#dc2626">${esc(String(e))}</p>`; });
