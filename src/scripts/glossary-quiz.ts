import { fetchJson, lang, ui, esc, digits, shuffle, addSession } from './store';

interface G { s: string; t: string; c: string; d: string }
const root = document.getElementById('gq-app')!;
const L = lang(); const a = ui().app;

async function setup(prev = { mode: 'dt', cat: '', n: '10' }) {
  const all = await fetchJson<G[]>(`/${L}/data/glossary.json`);
  const cats = [...new Set(all.map((g) => g.c))].sort();
  const sel = 'mt-1 w-full rounded-lg border px-3 py-2 font-normal';
  const st = 'border-color:var(--border);background:var(--bg-soft);color:var(--fg)';
  root.innerHTML = `<div class="card grid gap-4 p-5 sm:grid-cols-3">
    <label class="text-sm font-semibold">${a.gqMode}<select id="g-mode" class="${sel}" style="${st}"><option value="dt" ${prev.mode === 'dt' ? 'selected' : ''}>${a.gqDefToTerm}</option><option value="td" ${prev.mode === 'td' ? 'selected' : ''}>${a.gqTermToDef}</option></select></label>
    <label class="text-sm font-semibold">${a.gqCategory}<select id="g-cat" class="${sel}" style="${st}"><option value="">${a.qAny}</option>${cats.map((c) => `<option ${c === prev.cat ? 'selected' : ''}>${esc(c)}</option>`).join('')}</select></label>
    <label class="text-sm font-semibold">${a.gqRounds}<select id="g-n" class="${sel}" style="${st}">${['10', '20', '30'].map((v) => `<option value="${v}" ${v === prev.n ? 'selected' : ''}>${digits(v)}</option>`).join('')}</select></label>
    <div class="sm:col-span-3"><button id="g-go" class="rounded-xl px-6 py-3 font-semibold text-white" style="background:var(--accent)">${a.start}</button></div></div>`;
  (root.querySelector('#g-go') as HTMLElement).addEventListener('click', () => {
    const cfg = { mode: (root.querySelector('#g-mode') as HTMLSelectElement).value, cat: (root.querySelector('#g-cat') as HTMLSelectElement).value, n: (root.querySelector('#g-n') as HTMLSelectElement).value };
    const pool = cfg.cat ? all.filter((g) => g.c === cfg.cat) : all;
    play(all, pool, cfg);
  });
}

function play(all: G[], pool: G[], cfg: { mode: string; cat: string; n: string }) {
  const rounds = shuffle(pool).slice(0, Math.min(+cfg.n, pool.length));
  let i = 0, score = 0; const missed: G[] = [];
  const def2term = cfg.mode === 'dt';
  function distractors(g: G) {
    const same = shuffle(all.filter((x) => x.s !== g.s && x.c === g.c));
    const rest = shuffle(all.filter((x) => x.s !== g.s && x.c !== g.c));
    return [...same, ...rest].slice(0, 3);
  }
  function show() {
    if (i >= rounds.length) return end();
    const g = rounds[i]; const opts = shuffle([g, ...distractors(g)]);
    root.innerHTML = `
      <div class="flex justify-between text-sm" style="color:var(--fg-muted)"><span>${digits(i + 1)} ${a.ofN} ${digits(rounds.length)}</span><span>${a.gqScore}: ${digits(score)}</span></div>
      <div class="mt-2 h-1.5 overflow-hidden rounded-full" style="background:var(--bg-soft)"><div class="h-full" style="background:var(--accent);width:${Math.round((i / rounds.length) * 100)}%"></div></div>
      <div class="card mt-4 p-5"><div class="text-xs font-semibold uppercase" style="color:var(--fg-muted)">${esc(g.c)}</div>
        <div class="mt-2 text-lg font-semibold leading-relaxed" dir="auto">${esc(def2term ? g.d : g.t)}</div></div>
      <div class="mt-3 grid gap-2" id="g-opts">${opts.map((o) => `<button data-s="${o.s}" class="card w-full p-3 text-start hover:border-[var(--accent)]" dir="auto">${esc(def2term ? o.t : o.d)}</button>`).join('')}</div>
      <div id="g-fb" class="mt-3"></div>`;
    root.querySelectorAll<HTMLButtonElement>('#g-opts button').forEach((b) => b.addEventListener('click', () => {
      const ok = b.dataset.s === g.s; if (ok) score++; else missed.push(g);
      root.querySelectorAll<HTMLButtonElement>('#g-opts button').forEach((x) => {
        x.disabled = true;
        if (x.dataset.s === g.s) { x.style.borderColor = 'var(--accent)'; x.style.background = 'var(--accent-soft)'; }
        else if (x === b) { x.style.borderColor = '#dc2626'; x.style.background = 'color-mix(in srgb,#dc2626 10%,transparent)'; }
      });
      (root.querySelector('#g-fb') as HTMLElement).innerHTML = `<div class="flex flex-wrap items-center gap-3"><span class="font-bold" style="color:${ok ? 'var(--accent)' : '#dc2626'}">${ok ? a.gqCorrect : a.gqWrong}</span>
        <a class="text-sm underline" href="/${L}/glossary/${g.s}/" target="_blank" rel="noopener">${a.gqLearn}: ${esc(g.t)}</a>
        <button id="g-next" class="ms-auto rounded-xl px-5 py-2 font-semibold text-white" style="background:var(--accent)">${i + 1 >= rounds.length ? a.gqFinish : a.gqNext}</button></div>`;
      (root.querySelector('#g-next') as HTMLElement).addEventListener('click', () => { i++; show(); });
      (root.querySelector('#g-next') as HTMLElement).focus();
    }));
  }
  function end() {
    addSession({ kind: 'glossary', total: rounds.length, correct: score, label: cfg.cat || undefined });
    root.innerHTML = `<div class="card p-6 text-center"><div class="text-5xl font-extrabold" style="color:var(--accent)">${digits(Math.round((score / rounds.length) * 100))}%</div>
      <p class="mt-2" style="color:var(--fg-muted)">${digits(score)} / ${digits(rounds.length)}</p>
      <div class="mt-5 flex justify-center gap-2"><button id="g-again" class="rounded-xl px-5 py-2 font-semibold text-white" style="background:var(--accent)">${a.gqAgain}</button><button id="g-cfg" class="rounded-xl border px-5 py-2 font-semibold" style="border-color:var(--border)">${a.backToPractice}</button></div></div>
      ${missed.length ? `<h3 class="mt-6 font-bold">${a.gqMissedTerms}</h3><ul class="mt-2 grid gap-1.5 text-sm">${missed.map((g) => `<li class="card p-3"><a class="font-semibold underline" href="/${L}/glossary/${g.s}/">${esc(g.t)}</a> — <span style="color:var(--fg-muted)">${esc(g.d)}</span></li>`).join('')}</ul>` : ''}`;
    (root.querySelector('#g-again') as HTMLElement).addEventListener('click', () => play(all, pool, cfg));
    (root.querySelector('#g-cfg') as HTMLElement).addEventListener('click', () => setup(cfg));
  }
  show();
}
setup().catch((e) => { root.innerHTML = `<p class="card p-6" style="color:#dc2626">${esc(String(e))}</p>`; });
