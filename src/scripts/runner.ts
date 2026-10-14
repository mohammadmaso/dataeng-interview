// A reusable "show question → reveal → self-grade" session runner used by Review and Mock interview.
import { grade, addSession, ui, lang, digits, esc, type Session } from './store';

export interface Item {
  id: string;            // SRS id: `<chapter>-q<n>` or `fc:<tid>:<i>`
  front: string;         // HTML (already safe)
  back: string;          // HTML (already safe)
  label?: string;        // small caption e.g. chapter title
  group?: string;        // for per-chapter stats
  href?: string;         // deep link to the source (shown for missed items)
  d?: 0 | 1 | 2;
}
export interface RunOpts {
  root: HTMLElement;
  items: Item[];
  kind: Session['kind'];
  label: string;
  secsPerItem?: number;  // 0/undefined = no timer
  srs?: boolean;         // write grades to the spaced-repetition schedule (default true)
  requeueMissed?: boolean;
  onExit?: () => void;   // "back" button
  onRetryMissed?: (items: Item[]) => void;
}

const fmt = (s: number) => `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;

export function runSession(o: RunOpts) {
  const a = ui().app; const dirAttr = 'dir="auto"';
  const queue = o.items.slice();
  const graded = new Set<string>();
  const result = new Map<string, 0 | 1 | 2>();
  const total = o.items.length;
  const started = Date.now();
  let revealed = false, timer: number | undefined, left = 0, timedOut = 0;
  const stop = () => { if (timer) clearInterval(timer); timer = undefined; };

  function key(e: KeyboardEvent) {
    if (!o.root.isConnected) { document.removeEventListener('keydown', key); return; }
    if (/INPUT|TEXTAREA|SELECT/.test((e.target as HTMLElement).tagName)) return;
    if (e.key === ' ' || e.key === 'Enter') { if (!revealed) { e.preventDefault(); reveal(); } }
    else if (revealed && ['1', '2', '3'].includes(e.key)) answer((+e.key - 1) as 0 | 1 | 2);
  }
  document.addEventListener('keydown', key);

  function render() {
    stop(); revealed = false;
    const it = queue[0];
    if (!it) return finish();
    const done = graded.size;
    o.root.innerHTML = `
      <div class="flex items-center justify-between gap-3 text-sm" style="color:var(--fg-muted)">
        <span>${digits(done + 1 > total ? total : done + 1)} ${a.ofN} ${digits(total)}${it.label ? ' · ' + esc(it.label) : ''}</span>
        <span id="run-timer" class="tabular-nums font-semibold"></span>
      </div>
      <div class="mt-2 h-1.5 overflow-hidden rounded-full" style="background:var(--bg-soft)"><div class="h-full rounded-full" style="background:var(--accent);width:${Math.round((done / total) * 100)}%"></div></div>
      <div class="card mt-4 p-5 sm:p-7">
        <div class="text-lg font-semibold leading-relaxed" ${dirAttr} style="text-align:start">${it.front}</div>
        <div id="run-back" class="prose max-w-none mt-5 hidden border-t pt-5 dark:prose-invert" ${dirAttr} style="border-color:var(--border);text-align:start">${it.back}</div>
      </div>
      <div class="mt-4" id="run-actions">
        <button id="run-reveal" class="w-full rounded-xl px-5 py-3 font-semibold text-white" style="background:var(--accent)">${a.showAnswer} <kbd class="ms-1 rounded border border-white/40 px-1 text-[10px]">Space</kbd></button>
      </div>
      <p class="mt-3 text-center text-xs" style="color:var(--fg-muted)">${a.gradeHint}${o.requeueMissed !== false ? ' · ' + a.missedLater : ''}</p>`;
    o.root.querySelector('#run-reveal')!.addEventListener('click', reveal);
    if (o.secsPerItem) {
      left = o.secsPerItem; tick();
      timer = window.setInterval(() => { left--; tick(); if (left <= 0) { stop(); timedOut++; reveal(true); } }, 1000);
    }
  }
  function tick() {
    const el = o.root.querySelector('#run-timer') as HTMLElement | null;
    if (el) { el.textContent = '⏱ ' + fmt(Math.max(0, left)); el.style.color = left <= 10 ? '#dc2626' : ''; }
  }
  function reveal(timeUp = false) {
    if (revealed) return; revealed = true; stop();
    o.root.querySelector('#run-back')!.classList.remove('hidden');
    const act = o.root.querySelector('#run-actions')!;
    act.innerHTML = `${timeUp ? `<p class="mb-2 text-center text-sm font-semibold" style="color:#dc2626">${a.mockTimeUp}</p>` : ''}
      <div class="grid grid-cols-3 gap-2">
        <button data-g="0" class="rounded-xl border-2 px-3 py-3 font-semibold" style="border-color:#dc2626;color:#dc2626">${a.gradeMissed}<span class="block text-[10px] opacity-70">1</span></button>
        <button data-g="1" class="rounded-xl border-2 px-3 py-3 font-semibold" style="border-color:#d97706;color:#d97706">${a.gradeUnsure}<span class="block text-[10px] opacity-70">2</span></button>
        <button data-g="2" class="rounded-xl border-2 px-3 py-3 font-semibold" style="border-color:var(--accent);color:var(--accent)">${a.gradeGot}<span class="block text-[10px] opacity-70">3</span></button>
      </div>`;
    act.querySelectorAll('button').forEach((b) => b.addEventListener('click', () => answer(+(b as HTMLElement).dataset.g! as 0 | 1 | 2)));
  }
  function answer(g: 0 | 1 | 2) {
    const it = queue.shift()!;
    if (!graded.has(it.id)) {
      graded.add(it.id); result.set(it.id, g);
      if (o.srs !== false) grade(it.id, g, it.group);
    }
    if (g === 0 && o.requeueMissed !== false && !(it as any)._again) queue.push({ ...it, _again: true } as Item);
    render();
  }
  function finish() {
    stop(); document.removeEventListener('keydown', key);
    const vals = [...result.values()];
    const got = vals.filter((v) => v === 2).length, unsure = vals.filter((v) => v === 1).length, missed = vals.filter((v) => v === 0).length;
    const secs = Math.round((Date.now() - started) / 1000);
    addSession({ kind: o.kind, total, correct: got, label: o.label, secs });
    const missedItems = o.items.filter((i) => result.get(i.id) === 0 || result.get(i.id) === 1);
    const byGroup = new Map<string, { n: number; ok: number }>();
    for (const it of o.items) { const g = it.label || it.group || '—'; const e = byGroup.get(g) ?? { n: 0, ok: 0 }; e.n++; if (result.get(it.id) === 2) e.ok++; byGroup.set(g, e); }
    const pct = total ? Math.round((got / total) * 100) : 0;
    o.root.innerHTML = `
      <div class="card p-6 text-center">
        <div class="text-sm font-semibold" style="color:var(--accent)">${a.sessionDone}</div>
        <div class="mt-2 text-5xl font-extrabold" style="color:var(--accent)">${digits(pct)}%</div>
        <p class="mt-2" style="color:var(--fg-muted)">${a.youGot} ${digits(got)} / ${digits(total)} · ${digits(unsure)} ${esc(a.gradeUnsure)} · ${digits(missed)} ${esc(a.gradeMissed)} · ⏱ ${fmt(secs)}</p>
        ${byGroup.size > 1 ? `<div class="mx-auto mt-5 grid max-w-md gap-1.5 text-start text-sm">${[...byGroup].map(([g, v]) => `<div class="flex items-center gap-2"><span class="w-40 truncate">${esc(g)}</span><span class="h-2 flex-1 overflow-hidden rounded-full" style="background:var(--bg-soft)"><span class="block h-full" style="width:${Math.round((v.ok / v.n) * 100)}%;background:var(--accent)"></span></span><span class="tabular-nums text-xs" style="color:var(--fg-muted)">${digits(v.ok)}/${digits(v.n)}</span></div>`).join('')}</div>` : ''}
        <div class="mt-6 flex flex-wrap justify-center gap-2">
          ${missedItems.length ? `<button id="run-retry" class="rounded-xl px-4 py-2 font-semibold text-white" style="background:var(--accent)">${a.reviewAgainMissed} (${digits(missedItems.length)})</button>` : ''}
          <button id="run-exit" class="rounded-xl border px-4 py-2 font-semibold" style="border-color:var(--border)">${a.backToPractice}</button>
        </div>
      </div>
      ${missedItems.length ? `<h3 class="mt-8 font-bold">${a.mockMissedList}</h3><ul class="mt-2 grid gap-1.5 text-sm">${missedItems.map((i) => `<li class="card p-3" dir="auto">${i.href ? `<a class="underline" href="${esc(i.href)}">${i.front}</a>` : i.front}</li>`).join('')}</ul>` : ''}`;
    o.root.querySelector('#run-retry')?.addEventListener('click', () => o.onRetryMissed?.(missedItems));
    o.root.querySelector('#run-exit')?.addEventListener('click', () => o.onExit?.());
    o.root.scrollIntoView({ block: 'start' });
  }
  render();
  return { stop };
}

export const L = lang;
