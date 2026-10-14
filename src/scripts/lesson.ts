// Turns the static lesson markdown into an interactive study page:
// clickable quizzes, flip-card flashcards wired to spaced repetition, runnable SQL, self-check lists.
import {
  grade, enroll, setQuiz, setLast, bump, getChecks, setChecks, getList, toggleList, ui, esc, digits, lang,
} from './store';
import { openDb, resultsHtml, looksRunnable } from './sqlrun';

const main = document.querySelector<HTMLElement>('main[data-lesson]');
const article = document.querySelector<HTMLElement>('article.prose');
const S = ui(); const a = S.app; const L = lang();
const tid = main?.dataset.lesson ?? '';

// ---------- last visited + activity ----------
if (tid) {
  setLast({ tid, title: document.querySelector('h1')?.textContent?.trim() ?? tid, lang: L });
}

// ---------- reading progress bar ----------
const bar = document.getElementById('read-bar');
if (bar) {
  const upd = () => {
    const h = document.documentElement; const max = h.scrollHeight - h.clientHeight;
    bar.style.width = (max > 0 ? Math.min(100, (h.scrollTop / max) * 100) : 0) + '%';
  };
  addEventListener('scroll', upd, { passive: true }); upd();
}

// ---------- self-check checklist ----------
document.querySelectorAll<HTMLElement>('[data-selfcheck]').forEach((box) => {
  const key = 'sc:' + box.dataset.selfcheck;
  const boxes = [...box.querySelectorAll<HTMLInputElement>('input[type=checkbox]')];
  const saved = new Set(getChecks()[key] ?? []);
  const cnt = box.querySelector<HTMLElement>('[data-count]');
  const upd = () => { if (cnt) cnt.textContent = `${digits(boxes.filter((b) => b.checked).length)}/${digits(boxes.length)} ${a.recalled}`; };
  boxes.forEach((b, i) => { b.checked = saved.has(i); b.addEventListener('change', () => { setChecks(key, boxes.flatMap((x, j) => (x.checked ? [j] : []))); upd(); }); });
  upd();
});

// ---------- mark read & continue ----------
document.querySelectorAll<HTMLAnchorElement>('[data-markread-next]').forEach((el) => el.addEventListener('click', () => {
  if (tid && !getList('read').includes(tid)) toggleList('read', tid);
  // the inline progress painter listens to storage writes on the next paint
}));

if (article) {
  // ---------- headings: section anchors that copy their link ----------
  article.querySelectorAll<HTMLElement>('h2[id], h3[id]').forEach((h) => {
    const b = document.createElement('button');
    b.type = 'button'; b.className = 'anchor-btn'; b.textContent = '#'; b.setAttribute('aria-label', 'Copy link');
    b.addEventListener('click', async () => {
      const url = location.origin + location.pathname + '#' + h.id;
      try { await navigator.clipboard.writeText(url); b.textContent = '✓'; setTimeout(() => (b.textContent = '#'), 1200); } catch { location.hash = h.id; }
    });
    h.appendChild(b);
  });

  enhanceQuiz(article);
  enhanceFlashcards(article);
  enhanceSql(article);
}

// =====================================================================
function sectionAfter(root: HTMLElement, test: RegExp): HTMLElement[] {
  const h = [...root.querySelectorAll<HTMLElement>('h2')].find((x) => test.test(x.textContent ?? ''));
  const out: HTMLElement[] = [];
  for (let el = h?.nextElementSibling as HTMLElement | null; el && el.tagName !== 'H2'; el = el.nextElementSibling as HTMLElement | null) out.push(el);
  if (h) (out as any).heading = h;
  return out;
}

function enhanceQuiz(root: HTMLElement) {
  const sec = sectionAfter(root, /✅/);
  const heading = (sec as any).heading as HTMLElement | undefined;
  if (!heading) return;
  interface Q { li: HTMLElement; opts: HTMLElement[]; letters: string[]; ans: string; details: HTMLElement | null }
  const qs: Q[] = [];
  let lastLi: HTMLElement | null = null;
  const attach = (d: HTMLElement) => {
    const target = qs.find((q) => q.li === lastLi) ?? qs[qs.length - 1];
    if (target && !target.details) target.details = d;
  };
  const addLi = (li: HTMLElement) => {
    lastLi = li;
    const optLis = [...li.querySelectorAll<HTMLElement>(':scope > ul > li')];
    const opts = optLis.filter((o) => /^\s*[A-D]\)/.test(o.textContent ?? ''));
    if (opts.length >= 2) qs.push({ li, opts, letters: opts.map((o) => (o.textContent ?? '').trim()[0]), ans: '', details: null });
    const inner = li.querySelector<HTMLElement>(':scope > details');
    if (inner) attach(inner);
  };
  for (const el of sec) {
    if (el.tagName === 'OL') el.querySelectorAll<HTMLElement>(':scope > li').forEach(addLi);
    else if (el.tagName === 'DETAILS') attach(el);
  }
  const usable = qs.filter((q) => {
    const first = q.details?.querySelector('strong')?.textContent?.trim() ?? '';
    const m = first.match(/^([A-D])\b/);
    if (!m || !q.letters.includes(m[1])) return false;
    q.ans = m[1]; return true;
  });
  if (usable.length < 2) return;

  let answered = 0, correct = 0;
  const banner = document.createElement('div');
  banner.className = 'quiz-banner';
  const paintBanner = () => {
    banner.innerHTML = `<span><strong>${a.quizScore}:</strong> <span class="tabular-nums">${digits(correct)}/${digits(usable.length)}</span>${answered === usable.length ? ' · ' + a.quizAll : ''}</span><button type="button" class="quiz-reset" ${answered ? '' : 'hidden'}>${a.quizTryAgain}</button>`;
    banner.querySelector('.quiz-reset')?.addEventListener('click', reset);
  };
  heading.after(banner);

  const states = new Map<Q, boolean>();
  function reset() {
    answered = 0; correct = 0; states.clear();
    for (const q of usable) {
      q.opts.forEach((o) => { o.classList.remove('opt-ok', 'opt-bad', 'opt-dim'); o.removeAttribute('aria-disabled'); });
      q.li.querySelectorAll('.quiz-fb').forEach((x) => x.remove());
      if (q.details) q.details.open = false;
    }
    paintBanner();
  }
  for (const q of usable) {
    q.li.classList.add('quiz-q');
    q.opts.forEach((o, i) => {
      o.classList.add('quiz-opt'); o.tabIndex = 0; o.setAttribute('role', 'button');
      const pick = () => {
        if (states.has(q)) return;
        const ok = q.letters[i] === q.ans; states.set(q, ok); answered++; if (ok) correct++;
        q.opts.forEach((x, j) => { if (q.letters[j] === q.ans) x.classList.add('opt-ok'); else if (x === o) x.classList.add('opt-bad'); else x.classList.add('opt-dim'); x.setAttribute('aria-disabled', 'true'); });
        const fb = document.createElement('div'); fb.className = 'quiz-fb ' + (ok ? 'ok' : 'bad'); fb.textContent = ok ? a.quizCorrect : a.quizWrong;
        q.opts[0].parentElement!.after(fb);
        if (q.details) q.details.open = true;
        paintBanner();
        if (answered === usable.length) { setQuiz(tid, correct, usable.length); }
      };
      o.addEventListener('click', pick);
      o.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); pick(); } });
    });
  }
  paintBanner();
}

function enhanceFlashcards(root: HTMLElement) {
  const sec = sectionAfter(root, /🗂/);
  const heading = (sec as any).heading as HTMLElement | undefined;
  const table = sec.find((e) => e.tagName === 'TABLE') as HTMLTableElement | undefined;
  if (!heading || !table || !tid) return;
  const rows = [...table.querySelectorAll('tbody tr')].map((tr) => [...tr.children].map((c) => c.innerHTML));
  if (!rows.length) return;
  const ids = rows.map((_, i) => `fc:${tid}:${i}`);
  table.classList.add('fc-table'); table.closest('.table-wrap')?.classList.add('fc-table');

  const wrap = document.createElement('div');
  wrap.className = 'fc-deck';
  let idx = 0, flipped = false; const done = new Map<number, number>();
  const paint = () => {
    const [front, back] = rows[idx];
    wrap.innerHTML = `
      <div class="fc-meta"><span>${a.flashDeck} · ${digits(idx + 1)} ${a.ofN} ${digits(rows.length)}</span>
        <span class="fc-actions"><button type="button" data-act="shuffle" title="shuffle">🔀</button><button type="button" data-act="enroll">${a.addToReview}</button></span></div>
      <button type="button" class="fc-card ${flipped ? 'flipped' : ''}" aria-label="${a.flipCard}" data-act="flip">
        <span class="fc-face fc-front" dir="auto">${front}</span><span class="fc-face fc-back" dir="auto">${back}</span></button>
      <p class="fc-hint">${flipped ? '' : a.flipHint}</p>
      <div class="fc-nav">
        <button type="button" data-act="prev" aria-label="${S.prev}">←</button>
        ${flipped ? `<button type="button" data-g="0" class="g0">${a.gradeMissed}</button><button type="button" data-g="1" class="g1">${a.gradeUnsure}</button><button type="button" data-g="2" class="g2">${a.gradeGot}</button>` : `<button type="button" data-act="flip" class="fc-flip">${a.flipCard}</button>`}
        <button type="button" data-act="next" aria-label="${S.next}">→</button></div>
      <div class="fc-dots">${rows.map((_, i) => `<i class="${i === idx ? 'cur' : ''} ${done.has(i) ? 'g' + done.get(i) : ''}"></i>`).join('')}</div>`;
  };
  wrap.addEventListener('click', (e) => {
    const t = (e.target as HTMLElement).closest<HTMLElement>('[data-act],[data-g]'); if (!t) return;
    if (t.dataset.g !== undefined) {
      const g = +t.dataset.g as 0 | 1 | 2; grade(ids[idx], g, 'lesson:' + tid); done.set(idx, g);
      idx = Math.min(rows.length - 1, idx + 1); flipped = false; paint(); return;
    }
    switch (t.dataset.act) {
      case 'flip': flipped = !flipped; break;
      case 'next': idx = (idx + 1) % rows.length; flipped = false; break;
      case 'prev': idx = (idx - 1 + rows.length) % rows.length; flipped = false; break;
      case 'shuffle': { const o = rows.map((r, i) => [r, i] as const).sort(() => Math.random() - 0.5); rows.splice(0, rows.length, ...o.map((x) => x[0])); ids.splice(0, ids.length, ...o.map((x) => `fc:${tid}:${x[1]}`)); idx = 0; flipped = false; done.clear(); break; }
      case 'enroll': { const n = enroll(ids, 'lesson:' + tid); t.textContent = '✓ ' + a.addedToReview; (t as HTMLButtonElement).disabled = true; void n; return; }
    }
    paint();
  });
  paint();
  table.before(wrap);
}

function enhanceSql(root: HTMLElement) {
  const pres = [...root.querySelectorAll<HTMLElement>('pre[data-language="sql"]')];
  const blocks = pres.map((pre) => ({ pre, code: (pre.textContent ?? '').trim() })).filter((b) => looksRunnable(b.code));
  if (!blocks.length) return;
  const isSetup = (c: string) => {
    const t = c.replace(/--[^\n]*/g, '');
    const rest = t.replace(/insert\s+into[\s\S]*?;/gi, '').replace(/create\s+table[\s\S]*?;/gi, '');
    return /^\s*(create|insert)/i.test(t) && !/\b(select|with)\b/i.test(rest);
  };
  const fix = (c: string) => c.replace(/create\s+table\s+(if\s+not\s+exists\s+)?([\w"]+)/gi, 'DROP TABLE IF EXISTS $2; CREATE TABLE $2');
  const setups = blocks.filter((b) => isSetup(b.code)).map((b) => fix(b.code));

  for (const b of blocks) {
    const bar = document.createElement('div'); bar.className = 'sql-bar';
    bar.innerHTML = `<button type="button" class="sql-run">${a.runSql}</button><span class="sql-note">${a.runDialect}</span><a class="sql-pg" href="/${L}/playground/#q=${enc(b.code)}">${a.openPlayground} ↗</a>`;
    const res = document.createElement('div'); res.className = 'sql-out';
    b.pre.after(bar); bar.after(res);
    bar.querySelector('.sql-run')!.addEventListener('click', async (ev) => {
      const btn = ev.currentTarget as HTMLButtonElement; btn.disabled = true; res.innerHTML = `<p class="text-sm" style="color:var(--fg-muted)">${a.pgLoading}</p>`;
      let db: Awaited<ReturnType<typeof openDb>> | null = null;
      try {
        db = await openDb();
        for (const s of setups) { try { db.exec(s); } catch { /* a setup block from another dialect: skip */ } }
        const own = isSetup(b.code);
        if (own) { db.exec(fix(b.code)); res.innerHTML = `<div class="sql-ok">✓ ${db.tables().map((t) => esc(t.name)).join(', ')}</div>`; }
        else {
          const sets = db.exec(fix(b.code));
          res.innerHTML = resultsHtml(sets, { rows: a.pgRows, none: a.pgNoRows, truncated: a.pgTruncated }, 200);
        }
      } catch (e) {
        res.innerHTML = `<pre class="sql-err" dir="ltr">${a.pgError}: ${esc((e as Error).message)}</pre>`;
      } finally { db?.close(); btn.disabled = false; bump(1); }
    });
  }
}
function enc(s: string) { return btoa(unescape(encodeURIComponent(s))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''); }
