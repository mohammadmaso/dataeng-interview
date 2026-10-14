// Question-bank chapter page: filters, per-question grading (spaced repetition), bookmarks, bilingual view.
import { getSrs, grade, isDue, hasBm, toggleBm, loadChapter, lang, ui, digits, today } from './store';

const main = document.querySelector<HTMLElement>('main[data-chapter]');
if (main) init(main);

function init(main: HTMLElement) {
  const a = ui().app; const L = lang(); const slug = main.dataset.chapter!;
  const items = [...document.querySelectorAll<HTMLElement>('.q-item')];
  const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
  const f = $<HTMLInputElement>('q-filter'), c = $('q-count'), fd = $<HTMLSelectElement>('q-diff'), fs = $<HTMLSelectElement>('q-status'), sum = $('q-summary');

  const stateOf = (id: string): 'new' | 'missed' | 'learning' | 'mastered' => {
    const card = getSrs()[id]; if (!card) return 'new';
    if (card.b === 0 && card.l > 0) return 'missed';
    return card.b >= 4 ? 'mastered' : 'learning';
  };
  const dot = { new: 'var(--border)', missed: '#dc2626', learning: '#d97706', mastered: 'var(--accent)' };

  // inject star + status dot + grade bar once
  for (const li of items) {
    const id = li.id; const summary = li.querySelector('summary')!;
    const star = document.createElement('button');
    star.type = 'button'; star.className = 'q-star'; star.setAttribute('aria-label', a.bookmark);
    star.addEventListener('click', (e) => { e.preventDefault(); e.stopPropagation(); toggleBm('q:' + id); paint(); });
    summary.insertBefore(star, summary.querySelector('svg'));
    const dotEl = document.createElement('i'); dotEl.className = 'q-dot'; summary.prepend(dotEl);

    const body = li.querySelector('.prose')!;
    const gr = document.createElement('div'); gr.className = 'q-grade';
    gr.innerHTML = `<span>${a.qSelfGrade}</span><button data-g="0" class="g0">${a.gradeMissed}</button><button data-g="1" class="g1">${a.gradeUnsure}</button><button data-g="2" class="g2">${a.gradeGot}</button><em></em>`;
    gr.addEventListener('click', (e) => {
      const b = (e.target as HTMLElement).closest<HTMLElement>('button[data-g]'); if (!b) return;
      grade(id, +b.dataset.g! as 0 | 1 | 2, slug);
      gr.querySelector('em')!.textContent = '✓ ' + a.qGraded;
      paint();
    });
    body.after(gr);
  }

  function paint(refilter = true) {
    const srs = getSrs(); let seen = 0, mastered = 0, due = 0; const day = today();
    for (const li of items) {
      const id = li.id; const st = stateOf(id); const card = srs[id];
      if (card) { seen++; if (st === 'mastered') mastered++; if (isDue(card, day)) due++; }
      li.dataset.state = st; li.dataset.due = card && isDue(card, day) ? '1' : '';
      li.dataset.saved = hasBm('q:' + id) ? '1' : '';
      (li.querySelector('.q-dot') as HTMLElement).style.background = dot[st];
      const star = li.querySelector('.q-star') as HTMLElement; star.textContent = li.dataset.saved ? '★' : '☆'; star.classList.toggle('on', !!li.dataset.saved);
    }
    sum.textContent = `${digits(seen)}/${digits(items.length)} ${a.qSeen} · ${digits(mastered)} ${a.qMastered}${due ? ' · ' + digits(due) + ' ' + a.qDue : ''}`;
    if (refilter) filter();
  }
  function filter() {
    const v = f.value.trim().toLowerCase(), d = fd.value, s = fs.value; let shown = 0;
    for (const li of items) {
      const st = li.dataset.state!;
      const ok = (!v || li.dataset.q!.indexOf(v) > -1) && (d === '' || li.dataset.d === d) &&
        (s === '' || (s === 'new' && st === 'new') || (s === 'missed' && st === 'missed') || (s === 'due' && !!li.dataset.due) || (s === 'saved' && !!li.dataset.saved) || (s === 'mastered' && st === 'mastered'));
      li.style.display = ok ? '' : 'none'; if (ok) shown++;
    }
    c.textContent = `${digits(shown)} / ${digits(items.length)}`;
    $('q-none').classList.toggle('hidden', shown > 0);
  }
  [f, fd, fs].forEach((x) => x.addEventListener('input', filter));
  document.addEventListener('de:change', () => paint(false));

  $('q-expand').onclick = () => items.forEach((li) => { if (li.style.display !== 'none') li.querySelector('details')!.open = true; });
  $('q-collapse').onclick = () => items.forEach((li) => (li.querySelector('details')!.open = false));
  $('q-random').onclick = () => {
    const vis = items.filter((li) => li.style.display !== 'none'); if (!vis.length) return;
    const li = vis[Math.floor(Math.random() * vis.length)]; li.querySelector('details')!.open = true;
    li.scrollIntoView({ block: 'center', behavior: 'smooth' }); history.replaceState(null, '', '#' + li.id);
  };

  // bilingual view
  const other = L === 'fa' ? 'en' : 'fa'; let on = false; let loaded = false;
  const bil = $('q-bil');
  bil.textContent = L === 'fa' ? a.qShowOther : a.qShowOtherFa;
  bil.onclick = async () => {
    on = !on; bil.setAttribute('aria-pressed', String(on)); bil.classList.toggle('chip-P0', on);
    if (on && !loaded) {
      bil.textContent = a.qOtherLoading;
      try {
        const rows = new Map((await loadChapter(other, slug)).map((r) => [r.id, r]));
        for (const li of items) {
          const r = rows.get(li.id); if (!r) continue;
          const q = li.querySelector('.q-text') as HTMLElement;
          const sub = document.createElement('div'); sub.className = 'q-other-q'; sub.dir = other === 'fa' ? 'rtl' : 'ltr'; sub.textContent = r.q; q.after(sub);
          const ans = document.createElement('div'); ans.className = 'q-other prose max-w-none dark:prose-invert'; ans.dir = other === 'fa' ? 'rtl' : 'ltr'; ans.innerHTML = r.a;
          li.querySelector('.prose')!.after(ans);
        }
        loaded = true;
      } catch { on = false; }
      bil.textContent = L === 'fa' ? a.qShowOther : a.qShowOtherFa;
    }
    document.body.classList.toggle('show-other', on);
  };

  function openHash() { const h = location.hash.slice(1); if (!h) return; const el = document.getElementById(h); if (el) { const d = el.querySelector('details'); if (d) d.open = true; el.scrollIntoView({ block: 'center' }); } }
  paint(); openHash(); addEventListener('hashchange', openHash);
}
