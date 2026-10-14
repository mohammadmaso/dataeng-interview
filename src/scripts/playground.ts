import { SEED } from '../data/sample-db';
import { openDb, resultsHtml, type Database } from './sqlrun';
import { ui, esc } from './store';

const a = ui().app;
const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const editor = $<HTMLTextAreaElement>('pg-editor'), out = $('pg-out'), schema = $('pg-schema'), status = $('pg-status');
const examples: { id: string; title: string; sql: string }[] = JSON.parse($('pg-data').textContent || '[]');
let db: Database | null = null;

const enc = (s: string) => btoa(unescape(encodeURIComponent(s))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const dec = (s: string) => { try { return decodeURIComponent(escape(atob(s.replace(/-/g, '+').replace(/_/g, '/')))); } catch { return ''; } };

async function ensureDb() {
  if (db) return db;
  status.textContent = a.pgLoading;
  db = await openDb(SEED);
  status.textContent = '';
  drawSchema();
  return db;
}
function drawSchema() {
  if (!db) return;
  schema.innerHTML = db.tables().map((t) => `<details open class="card px-3 py-2"><summary class="cursor-pointer font-semibold">${esc(t.name)}</summary><ul class="mt-1 grid gap-0.5 text-xs" style="color:var(--fg-muted)">${t.columns.map((c) => `<li><code>${esc(c.name)}</code> <span class="opacity-70">${esc(c.type)}</span></li>`).join('')}</ul></details>`).join('');
}
async function run() {
  const sql = editor.value.trim(); if (!sql) return;
  out.innerHTML = '';
  try {
    const d = await ensureDb(); const t0 = performance.now();
    const sets = d.exec(sql);
    out.innerHTML = resultsHtml(sets, { rows: a.pgRows, none: a.pgNoRows, truncated: a.pgTruncated });
    status.textContent = `${Math.round(performance.now() - t0)} ms`;
    drawSchema(); // DDL may have changed the tables
  } catch (e) {
    out.innerHTML = `<pre class="whitespace-pre-wrap rounded-xl border p-3 text-sm" dir="ltr" style="border-color:#dc2626;color:#dc2626">${a.pgError}: ${esc((e as Error).message)}</pre>`;
    status.textContent = '';
  }
}

$('pg-run').addEventListener('click', run);
$('pg-reset').addEventListener('click', async () => { db?.close(); db = null; out.innerHTML = ''; await ensureDb(); status.textContent = '✓'; });
$('pg-link').addEventListener('click', async () => {
  const url = `${location.origin}${location.pathname}#q=${enc(editor.value)}`;
  history.replaceState(null, '', url);
  try { await navigator.clipboard.writeText(url); status.textContent = ui().app.copied ?? '✓'; } catch { status.textContent = '✓'; }
});
editor.addEventListener('keydown', (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); run(); }
  else if (e.key === 'Tab' && !e.shiftKey) { e.preventDefault(); const s = editor.selectionStart; editor.setRangeText('  ', s, editor.selectionEnd, 'end'); }
});
$('pg-examples').addEventListener('click', (e) => {
  const b = (e.target as HTMLElement).closest<HTMLElement>('[data-ex]'); if (!b) return;
  editor.value = examples.find((x) => x.id === b.dataset.ex)!.sql; run();
});

const fromHash = location.hash.startsWith('#q=') ? dec(location.hash.slice(3)) : '';
editor.value = fromHash || examples[0].sql;
ensureDb().then(() => { if (fromHash) run(); }).catch((e) => { schema.textContent = String(e); });
