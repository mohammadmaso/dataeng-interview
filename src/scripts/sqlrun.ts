// Lazy SQLite-in-the-browser engine (sql.js). The ~600 KB wasm is only fetched when someone presses Run.
let enginePromise: Promise<any> | null = null;

export function getEngine() {
  if (!enginePromise) {
    enginePromise = (async () => {
      const [{ default: initSqlJs }, { default: wasmUrl }] = await Promise.all([
        import('sql.js/dist/sql-wasm-browser.js') as Promise<any>,
        import('sql.js/dist/sql-wasm-browser.wasm?url') as Promise<any>,
      ]);
      return initSqlJs({ locateFile: () => wasmUrl });
    })();
    enginePromise.catch(() => { enginePromise = null; });
  }
  return enginePromise;
}

export interface ResultSet { columns: string[]; values: unknown[][] }

export class Database {
  private db: any;
  constructor(private SQL: any) { this.db = new SQL.Database(); }
  exec(sql: string): ResultSet[] { return this.db.exec(sql) as ResultSet[]; }
  tables(): { name: string; columns: { name: string; type: string }[] }[] {
    const t = this.exec("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")[0]?.values ?? [];
    return t.map(([name]) => ({
      name: String(name),
      columns: (this.exec(`PRAGMA table_info("${name}")`)[0]?.values ?? []).map((c) => ({ name: String(c[1]), type: String(c[2] || '') })),
    }));
  }
  close() { this.db.close(); }
}

export async function openDb(seed?: string): Promise<Database> {
  const SQL = await getEngine();
  const d = new Database(SQL);
  if (seed) d.exec(seed);
  return d;
}

/** Renders result sets as HTML tables (escaped). Caps rows for the DOM's sake. */
export function resultsHtml(sets: ResultSet[], labels: { rows: string; none: string; truncated: string }, cap = 500): string {
  const esc = (s: unknown) => String(s ?? 'NULL').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]!));
  if (!sets.length) return `<p class="text-sm" style="color:var(--fg-muted)">${labels.none}</p>`;
  return sets.map((r) => {
    const rows = r.values.slice(0, cap);
    return `<div class="sql-res"><div class="overflow-x-auto"><table dir="ltr"><thead><tr>${r.columns.map((c) => `<th>${esc(c)}</th>`).join('')}</tr></thead><tbody>${rows.map((row) => `<tr>${row.map((v) => `<td${v === null ? ' class="null"' : typeof v === 'number' ? ' class="num"' : ''}>${esc(v)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>
      <div class="mt-1 text-xs" style="color:var(--fg-muted)">${r.values.length} ${labels.rows}${r.values.length > cap ? ' · ' + labels.truncated : ''}</div></div>`;
  }).join('');
}

/** Heuristic: does this snippet look runnable on SQLite? Used to decide whether to show a Run button in lessons. */
export function looksRunnable(sql: string): boolean {
  const s = sql.trim();
  if (!/^(--[^\n]*\n\s*)*(select|with|create\s+table|insert|values|pragma)\b/i.test(s)) return false;
  if (/::|\bqualify\b|\bdate_trunc\b|\binterval\b|\bgenerate_series\b|\bilike\b|\blateral\b|\barray_agg\b|\bstring_agg\b|\bmerge\b|\bcluster\s+by\b|\bpartitioned\s+by\b|\bstored\s+as\b|\bengine\s*=|\bserial\b|\bjsonb\b|\bto_char\b|\bdatediff\b|\bdate_add\b|\bcurrent_date\b|\bnow\(\)|\bextract\s*\(|\bunnest\b|\brange\s+between\s+interval/i.test(s)) return false;
  if (/\bfrom\s+\w+\.\w+\./i.test(s)) return false;
  return true;
}
