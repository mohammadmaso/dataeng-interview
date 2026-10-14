// Extracts the flashcard table from a lesson body at build time.
// Lessons keep their cards in a `| Front | Back |` table under a heading that starts with 🗂️.
const inline = (s: string) =>
  s.trim()
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/(^|[^*])\*([^*\n]+)\*(?!\*)/g, '$1<em>$2</em>')
    .replace(/&lt;br\s*\/?&gt;/g, '<br>');

export interface Flashcard { front: string; back: string }

export function extractFlashcards(body: string): Flashcard[] {
  const lines = body.split('\n');
  const start = lines.findIndex((l) => /^##\s+🗂/.test(l));
  if (start < 0) return [];
  const out: Flashcard[] = [];
  for (let i = start + 1; i < lines.length; i++) {
    const l = lines[i];
    if (/^##\s/.test(l)) break;
    if (!l.trim().startsWith('|')) continue;
    // GFM splits on every unescaped pipe (even inside backticks) and ignores cells beyond the header's two columns
    const cells: string[] = []; let cur = '';
    const inner = l.trim().replace(/^\|/, '').replace(/\|$/, '');
    for (let k = 0; k < inner.length; k++) {
      const ch = inner[k];
      if (ch === '\\' && inner[k + 1] === '|') { cur += '|'; k++; continue; }
      if (ch === '|') { cells.push(cur); cur = ''; } else cur += ch;
    }
    cells.push(cur);
    // The separator row (|---|---|) marks the row before it as the table header, whatever language it is written in.
    if (/^\s*:?-{2,}:?\s*$/.test(cells[0])) { if (out.length === 1) out.pop(); continue; }
    out.push({ front: inline(cells[0]), back: inline(cells[1] ?? '') });
  }
  return out;
}
