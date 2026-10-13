// Verifies every internal href/src in dist/ resolves to a file.
import fs from 'node:fs';
import path from 'node:path';

const DIST = path.resolve(import.meta.dirname, '../dist');
const files = [];
(function walk(d) { for (const f of fs.readdirSync(d, { withFileTypes: true })) f.isDirectory() ? walk(path.join(d, f.name)) : f.name.endsWith('.html') && files.push(path.join(d, f.name)); })(DIST);

const exists = (url) => {
  const p = path.join(DIST, decodeURIComponent(url));
  return fs.existsSync(p) && (fs.statSync(p).isFile() || fs.existsSync(path.join(p, 'index.html')));
};
let bad = 0;
for (const f of files) {
  const html = fs.readFileSync(f, 'utf8');
  for (const m of html.matchAll(/(?:href|src)="(\/[^"#?]*)/g)) {
    if (m[1].startsWith('//') || m[1].startsWith('/pagefind/')) continue;
    if (!exists(m[1])) { bad++; if (bad < 40) console.error(`broken: ${m[1]}  in ${path.relative(DIST, f)}`); }
  }
}
console.log(`checked ${files.length} pages, ${bad} broken links`);
process.exit(bad ? 1 : 0);
