// Emits lesson-writing batches (same-track neighbours, ~4 topics each) for the agent fleet.
// Usage: node scripts/make-batches.mjs [--size=4] [--priority=P0,P1] [--tracks=7,8] → docs/batches.json
import fs from 'node:fs';
import path from 'node:path';
const ROOT = path.resolve(import.meta.dirname, '..');
const arg = (k, d) => process.argv.find((a) => a.startsWith(`--${k}=`))?.split('=')[1] ?? d;
const size = Number(arg('size', 4));
const pri = arg('priority', 'P0,P1,P2').split(',');
const trks = arg('tracks', '')?.split(',').filter(Boolean);
const qIndex = JSON.parse(fs.readFileSync(path.join(ROOT, 'src/data/question-index.json'), 'utf8'));

const dir = path.join(ROOT, 'src/content/topics/en');
const topics = fs.readdirSync(dir).map((f) => {
  const raw = fs.readFileSync(path.join(dir, f), 'utf8');
  const g = (k) => raw.match(new RegExp(`^${k}: (.*)$`, 'm'))?.[1];
  return { tid: JSON.parse(g('tid')), track: JSON.parse(g('track')), order: Number(g('order')), priority: JSON.parse(g('priority')), type: JSON.parse(g('type')), stub: raw.includes('<!-- stub -->') };
}).filter((t) => t.stub && pri.includes(t.priority) && (!trks.length || trks.includes(t.track))).sort((a, b) => a.order - b.order);

const batches = [];
for (const t of topics) {
  const last = batches.at(-1);
  if (last && last.track === t.track && last.topics.length < size) last.topics.push(t.tid);
  else batches.push({ id: `b${String(batches.length + 1).padStart(2, '0')}`, track: t.track, topics: [t.tid] });
}
for (const b of batches) b.questionChapters = [...new Set(qIndex.filter((c) => c.topics.some((id) => b.topics.includes(id))).map((c) => c.slug))];
fs.writeFileSync(path.join(ROOT, 'docs/batches.json'), JSON.stringify(batches, null, 1));
console.log(`${topics.length} topics → ${batches.length} batches`);
