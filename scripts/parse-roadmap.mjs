// Parses content-src/roadmap.md into stub topic files + src/data/tracks.json.
// Re-runnable: files that no longer contain the STUB marker (i.e. have real lessons) are never overwritten.
import fs from 'node:fs';
import path from 'node:path';

const ROOT = path.resolve(import.meta.dirname, '..');
const SRC = path.join(ROOT, 'content-src/roadmap.md');
const OUT = path.join(ROOT, 'src/content/topics');
const STUB = '<!-- stub -->';

const LEVELS = { '🟢': 'beginner', '🟡': 'intermediate', '🟠': 'advanced', '🔴': 'senior' };
const TYPES = ['Guide', 'Concept', 'Technology', 'Pattern', 'Problem', 'Practice'];

const lines = fs.readFileSync(SRC, 'utf8').split('\n');

// ---- track weights from the "Track map" table
const weights = {};
for (const l of lines) {
  const m = l.match(/^\| (\d+) \| .*? \| .*? \| (★+) \|$/);
  if (m) weights[m[1]] = m[2].length;
}

const tracks = [];
const topics = [];
let track = null;
let section = null;
let cur = null;
let curList = null;
let introBuf = null;

const flushTopic = () => {
  if (cur) topics.push(cur);
  cur = null;
  curList = null;
};

const clean = (s) => s.replace(/`/g, '').trim();

for (let i = 0; i < lines.length; i++) {
  const l = lines[i];

  let m = l.match(/^# Track (\d+) — (.+?) \| (.+)$/);
  if (m) {
    flushTopic();
    track = { id: m[1], title_en: m[2].trim(), title_fa: m[3].trim(), weight: weights[m[1]] ?? 3, intro_en: '', intro_fa: '' };
    tracks.push(track);
    section = null;
    introBuf = track;
    continue;
  }
  if (/^# Appendix/.test(l)) {
    flushTopic();
    track = null;
    introBuf = null;
    continue;
  }
  m = l.match(/^## (14[A-D]) — (.+?) \| (.+)$/);
  if (m && track) {
    flushTopic();
    section = { id: m[1], title_en: m[2].trim(), title_fa: m[3].trim() };
    track.sections = track.sections ?? [];
    track.sections.push(section);
    introBuf = null;
    continue;
  }
  if (!track) continue;

  // track intro blockquotes (before the first topic)
  if (introBuf && !cur) {
    const q = l.match(/^> \*\*(FA|EN):\*\* (.+)$/);
    if (q) introBuf[q[1] === 'FA' ? 'intro_fa' : 'intro_en'] = q[2].trim();
  }

  // topic heading: "### 2.4 Window Functions" or "### P3. Late & Out-of-Order Data"
  m = l.match(/^### (?:(\d+\.\d+)|(P\d+)\.) (.+)$/);
  if (m) {
    flushTopic();
    introBuf = null;
    const num = m[1] ?? m[2];
    const meta = (lines[i + 1] ?? '').match(/^`([^`]+)` · (\S+) \w+ · (P\d) · ~(\d+)h · (\w+)$/);
    if (!meta) throw new Error(`Bad meta line after heading "${l}": ${lines[i + 1]}`);
    cur = {
      id: meta[1], track: track.id, section: section?.id ?? null, num,
      title_en: m[3].trim(), level: LEVELS[meta[2]], priority: meta[3],
      est_hours: Number(meta[4]), type: meta[5].toLowerCase(),
      summary_en: '', summary_fa: '', covers: [], interview_qs: [], extras: [],
    };
    if (!TYPES.includes(meta[5])) throw new Error(`Unknown type ${meta[5]}`);
    i++;
    continue;
  }

  // "14D" style topic without a ### heading: `case-studies` · ... line directly after the section heading
  m = l.match(/^`([^`]+)` · (\S+) \w+ · (P\d) · ~(\d+)h · (\w+)$/);
  if (m && !cur) {
    cur = {
      id: m[1], track: track.id, section: section?.id ?? null, num: section?.id ?? '',
      title_en: m[1] === 'case-studies' ? 'In the Wild: Engineering Case Studies' : m[1],
      level: LEVELS[m[2]], priority: m[3], est_hours: Number(m[4]), type: m[5].toLowerCase(),
      summary_en: '', summary_fa: '', covers: [], interview_qs: [], extras: [],
    };
    continue;
  }

  if (!cur) continue;

  m = l.match(/^- \*\*(FA|EN):\*\* (.+)$/);
  if (m) { cur[m[1] === 'FA' ? 'summary_fa' : 'summary_en'] = m[2].trim(); curList = null; continue; }
  m = l.match(/^- \*\*Covers:\*\* (.+)$/);
  if (m) { cur.covers = m[1].split(/,\s+(?![^(]*\))/).map((s) => clean(s)); curList = null; continue; }
  m = l.match(/^- \*\*Interview Qs:\*\* (.+)$/);
  if (m) { cur.interview_qs = m[1].split(' · ').map((s) => s.trim()); curList = null; continue; }
  m = l.match(/^- \*\*(Practice|Problems|Patterns|Categories|Candidates|Template):\*\*\s*(.*)$/);
  if (m) {
    curList = { label: m[1].toLowerCase(), items: [] };
    cur.extras.push(curList);
    if (m[2]) curList.items.push(...m[2].split(' · ').map((s) => s.trim()));
    continue;
  }
  m = l.match(/^\s+\d+\.\s+(.+)$/);
  if (m && curList) { curList.items.push(m[1].replace(/\*\*/g, '').trim()); continue; }
}
flushTopic();

// ---- 14C problem table -> topics
const problemRows = lines.filter((l) => /^\| \d+ \| \*\*/.test(l));
for (const row of problemRows) {
  const c = row.split('|').map((s) => s.trim()).filter(Boolean);
  const [num, title, id, lvl, pri, dives] = c;
  const priority = pri.match(/P\d/)[0];
  topics.push({
    id: clean(id), track: '14', section: '14C', num: `Q${num}`,
    title_en: title.replace(/\*\*/g, '').trim(),
    level: LEVELS[lvl], priority, est_hours: priority === 'P0' ? 4 : priority === 'P1' ? 3 : 2, type: 'problem',
    summary_en: `Worked data system design problem. Key deep dives: ${dives}.`,
    summary_fa: `مسئله‌ی کامل طراحی سیستم داده. بررسی‌های عمیق کلیدی: ${dives}.`,
    covers: dives.split(/,\s+/), interview_qs: [], extras: [],
  });
}

// ---- stable document order: track, then section (none < A < B < C < D), then original index
const secRank = (t) => (t.section ? t.section.slice(-1).charCodeAt(0) - 64 : 0);
topics.forEach((t, i) => (t._i = i));
topics.sort((a, b) => Number(a.track) - Number(b.track) || secRank(a) - secRank(b) || a._i - b._i);
topics.forEach((t, i) => (t.order = i));

// ---- ids must be unique
const seen = new Set();
for (const t of topics) {
  if (seen.has(t.id)) throw new Error(`Duplicate id ${t.id}`);
  seen.add(t.id);
}

// ---- write
const q = (v) => JSON.stringify(v);
const frontmatter = (t, lang) => {
  const fm = {
    order: t.order, tid: t.id, track: t.track, section: t.section, num: t.num,
    title: t.title_en, // roadmap titles are English by design
    type: t.type, level: t.level, priority: t.priority, est_hours: t.est_hours,
    summary: lang === 'fa' ? t.summary_fa : t.summary_en,
    covers: t.covers, interview_qs: t.interview_qs, extras: t.extras,
    prerequisites: [], related: [],
  };
  const body = Object.entries(fm).map(([k, v]) => `${k}: ${q(v)}`).join('\n');
  return `---\n${body}\n---\n${STUB}\n`;
};

let written = 0, kept = 0;
for (const lang of ['en', 'fa']) {
  const dir = path.join(OUT, lang);
  fs.mkdirSync(dir, { recursive: true });
  for (const t of topics) {
    const file = path.join(dir, `${t.id}.md`);
    if (fs.existsSync(file) && !fs.readFileSync(file, 'utf8').includes(STUB)) { kept++; continue; }
    fs.writeFileSync(file, frontmatter(t, lang));
    written++;
  }
}

fs.mkdirSync(path.join(ROOT, 'src/data'), { recursive: true });
fs.writeFileSync(path.join(ROOT, 'src/data/tracks.json'), JSON.stringify(tracks, null, 2));

const byPri = (p) => topics.filter((t) => t.priority === p).length;
console.log(`tracks: ${tracks.length}  topics: ${topics.length} (P0 ${byPri('P0')} · P1 ${byPri('P1')} · P2 ${byPri('P2')})`);
console.log(`files written: ${written}, kept (real content): ${kept}`);
