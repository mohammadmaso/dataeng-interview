// "Last updated" dates for lessons, read once from git history at build time.
// Falls back to an empty map when git (or full history) is unavailable, and the UI then hides the date.
import { execFileSync } from 'node:child_process';

let cache: Map<string, string> | null = null;

export function lastUpdated(): Map<string, string> {
  if (cache) return cache;
  cache = new Map();
  try {
    const out = execFileSync('git', ['log', '--format=@%cs', '--name-only', '--', 'src/content/topics', 'src/content/glossary'], {
      cwd: process.cwd(), encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: ['ignore', 'pipe', 'ignore'],
    });
    let date = '';
    for (const line of out.split('\n')) {
      if (line.startsWith('@')) date = line.slice(1);
      else if (line && date && !cache.has(line)) cache.set(line, date);
    }
  } catch { /* no git available */ }
  return cache;
}

/** `lang` is en|fa, `kind` is topics|glossary, `id` is the file slug. */
export const updatedOf = (kind: 'topics' | 'glossary', lang: string, id: string) =>
  lastUpdated().get(`src/content/${kind}/${lang}/${id}.md`) ?? '';

export const REPO = 'mohammadmaso/dataeng-interview';
export const editUrl = (kind: 'topics' | 'glossary', lang: string, id: string) =>
  `https://github.com/${REPO}/edit/main/src/content/${kind}/${lang}/${id}.md`;
export const issueUrl = (title: string, body: string) =>
  `https://github.com/${REPO}/issues/new?title=${encodeURIComponent(title)}&body=${encodeURIComponent(body)}`;
