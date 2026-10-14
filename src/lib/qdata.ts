// Question-bank loading shared by the chapter pages and the JSON data endpoints.
import qIndex from '../data/question-index.json';
import { difficultyScore, assignLevels } from './difficulty';
import type { Lang } from '../i18n/ui';

const en = import.meta.glob('../data/questions/*.json', { eager: true, import: 'default' }) as Record<string, any>;
const fa = import.meta.glob('../data/questions-fa/*.json', { eager: true, import: 'default' }) as Record<string, any>;
const pick = (m: Record<string, any>, slug: string) => Object.entries(m).find(([k]) => k.endsWith(`/${slug}.json`))?.[1];

export const chapters = qIndex as { slug: string; title: string; vol: number; count: number; topics: string[] }[];

export interface QRow { id: string; q: string; a: string; d: 0 | 1 | 2; tr: boolean }

/** Questions of a chapter in the requested language (English fallback when no translation exists). */
export function chapterQuestions(slug: string, lang: Lang): { title: string; rows: QRow[] } {
  const base = pick(en, slug);
  const tr: Record<string, { q: string; a: string }> = lang === 'fa' ? pick(fa, slug) ?? {} : {};
  const src = base.questions as { id: string; q: string; a: string }[];
  const levels = assignLevels(src.map((x) => difficultyScore(x.q, x.a))); // always scored on the English text, so both languages agree
  const rows = src.map((x, i) => {
    const t = tr[x.id];
    return { id: x.id, q: t?.q ?? x.q, a: t?.a ?? x.a, d: levels[i], tr: !!t };
  });
  return { title: base.title as string, rows };
}
