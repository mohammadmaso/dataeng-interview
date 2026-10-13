import { getCollection, type CollectionEntry } from 'astro:content';
import tracksJson from '../data/tracks.json';
import type { Lang } from '../i18n/ui';

export type Topic = CollectionEntry<'topics'>;
export type Term = CollectionEntry<'glossary'>;

export interface TrackInfo {
  id: string; title_en: string; title_fa: string; weight: number; intro_en: string; intro_fa: string;
  sections?: { id: string; title_en: string; title_fa: string }[];
}
export const tracks = tracksJson as TrackInfo[];
export const trackTitle = (tr: TrackInfo, l: Lang) => (l === 'fa' ? tr.title_fa : tr.title_en);
export const trackIntro = (tr: TrackInfo, l: Lang) => (l === 'fa' ? tr.intro_fa : tr.intro_en);

export async function getTopics(lang: Lang): Promise<Topic[]> {
  const all = await getCollection('topics', (e) => e.id.startsWith(`${lang}/`));
  return all.sort((a, b) => a.data.order - b.data.order);
}
export async function getTerms(lang: Lang): Promise<Term[]> {
  const all = await getCollection('glossary', (e) => e.id.startsWith(`${lang}/`));
  return all.sort((a, b) => a.id.localeCompare(b.id));
}
export const slugOf = (e: { id: string }) => e.id.split('/').pop()!.replace(/\.mdx?$/, '');
export const isStub = (t: Topic) => (t.body ?? '').includes('<!-- stub -->') || !(t.body ?? '').trim();

export const groups = [
  { id: 'start', en: 'Start here', fa: 'شروع از اینجا', tracks: ['0'] },
  { id: 'code', en: 'Coding', fa: 'کدنویسی', tracks: ['2', '3'] },
  { id: 'concepts', en: 'Core concepts', fa: 'مفاهیم پایه', tracks: ['1', '4', '5', '6'] },
  { id: 'tech', en: 'Key technologies', fa: 'تکنولوژی‌های کلیدی', tracks: ['7', '8', '9', '10', '11', '12', '13'] },
  { id: 'design', en: 'System design', fa: 'طراحی سیستم', tracks: ['14'] },
  { id: 'more', en: 'ML/AI & behavioral', fa: 'ML/AI و رفتاری', tracks: ['15', '16'] },
];
export const groupTitle = (g: (typeof groups)[number], l: Lang) => (l === 'fa' ? g.fa : g.en);
