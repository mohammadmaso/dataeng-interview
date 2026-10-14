import type { APIRoute } from 'astro';
import { getTopics } from '../../../lib/data';
import { extractFlashcards } from '../../../lib/flashcards';
import type { Lang } from '../../../i18n/ui';

export function getStaticPaths() { return [{ params: { lang: 'en' } }, { params: { lang: 'fa' } }]; }
// { "<tid>": { t: title, c: [[front, back], …] } } — card id is `fc:<tid>:<index>`
export const GET: APIRoute = async ({ params }) => {
  const topics = await getTopics(params.lang as Lang);
  const out: Record<string, { t: string; k: string; c: string[][] }> = {};
  for (const x of topics) {
    const cards = extractFlashcards(x.body ?? '');
    if (cards.length) out[x.data.tid] = { t: x.data.title, k: x.data.track, c: cards.map((c) => [c.front, c.back]) };
  }
  return new Response(JSON.stringify(out), { headers: { 'Content-Type': 'application/json' } });
};
