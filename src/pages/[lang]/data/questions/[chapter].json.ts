import type { APIRoute } from 'astro';
import { chapters, chapterQuestions } from '../../../../lib/qdata';
import type { Lang } from '../../../../i18n/ui';

export function getStaticPaths() {
  return ['en', 'fa'].flatMap((lang) => chapters.map((c) => ({ params: { lang, chapter: c.slug } })));
}
export const GET: APIRoute = ({ params }) => {
  const { rows } = chapterQuestions(params.chapter!, params.lang as Lang);
  return new Response(JSON.stringify(rows.map(({ id, q, a, d }) => ({ id, q, a, d }))), { headers: { 'Content-Type': 'application/json' } });
};
