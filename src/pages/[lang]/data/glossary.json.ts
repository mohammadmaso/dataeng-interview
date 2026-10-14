import type { APIRoute } from 'astro';
import { getTerms, slugOf } from '../../../lib/data';
import type { Lang } from '../../../i18n/ui';

export function getStaticPaths() { return [{ params: { lang: 'en' } }, { params: { lang: 'fa' } }]; }
export const GET: APIRoute = async ({ params }) => {
  const terms = await getTerms(params.lang as Lang);
  const body = terms.map((x) => ({ s: slugOf(x), t: x.data.term, c: x.data.category, d: x.data.short }));
  return new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } });
};
