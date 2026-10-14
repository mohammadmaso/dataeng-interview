import type { APIRoute } from 'astro';
import { getTopics, tracks, trackTitle } from '../../../lib/data';
import { chapters } from '../../../lib/qdata';
import type { Lang } from '../../../i18n/ui';

export function getStaticPaths() { return [{ params: { lang: 'en' } }, { params: { lang: 'fa' } }]; }
export const GET: APIRoute = async ({ params }) => {
  const lang = params.lang as Lang;
  const topics = await getTopics(lang);
  const body = {
    chapters,
    tracks: tracks.map((t) => ({ id: t.id, title: trackTitle(t, lang) })),
    topics: topics.map((x) => ({
      tid: x.data.tid, title: x.data.title, track: x.data.track, level: x.data.level, priority: x.data.priority,
      type: x.data.type, hours: x.data.est_hours, covers: x.data.covers, summary: x.data.summary,
    })),
  };
  return new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } });
};
