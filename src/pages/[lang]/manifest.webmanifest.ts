import type { APIRoute } from 'astro';
import { t, dirOf, type Lang } from '../../i18n/ui';

export function getStaticPaths() { return [{ params: { lang: 'en' } }, { params: { lang: 'fa' } }]; }
export const GET: APIRoute = ({ params }) => {
  const lang = params.lang as Lang; const s = t(lang);
  const manifest = {
    name: s.siteName, short_name: lang === 'fa' ? 'مصاحبه داده' : 'DE Interview', description: s.heroSub,
    lang, dir: dirOf(lang), start_url: `/${lang}/`, scope: '/', display: 'standalone',
    background_color: '#ffffff', theme_color: '#059669',
    icons: [
      { src: '/icons/icon-192.png', sizes: '192x192', type: 'image/png' },
      { src: '/icons/icon-512.png', sizes: '512x512', type: 'image/png' },
      { src: '/favicon.svg', sizes: 'any', type: 'image/svg+xml' },
    ],
    shortcuts: [
      { name: lang === 'fa' ? 'مرور' : 'Review', url: `/${lang}/review/` },
      { name: lang === 'fa' ? 'مصاحبهٔ آزمایشی' : 'Mock interview', url: `/${lang}/mock/` },
    ],
  };
  return new Response(JSON.stringify(manifest), { headers: { 'Content-Type': 'application/manifest+json' } });
};
