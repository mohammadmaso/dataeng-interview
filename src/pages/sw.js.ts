import type { APIRoute } from 'astro';

// Service worker: pages and data you visit stay available offline.
// - hashed build assets and fonts: cache-first (they never change under the same URL)
// - everything else same-origin: network-first, falling back to the cache
const BUILD = Date.now().toString(36);

const source = `
const CACHE = 'de-${BUILD}';
const IMMUTABLE = /^\\/(_astro|fonts|icons)\\//;

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(['/en/', '/fa/'])).then(() => self.skipWaiting()));
});
self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== location.origin) return;
  if (IMMUTABLE.test(url.pathname)) {
    e.respondWith(caches.match(req).then((hit) => hit || fetch(req).then((res) => { const copy = res.clone(); caches.open(CACHE).then((c) => c.put(req, copy)); return res; })));
    return;
  }
  e.respondWith(
    fetch(req).then((res) => {
      if (res.ok) { const copy = res.clone(); caches.open(CACHE).then((c) => c.put(req, copy)); }
      return res;
    }).catch(() => caches.match(req).then((hit) => hit || (req.mode === 'navigate' ? caches.match('/' + (url.pathname.split('/')[1] === 'fa' ? 'fa' : 'en') + '/') : Response.error())))
  );
});
`;

export const GET: APIRoute = () => new Response(source, { headers: { 'Content-Type': 'application/javascript; charset=utf-8' } });
