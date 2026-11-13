import type { APIRoute } from 'astro';
import { SITE_URL } from '../../site.config.mjs';
import { textResponse } from '../lib/llms';

// Everything may be crawled; the sitemap is Starlight's. Crawlers only read robots.txt at the root of a domain, so with a base
// path in SITE_URL this file isn't used.
export const GET: APIRoute = () => {
  const sitemap = new URL('sitemap-index.xml', SITE_URL).href;

  return textResponse(`User-agent: *\nAllow: /\n\nSitemap: ${sitemap}\n`);
};
