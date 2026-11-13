import type { APIRoute } from 'astro';
import { SITE_URL } from '../../site.config.mjs';
import { sidebar } from '../sidebar.mjs';
import { getDocs, intro, pageUrl, textResponse } from '../lib/llms';

export const GET: APIRoute = async () => {
  const docs = new Map((await getDocs()).map((doc) => [doc.id, doc]));

  const sections = sidebar.map((group) => {
    const lines = group.items.map((item) => {
      const doc = docs.get(item.slug)!;
      return `- [${doc.data.title}](${pageUrl(item.slug)}): ${doc.data.description}`;
    });
    return `## ${group.label}\n\n${lines.join('\n')}`;
  });

  const full = new URL('llms-full.txt', SITE_URL).href;

  return textResponse(`${intro}\n\n${sections.join('\n\n')}\n\n## Optional\n\n- [All the docs in one file](${full}): every page above as plain markdown`);
};
