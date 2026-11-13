import type { APIRoute } from 'astro';
import { getDocs, intro, pageUrl, textResponse, toPlainMarkdown } from '../lib/llms';

export const GET: APIRoute = async () => {
  const pages = (await getDocs()).map(
    (doc) => `# ${doc.data.title}\n\nSource: ${pageUrl(doc.id)}\n\n${toPlainMarkdown(doc)}`,
  );

  return textResponse(`${intro}\n\n${pages.join('\n\n---\n\n')}`);
};
