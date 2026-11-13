import { getCollection, type CollectionEntry } from 'astro:content';
import { SITE_URL, GITHUB_URL, DESCRIPTION } from '../../site.config.mjs';
import { docSlugs } from '../sidebar.mjs';

type Doc = CollectionEntry<'docs'>;

export const pageUrl = (slug: string) => new URL(`${slug}/`, SITE_URL).href;

/** The docs pages, in the sidebar's order. */
export async function getDocs(): Promise<Doc[]> {
  const docs = await getCollection('docs');
  const bySlug = new Map(docs.map((doc) => [doc.id, doc]));

  return docSlugs.map((slug) => {
    const doc = bySlug.get(slug);
    if (!doc) throw new Error(`The sidebar has ${slug}, but there's no such page`);
    return doc;
  });
}

/** A page's source as plain markdown, with absolute links. */
export function toPlainMarkdown(doc: Doc): string {
  const base = new URL(`${doc.id}/`, SITE_URL);

  return (doc.body ?? '')
    .replace(/\]\((\.\.\/[^)]*)\)/g, (_, href: string) => `](${new URL(href, base).href})`)
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

export const intro = `# PgOutput2Json

> ${DESCRIPTION} It reads the built-in pgoutput plugin, so PostgreSQL needs no extension. Delivery is at-least-once, and every message carries a deduplication key. .NET 8, 9 and 10.

The library is under active development. Source: ${GITHUB_URL}`;

export const textResponse = (text: string) =>
  new Response(text + '\n', { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
