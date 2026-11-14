# PgOutput2Json website

The landing page and the docs, built with [Astro](https://astro.build) and [Starlight](https://starlight.astro.build) into static files.

- `src/pages/index.astro` - the landing page, with its layout in `src/layouts/Landing.astro` and its parts in `src/components/`
- `src/content/docs/docs/` - the docs pages, served under `/docs/`. The sidebar is in `src/sidebar.mjs`
- `src/pages/llms.txt.ts`, `src/pages/llms-full.txt.ts` - `/llms.txt` (an index of the docs) and `/llms-full.txt` (all the docs as markdown), generated from the docs pages
- `src/pages/robots.txt.ts` - `/robots.txt`: everything may be crawled, and the sitemap's address (Starlight generates `/sitemap-index.xml`)
- `src/styles/theme.css` - the colors and fonts, shared by the docs and the landing page

The docs pages were split from the repository's `README.md` and the adapters' READMEs: when one of those changes, update the page too.

## Run it locally

It needs Node.js 22.23 or later (Astro 7).

```bash
cd website
npm ci
npm run dev        # http://localhost:4321/, reloads on changes
npm run build      # the static site, in dist/
npm run preview    # serves dist/
```

## Deployment

`.github/workflows/website.yml` builds the site and deploys it to GitHub Pages on every push to `main` that changes `website/` or the
workflow, and on demand (Actions > Website > Run workflow). The repository's Settings > Pages > Source must be "GitHub Actions".

## The domain

The site is at `https://pg2json.com/`. The address is set in three places, which must agree:

1. `SITE_URL` in `site.config.mjs`. The links, the canonical URLs, the sitemap, `/robots.txt` and `/llms.txt` are built from it.
2. `public/CNAME`, the host name.
3. The repository's Settings > Pages > Custom domain, with its DNS records. With a workflow deployment, GitHub takes the domain from
   this setting, not from the CNAME file.
