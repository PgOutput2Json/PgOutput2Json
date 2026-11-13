// @ts-check
import { defineConfig, fontProviders } from 'astro/config';
import starlight from '@astrojs/starlight';
import { SITE_URL, GITHUB_URL, DESCRIPTION } from './site.config.mjs';
import { sidebar } from './src/sidebar.mjs';

const siteUrl = new URL(SITE_URL);
const base = siteUrl.pathname.replace(/\/$/, '');

/** A self-hosted variable font from its @fontsource-variable package. */
const font = (name, cssVariable, file, weight, fallbacks) => ({
  provider: fontProviders.local(),
  name,
  cssVariable,
  fallbacks,
  options: { variants: [{ src: [file], weight, style: 'normal' }] },
});

export default defineConfig({
  site: siteUrl.origin,
  base: base || '/',
  trailingSlash: 'ignore',
  output: 'static',
  redirects: {
    // the destination isn't prefixed with the base path on its own
    '/docs': `${base}/docs/getting-started/`,
  },
  // preloaded, with a fallback font adjusted to their metrics, so the text doesn't shift
  fonts: [
    font('Inter', '--font-inter', '@fontsource-variable/inter/files/inter-latin-wght-normal.woff2', '100 900', ['ui-sans-serif', 'system-ui', 'sans-serif']),
    font('Space Grotesk', '--font-display', '@fontsource-variable/space-grotesk/files/space-grotesk-latin-wght-normal.woff2', '300 700', ['ui-sans-serif', 'system-ui', 'sans-serif']),
    font('JetBrains Mono', '--font-mono', '@fontsource-variable/jetbrains-mono/files/jetbrains-mono-latin-wght-normal.woff2', '100 800', ['ui-monospace', 'monospace']),
  ],
  integrations: [
    starlight({
      title: 'PgOutput2Json',
      description: DESCRIPTION,
      logo: { src: './src/assets/logo.svg', alt: '' },
      favicon: '/favicon.svg',
      // src/pages/404.astro, in the landing page's layout
      disable404Route: true,
      social: [{ icon: 'github', label: 'GitHub', href: GITHUB_URL }],
      editLink: { baseUrl: `${GITHUB_URL}/edit/main/website/` },
      customCss: ['./src/styles/theme.css'],
      components: {
        // adds the fonts
        Head: './src/components/StarlightHead.astro',
      },
      expressiveCode: {
        themes: ['github-dark-default', 'github-light'],
        styleOverrides: { borderRadius: '0.375rem' },
        // a first-line comment is a comment, not a file name for the frame's title
        frames: { extractFileNameFromCode: false },
      },
      sidebar,
    }),
  ],
});
