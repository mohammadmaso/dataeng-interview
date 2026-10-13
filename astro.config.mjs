import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import tailwindcss from '@tailwindcss/vite';
import remarkGlossary from './src/plugins/remark-glossary.mjs';

export default defineConfig({
  site: 'https://dataeng.mohammadmaso.ir',
  trailingSlash: 'always',
  build: { format: 'directory' },
  markdown: { remarkPlugins: [remarkGlossary], shikiConfig: { themes: { light: 'github-light', dark: 'github-dark' } } },
  integrations: [mdx()],
  vite: { plugins: [tailwindcss()] },
});
