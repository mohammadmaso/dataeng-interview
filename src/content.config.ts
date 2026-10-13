import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { z } from 'astro/zod';

const topics = defineCollection({
  loader: glob({ pattern: '**/*.{md,mdx}', base: './src/content/topics' }),
  schema: z.object({
    order: z.number(),
    tid: z.string(),
    track: z.string(),
    section: z.string().nullable(),
    num: z.string(),
    title: z.string(),
    type: z.enum(['guide', 'concept', 'technology', 'pattern', 'problem', 'practice']),
    level: z.enum(['beginner', 'intermediate', 'advanced', 'senior']),
    priority: z.enum(['P0', 'P1', 'P2']),
    est_hours: z.number(),
    summary: z.string(),
    covers: z.array(z.string()).default([]),
    interview_qs: z.array(z.string()).default([]),
    extras: z.array(z.object({ label: z.string(), items: z.array(z.string()) })).default([]),
    prerequisites: z.array(z.string()).default([]),
    related: z.array(z.string()).default([]),
  }),
});

const glossary = defineCollection({
  loader: glob({ pattern: '**/*.md', base: './src/content/glossary' }),
  schema: z.object({
    term: z.string(),
    term_en: z.string().optional(),
    aliases: z.array(z.string()).default([]),
    category: z.string().default('general'),
    short: z.string(),
    related: z.array(z.string()).default([]),
  }),
});

export const collections = { topics, glossary };
