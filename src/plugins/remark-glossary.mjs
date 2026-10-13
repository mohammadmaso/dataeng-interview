// Links the first occurrence (per page) of every glossary term; supports explicit [[slug]] / [[slug|label]].
import { SKIP, visit } from 'unist-util-visit';
import fs from 'node:fs';
import path from 'node:path';
import { findNext, loadGlossary } from '../lib/glossary.mjs';

const topicTitle = (id) => {
  const f = path.resolve(process.cwd(), 'src/content/topics/en', `${id}.md`);
  return fs.existsSync(f) ? fs.readFileSync(f, 'utf8').match(/^title: "(.*?)"/m)?.[1] ?? id : null;
};

const BLOCKED = new Set(['link', 'linkReference', 'heading', 'code', 'inlineCode', 'mdxJsxFlowElement', 'mdxJsxTextElement']);

export default function remarkGlossary() {
  return (tree, file) => {
    const p = (file.path ?? '').replace(/\\/g, '/');
    const lang = /\/fa\//.test(p) ? 'fa' : 'en';
    const { bySlug } = loadGlossary(lang);
    const used = new Set();
    if (/\/glossary\//.test(p)) used.add(p.split('/').pop().replace(/\.mdx?$/, ''));

    const mk = (entry, label) => ({
      type: 'link',
      url: `/${lang}/glossary/${entry.slug}/`,
      data: { hProperties: { className: ['gloss'], 'data-tip': entry.short } },
      children: [{ type: 'text', value: label }],
    });

    visit(tree, 'text', (node, index, parent) => {
      if (!parent || index == null) return;
      if (BLOCKED.has(parent.type)) return SKIP;
      let rest = node.value;
      const out = [];
      const push = (v) => v && out.push({ type: 'text', value: v });
      for (;;) {
        const exp = rest.match(/\[\[([a-z0-9-]+)(?:\|([^\]]+))?\]\]/);
        const hit = findNext(rest, lang, used);
        if (exp && (!hit || exp.index <= hit.index)) {
          const entry = bySlug.get(exp[1]);
          push(rest.slice(0, exp.index));
          if (entry) out.push(mk(entry, exp[2] ?? entry.term.replace(/\s*\(.*\)\s*$/, '')));
          else if (topicTitle(exp[1])) {
            // [[topic-id]] -> link to another lesson
            out.push({ type: 'link', url: `/${lang}/topics/${exp[1]}/`, children: [{ type: 'text', value: exp[2] ?? topicTitle(exp[1]) }] });
          } else {
            console.warn(`[glossary] unknown [[${exp[1]}]] in ${p}`);
            push(exp[2] ?? exp[1].replace(/-/g, ' '));
          }
          rest = rest.slice(exp.index + exp[0].length);
        } else if (hit) {
          push(rest.slice(0, hit.index));
          used.add(hit.entry.slug);
          out.push(mk(hit.entry, hit.text));
          rest = rest.slice(hit.index + hit.text.length);
        } else { push(rest); break; }
      }
      if (out.length === 1 && out[0].type === 'text') return;
      parent.children.splice(index, 1, ...out);
      return index + out.length;
    });
  };
}
