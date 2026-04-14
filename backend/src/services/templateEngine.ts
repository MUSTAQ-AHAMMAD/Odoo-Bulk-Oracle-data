import Handlebars, { type TemplateDelegate } from 'handlebars';

const ignored = new Set(['if', 'each', 'unless', 'with']);

export const compileTemplate = (content: string): TemplateDelegate<Record<string, unknown>> =>
  Handlebars.compile(content);

export const renderRow = (compiledFn: TemplateDelegate<Record<string, unknown>>, data: Record<string, unknown>): string =>
  compiledFn(data);

export const extractPlaceholders = (content: string): string[] => {
  const placeholders = new Set<string>();
  const regex = /{{\s*([\w.]+)\s*}}/g;
  for (const match of content.matchAll(regex)) {
    const key = (match[1] ?? '').split('.')[0];
    if (key && !ignored.has(key)) placeholders.add(key);
  }
  return [...placeholders];
};
