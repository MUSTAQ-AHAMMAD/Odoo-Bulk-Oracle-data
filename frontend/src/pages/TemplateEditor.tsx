import { useEffect, useState } from 'react';
import client from '../api/client';

const findPlaceholders = (content: string): string[] => {
  const matches = new Set<string>();
  for (const match of content.matchAll(/{{\s*([\w.]+)\s*}}/g)) {
    const placeholder = match[1]?.split('.')[0];
    if (placeholder) matches.add(placeholder);
  }
  return [...matches];
};

export default function TemplateEditor(): JSX.Element {
  const [name, setName] = useState('');
  const [content, setContent] = useState('');
  const [outputType, setOutputType] = useState<'html' | 'pdf' | 'txt'>('html');
  const [placeholders, setPlaceholders] = useState<string[]>([]);

  useEffect(() => {
    setPlaceholders(findPlaceholders(content));
  }, [content]);

  const save = async (): Promise<void> => {
    await client.post('/api/templates', { name, content, outputType });
    setName('');
    setContent('');
  };

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Template Editor</h2>
      <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Template name" className="w-full rounded border p-2" />
      <select value={outputType} onChange={(e) => setOutputType(e.target.value as 'html' | 'pdf' | 'txt')} className="rounded border p-2">
        <option value="html">HTML</option>
        <option value="pdf">PDF</option>
        <option value="txt">TXT</option>
      </select>
      <textarea value={content} onChange={(e) => setContent(e.target.value)} className="h-64 w-full rounded border p-3 font-mono" />
      <div className="flex flex-wrap gap-2">
        {placeholders.map((p) => <span key={p} className="rounded bg-blue-100 px-2 py-1 text-xs text-blue-700">{p}</span>)}
      </div>
      <button className="rounded bg-blue-600 px-4 py-2 text-white" onClick={save}>Save Template</button>
    </div>
  );
}
