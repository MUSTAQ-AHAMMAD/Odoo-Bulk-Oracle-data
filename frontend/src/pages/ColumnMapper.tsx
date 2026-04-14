import { useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import client from '../api/client';
import type { Template } from '../types';

export default function ColumnMapper(): JSX.Element {
  const { jobId = '' } = useParams();
  const navigate = useNavigate();
  const [columns, setColumns] = useState<string[]>([]);
  const [templateId, setTemplateId] = useState('');
  const [templates, setTemplates] = useState<Template[]>([]);
  const [mapping, setMapping] = useState<Record<string, string>>({});

  const selectedTemplate = templates.find((item) => item.id === templateId);

  useEffect(() => {
    client.get(`/api/preview/${jobId}`).then((res) => setColumns(res.data.headers as string[]));
    client.get('/api/templates').then((res) => setTemplates(res.data as Template[]));
  }, [jobId]);

  const saveAndGenerate = async (): Promise<void> => {
    await client.post('/api/map', { templateId, columnMappings: mapping });
    await client.post(`/api/generate/${jobId}`, { templateId, mapping });
    navigate('/jobs');
  };

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Column Mapper</h2>
      <select className="rounded border p-2" value={templateId} onChange={(e) => setTemplateId(e.target.value)}>
        <option value="">Select template</option>
        {templates.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
      </select>
      {selectedTemplate && (
        <div className="grid gap-3">
          {selectedTemplate.placeholders.map((placeholder) => (
            <label key={placeholder} className="flex items-center gap-2">
              <span className="w-40">{placeholder}</span>
              <select
                className="rounded border p-2"
                value={mapping[placeholder] ?? ''}
                onChange={(e) => setMapping((prev) => ({ ...prev, [placeholder]: e.target.value }))}
              >
                <option value="">Select column</option>
                {columns.map((column) => <option key={column} value={column}>{column}</option>)}
              </select>
            </label>
          ))}
        </div>
      )}
      <button className="rounded bg-blue-600 px-4 py-2 text-white" onClick={saveAndGenerate} disabled={!templateId}>Save Mapping & Generate</button>
    </div>
  );
}
