import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import client from '../api/client';
import type { Job } from '../types';
import { ProgressBar } from '../components/common/ProgressBar';
import { StatusBadge } from '../components/common/StatusBadge';
import { useWebSocket } from '../hooks/useWebSocket';

export default function JobDetail(): JSX.Element {
  const { id = '' } = useParams();
  const [job, setJob] = useState<Job | null>(null);
  const ws = useWebSocket(id);

  useEffect(() => {
    client.get('/api/jobs?limit=100').then((res) => {
      const found = (res.data.items as Job[]).find((item) => item.id === id) ?? null;
      setJob(found);
    });
  }, [id]);

  if (!job) return <p>Loading...</p>;

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Job Detail</h2>
      <StatusBadge status={(ws.status as Job['status']) || job.status} />
      <ProgressBar progress={ws.progress || job.progress} />
      <p>Total: {job.totalRows} | Success: {job.successRows} | Failed: {job.failedRows}</p>
      {job.errorRows && job.errorRows.length > 0 && (
        <div className="rounded border p-3">
          <h3 className="mb-2 font-semibold">Error Rows</h3>
          {job.errorRows.map((err) => (
            <p key={`${err.rowNumber}-${err.message}`}>Row {err.rowNumber}: {err.message}</p>
          ))}
        </div>
      )}
      {job.status === 'completed' && <a className="text-blue-600" href={`${import.meta.env.VITE_API_URL}/api/download/${job.id}`}>Download ZIP</a>}
    </div>
  );
}
