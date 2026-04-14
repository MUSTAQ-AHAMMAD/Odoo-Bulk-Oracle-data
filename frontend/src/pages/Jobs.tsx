import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import client from '../api/client';
import { ProgressBar } from '../components/common/ProgressBar';
import { StatusBadge } from '../components/common/StatusBadge';
import type { Job } from '../types';

export default function Jobs(): JSX.Element {
  const [jobs, setJobs] = useState<Job[]>([]);

  useEffect(() => {
    client.get('/api/jobs?page=1&limit=10').then((res) => setJobs(res.data.items as Job[]));
  }, []);

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Jobs</h2>
      <div className="space-y-2">
        {jobs.map((job) => (
          <div key={job.id} className="grid grid-cols-1 gap-2 rounded border p-3 md:grid-cols-6 md:items-center">
            <span>{job.fileName}</span>
            <span>{job.template?.name ?? '-'}</span>
            <StatusBadge status={job.status} />
            <ProgressBar progress={job.progress} />
            <span>{new Date(job.createdAt).toLocaleString()}</span>
            <div className="flex gap-2">
              <Link className="text-blue-600" to={`/jobs/${job.id}`}>View</Link>
              {job.status === 'completed' && <a className="text-green-600" href={`${import.meta.env.VITE_API_URL}/api/download/${job.id}`}>Download</a>}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
