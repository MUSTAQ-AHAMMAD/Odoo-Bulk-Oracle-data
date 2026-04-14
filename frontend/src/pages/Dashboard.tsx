import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import client from '../api/client';
import type { Job } from '../types';
import { StatusBadge } from '../components/common/StatusBadge';
import { LoadingSkeleton } from '../components/common/LoadingSkeleton';

export default function Dashboard(): JSX.Element {
  const [jobs, setJobs] = useState<Job[] | null>(null);

  useEffect(() => {
    client.get('/api/jobs?limit=5').then((res) => setJobs(res.data.items as Job[])).catch(() => setJobs([]));
  }, []);

  if (!jobs) return <LoadingSkeleton />;

  const completed = jobs.filter((job) => job.status === 'completed').length;
  const failed = jobs.filter((job) => job.status === 'failed').length;
  const processing = jobs.filter((job) => job.status === 'processing').length;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <Card label="Total Jobs" value={jobs.length} />
        <Card label="Completed" value={completed} />
        <Card label="Failed" value={failed} />
        <Card label="Processing" value={processing} />
      </div>
      <div className="rounded border p-4">
        <h3 className="mb-3 text-lg font-semibold">Recent Jobs</h3>
        <div className="space-y-2">
          {jobs.map((job) => (
            <div key={job.id} className="flex items-center justify-between rounded border p-2">
              <span>{job.fileName}</span>
              <StatusBadge status={job.status} />
              <Link className="text-blue-600" to={`/jobs/${job.id}`}>Details</Link>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

const Card = ({ label, value }: { label: string; value: number }): JSX.Element => (
  <div className="rounded border p-4">
    <p className="text-sm text-gray-500">{label}</p>
    <p className="text-2xl font-bold">{value}</p>
  </div>
);
