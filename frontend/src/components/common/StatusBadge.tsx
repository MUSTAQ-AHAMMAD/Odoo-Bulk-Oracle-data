import type { JobStatus } from '../../types';

const classMap: Record<JobStatus, string> = {
  pending: 'bg-gray-100 text-gray-700',
  processing: 'bg-blue-100 text-blue-700',
  completed: 'bg-green-100 text-green-700',
  failed: 'bg-red-100 text-red-700'
};

export const StatusBadge = ({ status }: { status: JobStatus }): JSX.Element => (
  <span className={`rounded px-2 py-1 text-xs font-semibold ${classMap[status]}`}>{status}</span>
);
