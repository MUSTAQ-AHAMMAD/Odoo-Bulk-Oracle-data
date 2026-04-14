export type JobStatus = 'pending' | 'processing' | 'completed' | 'failed';

export interface Template {
  id: string;
  name: string;
  content: string;
  outputType: 'html' | 'pdf' | 'txt';
  placeholders: string[];
  createdAt: string;
}

export interface Job {
  id: string;
  fileName: string;
  status: JobStatus;
  progress: number;
  totalRows: number;
  successRows: number;
  failedRows: number;
  createdAt: string;
  template?: Template | null;
  errorRows?: Array<{ rowNumber: number; message: string }> | null;
}
