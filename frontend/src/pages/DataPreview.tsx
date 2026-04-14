import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { createColumnHelper, flexRender, getCoreRowModel, useReactTable } from '@tanstack/react-table';
import client from '../api/client';

export default function DataPreview(): JSX.Element {
  const { jobId = '' } = useParams();
  const navigate = useNavigate();
  const [headers, setHeaders] = useState<string[]>([]);
  const [rows, setRows] = useState<Array<Record<string, unknown>>>([]);

  useEffect(() => {
    client.get(`/api/preview/${jobId}`).then((res) => {
      setHeaders(res.data.headers as string[]);
      setRows(res.data.rows as Array<Record<string, unknown>>);
    });
  }, [jobId]);

  const columnHelper = createColumnHelper<Record<string, unknown>>();
  const columns = useMemo(
    () => headers.map((header) => columnHelper.accessor(header, { header, cell: (info) => String(info.getValue() ?? '') })),
    [columnHelper, headers]
  );

  const table = useReactTable({ data: rows, columns, getCoreRowModel: getCoreRowModel() });

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Data Preview</h2>
      <div className="overflow-x-auto rounded border">
        <table className="min-w-full text-sm">
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id}>{hg.headers.map((h) => <th key={h.id} className="border-b p-2 text-left">{flexRender(h.column.columnDef.header, h.getContext())}</th>)}</tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.slice(0, 10).map((r) => (
              <tr key={r.id}>{r.getVisibleCells().map((c) => <td key={c.id} className="border-b p-2">{flexRender(c.column.columnDef.cell, c.getContext())}</td>)}</tr>
            ))}
          </tbody>
        </table>
      </div>
      <button className="rounded bg-blue-600 px-4 py-2 text-white" onClick={() => navigate(`/mapper/${jobId}`)}>Map Columns</button>
    </div>
  );
}
