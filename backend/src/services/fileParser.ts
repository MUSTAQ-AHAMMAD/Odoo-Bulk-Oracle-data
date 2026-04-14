import fs from 'node:fs';
import fsp from 'node:fs/promises';
import Papa from 'papaparse';
import xlsx from 'xlsx';

export type ParsedRow = Record<string, string | number | boolean | null>;

const normalize = (row: Record<string, unknown>): ParsedRow => {
  const out: ParsedRow = {};
  Object.entries(row).forEach(([k, v]) => {
    out[k] = v === undefined ? null : (v as string | number | boolean | null);
  });
  return out;
};

export const parseCSV = async (
  filePath: string,
  chunkSize: number,
  callback: (rows: ParsedRow[]) => Promise<void>
): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    const chunk: ParsedRow[] = [];
    const parser = Papa.parse(Papa.NODE_STREAM_INPUT, { header: true, skipEmptyLines: true });
    parser.on('data', (row: Record<string, unknown>) => {
      chunk.push(normalize(row));
      if (chunk.length >= chunkSize) {
        parser.pause();
        callback([...chunk])
          .then(() => {
            chunk.length = 0;
            parser.resume();
          })
          .catch(reject);
      }
    });
    parser.on('end', async () => {
      if (chunk.length) await callback([...chunk]);
      resolve();
    });
    parser.on('error', reject);
    fs.createReadStream(filePath).pipe(parser);
  });

export const parseExcel = async (
  filePath: string,
  chunkSize: number,
  callback: (rows: ParsedRow[]) => Promise<void>
): Promise<void> => {
  const wb = xlsx.readFile(filePath, { cellDates: false });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const rows = xlsx.utils.sheet_to_json<Record<string, unknown>>(sheet, { defval: null }).map(normalize);
  for (let i = 0; i < rows.length; i += chunkSize) {
    await callback(rows.slice(i, i + chunkSize));
  }
};

const isCsv = (filePath: string): boolean => filePath.toLowerCase().endsWith('.csv');

export const parseFileInChunks = async (
  filePath: string,
  chunkSize: number,
  callback: (rows: ParsedRow[]) => Promise<void>
): Promise<void> => {
  if (isCsv(filePath)) return parseCSV(filePath, chunkSize, callback);
  return parseExcel(filePath, chunkSize, callback);
};

export const getPreviewRows = async (
  filePath: string,
  maxRows = 100
): Promise<{ headers: string[]; rows: ParsedRow[] }> => {
  if (isCsv(filePath)) {
    const raw = await fsp.readFile(filePath, 'utf-8');
    const parsed = Papa.parse<Record<string, unknown>>(raw, { header: true, skipEmptyLines: true });
    const rows = parsed.data.slice(0, maxRows).map(normalize);
    return { headers: parsed.meta.fields ?? Object.keys(rows[0] ?? {}), rows };
  }
  const wb = xlsx.readFile(filePath, { cellDates: false });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const rows = xlsx.utils.sheet_to_json<Record<string, unknown>>(sheet, { defval: null }).slice(0, maxRows).map(normalize);
  return { headers: Object.keys(rows[0] ?? {}), rows };
};

export const getTotalRows = async (filePath: string): Promise<number> => {
  if (isCsv(filePath)) {
    const raw = await fsp.readFile(filePath, 'utf-8');
    return Papa.parse<Record<string, unknown>>(raw, { header: true, skipEmptyLines: true }).data.length;
  }
  const wb = xlsx.readFile(filePath, { cellDates: false });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  return xlsx.utils.sheet_to_json(sheet, { defval: null }).length;
};
