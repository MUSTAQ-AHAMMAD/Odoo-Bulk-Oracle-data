import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import ExcelJS, { type CellValue } from 'exceljs';
import Papa from 'papaparse';

export type ParsedRow = Record<string, string | number | boolean | null>;

const uploadRoot = path.resolve(process.cwd(), 'uploads');
const forbiddenKeys = new Set(['__proto__', 'constructor', 'prototype']);

const resolveUploadPath = (filePath: string): string => {
  const safeName = path.basename(filePath);
  const normalized = path.resolve(uploadRoot, safeName);
  if (!normalized.startsWith(`${uploadRoot}${path.sep}`)) {
    throw new Error('Invalid file path');
  }
  return normalized;
};

const normalize = (row: Record<string, unknown>): ParsedRow => {
  const out = Object.create(null) as ParsedRow;
  Object.entries(row).forEach(([key, value]) => {
    if (!forbiddenKeys.has(key)) {
      out[key] = value === undefined ? null : (value as string | number | boolean | null);
    }
  });
  return out;
};

const toPrimitive = (value: CellValue | undefined): string | number | boolean | null => {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === 'object') {
    if ('text' in value && typeof value.text === 'string') {
      return value.text;
    }
    if ('hyperlink' in value && typeof value.hyperlink === 'string') {
      return value.hyperlink;
    }
    if ('result' in value && (typeof value.result === 'string' || typeof value.result === 'number' || typeof value.result === 'boolean')) {
      return value.result;
    }
  }
  return String(value);
};

const isCsv = (filePath: string): boolean => filePath.toLowerCase().endsWith('.csv');

const readWorksheet = async (filePath: string): Promise<{ worksheet: ExcelJS.Worksheet; headers: string[] }> => {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(resolveUploadPath(filePath));
  const worksheet = workbook.worksheets[0];
  if (!worksheet) {
    throw new Error('Workbook has no worksheets');
  }

  const headerRow = worksheet.getRow(1);
  const headers: string[] = [];
  for (let index = 1; index <= headerRow.cellCount; index += 1) {
    const raw = String(toPrimitive(headerRow.getCell(index).value as CellValue) ?? '').trim();
    const fallback = `Column_${index}`;
    const header = raw.length > 0 ? raw : fallback;
    headers.push(forbiddenKeys.has(header) ? fallback : header);
  }

  return { worksheet, headers };
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
      if (chunk.length > 0) {
        await callback([...chunk]);
      }
      resolve();
    });

    parser.on('error', reject);
    fs.createReadStream(resolveUploadPath(filePath)).pipe(parser);
  });

export const parseExcel = async (
  filePath: string,
  chunkSize: number,
  callback: (rows: ParsedRow[]) => Promise<void>
): Promise<void> => {
  const { worksheet, headers } = await readWorksheet(filePath);
  const chunk: ParsedRow[] = [];

  for (let rowIndex = 2; rowIndex <= worksheet.rowCount; rowIndex += 1) {
    const row = worksheet.getRow(rowIndex);
    const output = Object.create(null) as ParsedRow;
    headers.forEach((header, index) => {
      output[header] = toPrimitive(row.getCell(index + 1).value as CellValue);
    });
    chunk.push(output);

    if (chunk.length >= chunkSize) {
      await callback([...chunk]);
      chunk.length = 0;
    }
  }

  if (chunk.length > 0) {
    await callback([...chunk]);
  }
};

export const parseFileInChunks = async (
  filePath: string,
  chunkSize: number,
  callback: (rows: ParsedRow[]) => Promise<void>
): Promise<void> => {
  if (isCsv(filePath)) {
    await parseCSV(filePath, chunkSize, callback);
    return;
  }
  await parseExcel(filePath, chunkSize, callback);
};

export const getPreviewRows = async (
  filePath: string,
  maxRows = 100
): Promise<{ headers: string[]; rows: ParsedRow[] }> => {
  if (isCsv(filePath)) {
    const raw = await fsp.readFile(resolveUploadPath(filePath), 'utf-8');
    const parsed = Papa.parse<Record<string, unknown>>(raw, { header: true, skipEmptyLines: true });
    const rows = parsed.data.slice(0, maxRows).map(normalize);
    return { headers: parsed.meta.fields ?? Object.keys(rows[0] ?? {}), rows };
  }

  const { worksheet, headers } = await readWorksheet(filePath);
  const rows: ParsedRow[] = [];

  for (let rowIndex = 2; rowIndex <= worksheet.rowCount && rows.length < maxRows; rowIndex += 1) {
    const row = worksheet.getRow(rowIndex);
    const output = Object.create(null) as ParsedRow;
    headers.forEach((header, index) => {
      output[header] = toPrimitive(row.getCell(index + 1).value as CellValue);
    });
    rows.push(output);
  }

  return { headers, rows };
};

export const getTotalRows = async (filePath: string): Promise<number> => {
  if (isCsv(filePath)) {
    const raw = await fsp.readFile(resolveUploadPath(filePath), 'utf-8');
    return Papa.parse<Record<string, unknown>>(raw, { header: true, skipEmptyLines: true }).data.length;
  }

  const { worksheet } = await readWorksheet(filePath);
  return Math.max(worksheet.rowCount - 1, 0);
};
