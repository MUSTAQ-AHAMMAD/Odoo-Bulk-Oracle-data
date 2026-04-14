import fs from 'node:fs/promises';
import path from 'node:path';
import { Worker, type Job as BullJob } from 'bullmq';
import IORedis from 'ioredis';
import { env } from '../config/env';
import { prisma } from '../prisma/client';
import { parseFileInChunks, getTotalRows, type ParsedRow } from '../services/fileParser';
import { compileTemplate, renderRow } from '../services/templateEngine';
import { generateHTML } from '../services/htmlGenerator';
import { generateTXT } from '../services/txtGenerator';
import { generatePDF } from '../services/pdfGenerator';
import { buildZip } from '../services/zipBuilder';
import { emitProgress } from '../websocket/wsServer';

type Payload = { jobId: string };
type Mapping = Record<string, string>;

type ErrorRow = { rowNumber: number; message: string };

const connection = new IORedis(env.REDIS_URL, { maxRetriesPerRequest: null });

const mapRow = (row: ParsedRow, mapping: Mapping): Record<string, unknown> => {
  const out: Record<string, unknown> = {};
  Object.entries(mapping).forEach(([fileColumn, placeholder]) => {
    out[placeholder] = row[fileColumn] ?? null;
  });
  return out;
};

const writeOutput = async (outputType: string, content: string, targetPath: string): Promise<void> => {
  if (outputType === 'pdf') return generatePDF(content, targetPath);
  if (outputType === 'txt') return generateTXT(content, targetPath);
  return generateHTML(content, targetPath);
};

const processJob = async (jobId: string): Promise<void> => {
  const dbJob = await prisma.job.findUnique({ where: { id: jobId } });
  if (!dbJob || !dbJob.templateId || !dbJob.mapping) throw new Error('Job, template, or mapping missing');

  const template = await prisma.template.findFirst({ where: { id: dbJob.templateId, deletedAt: null } });
  if (!template) throw new Error('Template not found');

  const totalRows = await getTotalRows(dbJob.filePath);
  await prisma.job.update({ where: { id: jobId }, data: { status: 'processing', progress: 0, totalRows } });

  const compiler = compileTemplate(template.content);
  const mapping = dbJob.mapping as Mapping;
  const errorRows: ErrorRow[] = [];
  let processed = 0;
  let successRows = 0;
  let failedRows = 0;
  let latestProgress = 0;

  const tempDir = path.resolve(process.cwd(), 'outputs', `${jobId}-tmp`);
  await fs.mkdir(tempDir, { recursive: true });

  await parseFileInChunks(dbJob.filePath, 1000, async (rows) => {
    for (const row of rows) {
      processed += 1;
      try {
        const rendered = renderRow(compiler, mapRow(row, mapping));
        const ext = template.outputType === 'pdf' ? 'pdf' : template.outputType === 'txt' ? 'txt' : 'html';
        await writeOutput(template.outputType, rendered, path.join(tempDir, `${processed}.${ext}`));
        successRows += 1;
      } catch (error) {
        failedRows += 1;
        errorRows.push({
          rowNumber: processed,
          message: error instanceof Error ? error.message : 'Unknown row error'
        });
      }
      const progress = totalRows > 0 ? Math.floor((processed / totalRows) * 100) : 100;
      if (progress > latestProgress) {
        latestProgress = progress;
        await prisma.job.update({ where: { id: jobId }, data: { progress, status: 'processing', successRows, failedRows } });
        emitProgress(jobId, progress, 'processing');
      }
    }
  });

  const zipPath = path.resolve(process.cwd(), 'outputs', `${jobId}.zip`);
  await buildZip(tempDir, zipPath);
  await fs.rm(tempDir, { recursive: true, force: true });

  await prisma.job.update({
    where: { id: jobId },
    data: {
      status: 'completed',
      progress: 100,
      successRows,
      failedRows,
      outputUrl: zipPath,
      errorRows: errorRows.length > 0 ? errorRows : null
    }
  });
  emitProgress(jobId, 100, 'completed');
};

export const generationWorker = new Worker<Payload>(
  'generation',
  async (job: BullJob<Payload>) => {
    try {
      await processJob(job.data.jobId);
    } catch (error) {
      await prisma.job.update({
        where: { id: job.data.jobId },
        data: {
          status: 'failed',
          errorRows: [{ rowNumber: 0, message: error instanceof Error ? error.message : 'Job failed' }]
        }
      });
      emitProgress(job.data.jobId, 0, 'failed');
      throw error;
    }
  },
  { connection }
);
