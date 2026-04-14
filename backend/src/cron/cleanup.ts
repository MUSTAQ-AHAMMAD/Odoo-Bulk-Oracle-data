import fs from 'node:fs/promises';
import cron from 'node-cron';
import { env } from '../config/env';
import { prisma } from '../prisma/client';

const cleanup = async (): Promise<void> => {
  const cutoff = new Date(Date.now() - env.FILE_RETENTION_DAYS * 24 * 60 * 60 * 1000);
  const jobs = await prisma.job.findMany({
    where: {
      createdAt: { lt: cutoff },
      status: { not: 'processing' }
    }
  });
  for (const job of jobs) {
    for (const file of [job.filePath, job.outputUrl].filter((value): value is string => Boolean(value))) {
      try {
        await fs.unlink(file);
      } catch {
        // ignore if missing
      }
    }
  }
};

export const startCleanupCron = (): void => {
  cron.schedule('0 0 * * *', () => {
    void cleanup();
  });
};
