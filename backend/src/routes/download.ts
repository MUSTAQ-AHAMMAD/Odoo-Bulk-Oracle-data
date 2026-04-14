import fs from 'node:fs';
import path from 'node:path';
import { Router, type Request, type Response } from 'express';
import { prisma } from '../prisma/client';
import { downloadRateLimiter } from '../middleware/rateLimit';

const router = Router();

router.get('/:jobId', downloadRateLimiter, async (req: Request, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
    const job = await prisma.job.findFirst({ where: { id: req.params.jobId, userId: req.user.userId } });
    if (!job?.outputUrl) return res.status(404).json({ message: 'Output not found' });

    const outputRoot = path.resolve(process.cwd(), 'outputs');
    const filePath = path.resolve(job.outputUrl);
    if (!(filePath === outputRoot || filePath.startsWith(`${outputRoot}${path.sep}`))) {
      return res.status(400).json({ message: 'Invalid output path' });
    }
    if (!fs.existsSync(filePath)) return res.status(404).json({ message: 'Output missing' });
    return res.download(filePath, `${job.id}.zip`);
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Download failed' });
  }
});

export default router;
