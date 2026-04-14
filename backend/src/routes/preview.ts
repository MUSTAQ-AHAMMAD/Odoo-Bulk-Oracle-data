import { Router, type Request, type Response } from 'express';
import { prisma } from '../prisma/client';
import { getPreviewRows } from '../services/fileParser';

const router = Router();

router.get('/:jobId', async (req: Request, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
    const job = await prisma.job.findFirst({ where: { id: req.params.jobId, userId: req.user.userId } });
    if (!job) return res.status(404).json({ message: 'Job not found' });
    const preview = await getPreviewRows(job.filePath, 100);
    return res.json(preview);
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Preview failed' });
  }
});

export default router;
