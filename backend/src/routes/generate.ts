import { Router, type Request, type Response } from 'express';
import { z } from 'zod';
import { prisma } from '../prisma/client';
import { addJob } from '../workers/queue';

const router = Router();
const schema = z.object({ templateId: z.string().min(1), mapping: z.record(z.string(), z.string()) });

router.post('/:jobId', async (req: Request, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
    const data = schema.parse(req.body);
    const job = await prisma.job.findFirst({ where: { id: req.params.jobId, userId: req.user.userId } });
    if (!job) return res.status(404).json({ message: 'Job not found' });

    await prisma.job.update({
      where: { id: job.id },
      data: { templateId: data.templateId, mapping: data.mapping, status: 'pending', progress: 0 }
    });

    await addJob(job.id);
    return res.status(202).json({ message: 'Generation queued', jobId: job.id });
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Generate failed' });
  }
});

export default router;
