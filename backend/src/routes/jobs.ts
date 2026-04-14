import { Router, type Request, type Response } from 'express';
import { prisma } from '../prisma/client';

const router = Router();

router.get('/', async (req: Request, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
    const page = Number(req.query.page ?? 1);
    const limit = Number(req.query.limit ?? 10);
    const skip = (page - 1) * limit;
    const [items, total] = await Promise.all([
      prisma.job.findMany({
        where: { userId: req.user.userId },
        orderBy: { createdAt: 'desc' },
        include: { template: true },
        skip,
        take: limit
      }),
      prisma.job.count({ where: { userId: req.user.userId } })
    ]);
    return res.json({ page, limit, total, items });
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Jobs load failed' });
  }
});

export default router;
