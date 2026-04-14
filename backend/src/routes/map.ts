import { Router, type Request, type Response } from 'express';
import { z } from 'zod';
import { prisma } from '../prisma/client';

const router = Router();
const schema = z.object({ templateId: z.string().min(1), columnMappings: z.record(z.string(), z.string()) });

router.post('/', async (req: Request, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
    const data = schema.parse(req.body);
    const saved = await prisma.mappingConfig.create({
      data: { userId: req.user.userId, templateId: data.templateId, columnMappings: data.columnMappings }
    });
    return res.status(201).json(saved);
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Mapping save failed' });
  }
});

export default router;
