import { Router, type Request, type Response } from 'express';
import { z } from 'zod';
import { prisma } from '../prisma/client';
import { extractPlaceholders } from '../services/templateEngine';

const router = Router();

const schema = z.object({
  name: z.string().min(1),
  content: z.string().min(1),
  outputType: z.enum(['html', 'pdf', 'txt'])
});

router.post('/', async (req: Request, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
    const data = schema.parse(req.body);
    const template = await prisma.template.create({
      data: {
        userId: req.user.userId,
        name: data.name,
        content: data.content,
        outputType: data.outputType,
        placeholders: extractPlaceholders(data.content)
      }
    });
    return res.status(201).json(template);
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Template create failed' });
  }
});

router.get('/', async (req: Request, res: Response) => {
  if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
  const items = await prisma.template.findMany({ where: { userId: req.user.userId, deletedAt: null }, orderBy: { createdAt: 'desc' } });
  return res.json(items);
});

router.put('/:id', async (req: Request, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
    const data = schema.parse(req.body);
    const result = await prisma.template.updateMany({
      where: { id: req.params.id, userId: req.user.userId, deletedAt: null },
      data: {
        name: data.name,
        content: data.content,
        outputType: data.outputType,
        placeholders: extractPlaceholders(data.content)
      }
    });
    if (!result.count) return res.status(404).json({ message: 'Template not found' });
    return res.json({ message: 'Updated' });
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Template update failed' });
  }
});

router.delete('/:id', async (req: Request, res: Response) => {
  if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
  const result = await prisma.template.updateMany({
    where: { id: req.params.id, userId: req.user.userId, deletedAt: null },
    data: { deletedAt: new Date() }
  });
  if (!result.count) return res.status(404).json({ message: 'Template not found' });
  return res.json({ message: 'Template deleted' });
});

export default router;
