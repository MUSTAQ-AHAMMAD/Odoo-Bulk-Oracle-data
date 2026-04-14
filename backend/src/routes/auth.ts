import { Router, type Request, type Response } from 'express';
import bcrypt from 'bcryptjs';
import { z } from 'zod';
import { prisma } from '../prisma/client';
import { signToken } from '../utils/jwt';

const router = Router();

const schema = z.object({ email: z.string().email(), password: z.string().min(8) });

router.post('/register', async (req: Request, res: Response) => {
  try {
    const input = schema.parse(req.body);
    const existing = await prisma.user.findUnique({ where: { email: input.email } });
    if (existing) return res.status(409).json({ message: 'Email already exists' });

    const user = await prisma.user.create({
      data: { email: input.email, passwordHash: await bcrypt.hash(input.password, 10) }
    });
    return res.status(201).json({ token: signToken({ userId: user.id, email: user.email, role: user.role }) });
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Registration failed' });
  }
});

router.post('/login', async (req: Request, res: Response) => {
  try {
    const input = schema.parse(req.body);
    const user = await prisma.user.findUnique({ where: { email: input.email } });
    if (!user) return res.status(401).json({ message: 'Invalid credentials' });
    const ok = await bcrypt.compare(input.password, user.passwordHash);
    if (!ok) return res.status(401).json({ message: 'Invalid credentials' });
    return res.json({ token: signToken({ userId: user.id, email: user.email, role: user.role }) });
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Login failed' });
  }
});

export default router;
