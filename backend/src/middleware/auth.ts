import type { NextFunction, Request, Response } from 'express';
import { verifyToken } from '../utils/jwt';

export const authMiddleware = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const header = req.headers.authorization;
    if (!header?.startsWith('Bearer ')) {
      res.status(401).json({ message: 'Unauthorized' });
      return;
    }
    req.user = verifyToken(header.substring(7));
    next();
  } catch {
    res.status(401).json({ message: 'Invalid or expired token' });
  }
};
