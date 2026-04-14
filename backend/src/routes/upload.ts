import path from 'node:path';
import { Router, type Request, type Response } from 'express';
import multer from 'multer';
import { env } from '../config/env';
import { prisma } from '../prisma/client';
import { getPreviewRows } from '../services/fileParser';
import { uploadRateLimiter } from '../middleware/rateLimit';

const router = Router();
const uploadDir = path.resolve(process.cwd(), 'uploads');

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`)
});

const allowed = new Set([
  'text/csv',
  
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
]);

const upload = multer({
  storage,
  limits: { fileSize: env.MAX_FILE_SIZE_MB * 1024 * 1024 },
  fileFilter: (_req, file, cb) => (allowed.has(file.mimetype) ? cb(null, true) : cb(new Error('Invalid file type')))
});

router.post('/', uploadRateLimiter, upload.single('file'), async (req: Request, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ message: 'Unauthorized' });
    if (!req.file) return res.status(400).json({ message: 'File is required' });

    const preview = await getPreviewRows(req.file.path, 5);
    const job = await prisma.job.create({
      data: {
        userId: req.user.userId,
        fileName: req.file.originalname,
        filePath: req.file.path,
        status: 'pending'
      }
    });

    return res.status(201).json({ jobId: job.id, columns: preview.headers });
  } catch (error) {
    return res.status(400).json({ message: error instanceof Error ? error.message : 'Upload failed' });
  }
});

export default router;
