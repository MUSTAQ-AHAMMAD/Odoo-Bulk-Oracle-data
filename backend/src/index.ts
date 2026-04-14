import fs from 'node:fs/promises';
import http from 'node:http';
import express from 'express';
import cors from 'cors';
import { env } from './config/env';
import { authMiddleware } from './middleware/auth';
import { errorHandler } from './middleware/errorHandler';
import authRoutes from './routes/auth';
import uploadRoutes from './routes/upload';
import previewRoutes from './routes/preview';
import templatesRoutes from './routes/templates';
import mapRoutes from './routes/map';
import generateRoutes from './routes/generate';
import jobsRoutes from './routes/jobs';
import downloadRoutes from './routes/download';
import { initWebSocketServer } from './websocket/wsServer';
import { generationWorker } from './workers/processor';
import { startCleanupCron } from './cron/cleanup';
import { closePDFBrowser } from './services/pdfGenerator';

const start = async (): Promise<void> => {
  await fs.mkdir('uploads', { recursive: true });
  await fs.mkdir('outputs', { recursive: true });

  const app = express();
  app.use(cors({ origin: env.FRONTEND_URL }));
  app.use(express.json({ limit: '10mb' }));

  app.use('/api/auth', authRoutes);
  app.use('/api', authMiddleware);
  app.use('/api/upload', uploadRoutes);
  app.use('/api/preview', previewRoutes);
  app.use('/api/templates', templatesRoutes);
  app.use('/api/map', mapRoutes);
  app.use('/api/generate', generateRoutes);
  app.use('/api/jobs', jobsRoutes);
  app.use('/api/download', downloadRoutes);

  app.get('/api/health', (_req, res) => res.json({ ok: true }));
  app.use(errorHandler);

  const server = http.createServer(app);
  initWebSocketServer(server);
  startCleanupCron();

  server.listen(env.PORT, () => {
    console.log(`API listening on ${env.PORT}`);
  });

  const close = async (): Promise<void> => {
    await generationWorker.close();
    await closePDFBrowser();
    server.close(() => process.exit(0));
  };

  process.on('SIGINT', () => {
    void close();
  });
  process.on('SIGTERM', () => {
    void close();
  });
};

void start();
