import type { Server as HttpServer } from 'node:http';
import { WebSocketServer, type WebSocket } from 'ws';

const listeners = new Map<string, Set<WebSocket>>();

export const subscribeToJob = (ws: WebSocket, jobId: string): void => {
  const set = listeners.get(jobId) ?? new Set<WebSocket>();
  set.add(ws);
  listeners.set(jobId, set);
  ws.on('close', () => {
    const current = listeners.get(jobId);
    current?.delete(ws);
    if (current && current.size === 0) listeners.delete(jobId);
  });
};

export const emitProgress = (jobId: string, progress: number, status: string): void => {
  const subscribers = listeners.get(jobId);
  if (!subscribers) return;
  const data = JSON.stringify({ jobId, progress, status });
  subscribers.forEach((ws) => {
    if (ws.readyState === ws.OPEN) ws.send(data);
  });
};

export const initWebSocketServer = (server: HttpServer): WebSocketServer => {
  const wss = new WebSocketServer({ server, path: '/api/ws/jobs' });
  wss.on('connection', (ws, req) => {
    const url = new URL(req.url ?? '', 'http://localhost');
    const jobId = url.searchParams.get('jobId');
    if (!jobId) {
      ws.close(1008, 'jobId required');
      return;
    }
    subscribeToJob(ws, jobId);
  });
  return wss;
};
