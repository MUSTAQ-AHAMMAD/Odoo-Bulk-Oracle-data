import { useEffect, useState } from 'react';

interface ProgressState {
  progress: number;
  status: string;
}

export const useWebSocket = (jobId: string): ProgressState => {
  const [state, setState] = useState<ProgressState>({ progress: 0, status: 'pending' });

  useEffect(() => {
    let retries = 0;
    let socket: WebSocket | null = null;

    const connect = (): void => {
      const base = import.meta.env.VITE_WS_URL ?? 'ws://localhost:3001';
      socket = new WebSocket(`${base}/api/ws/jobs?jobId=${jobId}`);

      socket.onmessage = (event) => {
        const data = JSON.parse(event.data) as ProgressState;
        setState({ progress: data.progress, status: data.status });
      };

      socket.onclose = () => {
        if (retries < 3) {
          retries += 1;
          setTimeout(connect, 1000 * retries);
        }
      };
    };

    connect();
    return () => socket?.close();
  }, [jobId]);

  return state;
};
