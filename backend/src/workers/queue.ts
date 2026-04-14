import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { env } from '../config/env';

const connection = new IORedis(env.REDIS_URL, { maxRetriesPerRequest: null });

export const generationQueue = new Queue('generation', { connection });

export const addJob = async (jobId: string): Promise<void> => {
  await generationQueue.add('generate', { jobId });
};
