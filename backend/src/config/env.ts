import { config } from 'dotenv';
import { z } from 'zod';

config();

const schema = z.object({
  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().min(1),
  JWT_SECRET: z.string().min(16),
  PORT: z.coerce.number().int().positive().default(3001),
  MAX_FILE_SIZE_MB: z.coerce.number().positive().default(100),
  MAX_ROWS_PER_JOB: z.coerce.number().positive().default(100000),
  FILE_RETENTION_DAYS: z.coerce.number().positive().default(7),
  FRONTEND_URL: z.string().url().default('http://localhost:5173')
});

const parsed = schema.safeParse(process.env);
if (!parsed.success) {
  throw new Error(`Invalid env: ${parsed.error.message}`);
}

export const env = parsed.data;
