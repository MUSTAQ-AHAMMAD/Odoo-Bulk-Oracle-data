# Full-Stack CRM Bulk Mapping Application

A production-oriented full-stack CRM app for uploading CSV/Excel files, mapping columns to template placeholders, and generating bulk outputs (HTML/PDF/TXT) as downloadable ZIP archives.

## Tech Stack
- Backend: Node.js, Express, TypeScript, Prisma, PostgreSQL, BullMQ, Redis, ws
- Frontend: React, Vite, Tailwind CSS, React Router v6
- Parsing/Generation: multer, papaparse, xlsx, puppeteer, archiver

## Prerequisites
- Node.js 20+
- Docker + Docker Compose

## Setup
1. Start dependencies:
   ```bash
   docker-compose up -d
   ```
2. Install dependencies:
   ```bash
   cd backend && npm install
   cd ../frontend && npm install
   ```
3. Configure env files:
   ```bash
   cp backend/.env.example backend/.env
   cp frontend/.env.example frontend/.env
   ```
4. Push database schema:
   ```bash
   cd backend && npm run db:push
   ```
5. Run apps:
   ```bash
   cd backend && npm run dev
   cd frontend && npm run dev
   ```

## Environment Variables
### backend/.env
- `DATABASE_URL`
- `REDIS_URL`
- `JWT_SECRET`
- `PORT`
- `MAX_FILE_SIZE_MB`
- `MAX_ROWS_PER_JOB`
- `FILE_RETENTION_DAYS`
- `FRONTEND_URL`

### frontend/.env
- `VITE_API_URL`
- `VITE_WS_URL`

## API Endpoints
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/auth/register` | Register user and return JWT |
| POST | `/api/auth/login` | Login and return JWT |
| POST | `/api/upload` | Upload CSV/XLS/XLSX and create job |
| GET | `/api/preview/:jobId` | Get first 100 rows preview |
| POST | `/api/templates` | Create template |
| GET | `/api/templates` | List templates |
| PUT | `/api/templates/:id` | Update template |
| DELETE | `/api/templates/:id` | Soft delete template |
| POST | `/api/map` | Save mapping config |
| POST | `/api/generate/:jobId` | Queue generation job |
| GET | `/api/jobs` | Paginated jobs list |
| GET | `/api/download/:jobId` | Download generated ZIP |

## Architecture (ASCII)
```text
React (Vite + Tailwind)
   | HTTP + WS
Express API (JWT + rate limit)
   | Prisma
PostgreSQL
   | enqueue/dequeue
BullMQ Queue <-> Redis
   |
Worker Processor -> template rendering -> html/pdf/txt files -> ZIP output
```

## User Flow
1. Register/login.
2. Upload CSV/Excel file.
3. Preview data.
4. Create/select template and placeholders.
5. Map columns to placeholders.
6. Trigger generation.
7. Monitor job progress in real-time.
8. Download ZIP result and inspect failed rows.
