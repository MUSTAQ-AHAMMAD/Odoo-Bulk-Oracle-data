import fs from 'node:fs';
import archiver from 'archiver';

export const buildZip = async (sourceDir: string, outputPath: string): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    const out = fs.createWriteStream(outputPath);
    const zip = archiver('zip', { zlib: { level: 9 } });
    out.on('close', () => resolve());
    zip.on('error', reject);
    zip.pipe(out);
    zip.directory(sourceDir, false);
    zip.finalize().catch(reject);
  });
