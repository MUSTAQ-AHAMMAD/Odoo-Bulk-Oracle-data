import fs from 'node:fs/promises';

export const generateHTML = async (html: string, outputPath: string): Promise<void> => {
  await fs.writeFile(outputPath, html, 'utf8');
};
