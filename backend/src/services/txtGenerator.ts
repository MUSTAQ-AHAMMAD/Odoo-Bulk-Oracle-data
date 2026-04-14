import fs from 'node:fs/promises';

export const generateTXT = async (text: string, outputPath: string): Promise<void> => {
  await fs.writeFile(outputPath, text, 'utf8');
};
