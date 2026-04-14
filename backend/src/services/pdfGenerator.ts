import puppeteer, { type Browser } from 'puppeteer';

let browserPromise: Promise<Browser> | null = null;

const getBrowser = async (): Promise<Browser> => {
  if (!browserPromise) browserPromise = puppeteer.launch({ headless: 'new' });
  return browserPromise;
};

export const generatePDF = async (html: string, outputPath: string): Promise<void> => {
  const browser = await getBrowser();
  const page = await browser.newPage();
  await page.setContent(html, { waitUntil: 'networkidle0' });
  await page.pdf({ path: outputPath, format: 'A4', printBackground: true });
  await page.close();
};

export const closePDFBrowser = async (): Promise<void> => {
  if (browserPromise) {
    const browser = await browserPromise;
    await browser.close();
    browserPromise = null;
  }
};
