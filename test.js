const puppeteer = require('puppeteer');

(async () => {
    // Lanzamos navegador
    const browser = await puppeteer.launch({ headless: "shell" });
    const page = await browser.newPage();
    await page.goto('https://example.com');
    console.log(await page.title()); // Debería imprimir "Example Domain"
    await browser.close();
})();
