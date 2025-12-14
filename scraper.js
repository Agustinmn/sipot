// scraper.js - Versión Final (Selector de Modal Blindado con normalize-space)
const puppeteer = require('puppeteer-extra')
const StealthPlugin = require('puppeteer-extra-plugin-stealth')
puppeteer.use(StealthPlugin())

const fs = require('fs')
const { promisify } = require('util')
const exists = promisify(fs.stat)
const path = require('path')

let didReload = false
const downloadsInProgress = []
const fromTargetUrl = res => res.url().endsWith('consultaPublica.xhtml')

// Secuencia de navegación
const sequence = ['inicio', 'sujetosObligados', 'obligaciones', 'tarjetaInformativa']

async function backTo (page, nextLocation) {
  const url = page.url()
  const [base, target] = url.split('#')
  const nextLocationIndex = sequence.indexOf(nextLocation)
  const targetIndex = sequence.indexOf(target)

  if (nextLocationIndex - targetIndex >= 0) return true

  const navigationSteps = sequence.slice(nextLocationIndex, targetIndex).reverse()
  for (let i in navigationSteps) {
    await page.goto(`${base}#${navigationSteps[i]}`)
  }
}

async function takeTo (page, nextLocation, stateCode, params) {
  const { organizationName, organizationIndex, year } = params
  const url = page.url()
  const [base, target] = url.split('#')
  const nextLocationIndex = sequence.indexOf(nextLocation)
  const targetIndex = sequence.indexOf(target)

  if (nextLocationIndex - targetIndex <= 0) return await backTo(page, nextLocation)

  const steps = sequence.slice(targetIndex + 1, nextLocationIndex + 1)
  for (let i in steps) {
    const step = steps[i]
    console.log(`navegando a #${step}`)
    switch (step) {
      case 'sujetosObligados': await navigateToOrganizations(page, stateCode); break;
      case 'obligaciones': await navigateToObligations(page, organizationName, organizationIndex, year); break;
      case 'tarjetaInformativa': await navigateToInformationCard(page, year); break;
    }
  }
}

/**
 * FASE 1: NAVEGACIÓN LIMPIA
 */
async function navigateToObligations (page, organizationName = null, organizationIndex = 0, year = 2024) {
  console.log(`\n--- Buscando Institución: ${organizationName} (Año: ${year}) ---`);

  const institutionDropdown = await page.waitForSelector('#tooltipInst > div > button');
  await institutionDropdown.click();

  const dropdownOrg = await page.$x(`//a/span[contains(text(), '${organizationName}')]`);
  if (!dropdownOrg.length) throw new Error(`❌ No encontramos la institución '${organizationName}'`);
  
  await dropdownOrg[0].click();
  console.log(" Institución seleccionada.");
  await page.waitForTimeout(1500);

  console.log(`Seleccionando año ${year}...`);
  try {
      const yearSelector = 'select[id*="cboEjercicio"]';
      await page.waitForSelector(yearSelector, { timeout: 5000 });
      await page.select(yearSelector, String(year));
      console.log(" Año seleccionado.");
  } catch (e) {
      console.log(" No pude seleccionar el año.");
  }

  try { await page.waitForSelector('div.capaBloqueaPantalla', { hidden: true, timeout: 5000 }); } catch(e) {}

  console.log(" Esperando que carguen las carpetas automáticas...");
  await page.waitForSelector('div.tituloObligacion', { timeout: 60000 });
  console.log(" Carpetas detectadas.");
}

/**
 * FASE 2: LÓGICA DE DESCARGA (AJUSTADA AL HTML DEL MODAL)
 */
async function getContract (page, organizationName = null, organizationIndex = 0, year = 2021, type) {
  console.log("\n Iniciando Fase 2: Configuración de Consulta...");

  // --- 1. VERIFICACIÓN DE ZONA ---
  try {
    await page.waitForXPath('//label[contains(text(), "Periodo de actualización")]', { visible: true, timeout: 20000 });
  } catch (e) {
    throw new Error(" Error Crítico: No veo el formulario de consulta.");
  }
  await page.waitForTimeout(1500);

  // --- 2. SELECCIONAR TRIMESTRES ---
  console.log(" Seleccionando trimestres...");
  const selectAllPeriods = await page.$x('//input[@value="99" and contains(@id, "checkPeriodos")]');
  
  if (selectAllPeriods.length > 0) {
      for(let intento = 1; intento <= 3; intento++) {
          const isChecked = await page.evaluate(el => el.checked, selectAllPeriods[0]);
          if (isChecked) {
              console.log("✅ Trimestres marcados.");
              break;
          }
          const parentLabel = await page.$x('//label[contains(@for, "checkPeriodos") and contains(text(), "Seleccionar todos")]');
          if (parentLabel.length > 0) await parentLabel[0].click();
          else await selectAllPeriods[0].click();
          
          await page.waitForTimeout(1000);
      }
  } else {
      const allPeriodChecks = await page.$x('//input[contains(@id, "checkPeriodos")]');
      for (const check of allPeriodChecks) { await check.click(); await page.waitForTimeout(100); }
  }
  
  try { await page.waitForSelector('div.capaBloqueaPantalla', { hidden: true, timeout: 5000 }); } catch(e) {}


  // --- 3. CONSULTAR ---
  console.log("🔎 Presionando CONSULTAR...");
  const queryButton = await page.$x('//a[contains(text(), "CONSULTAR") or contains(text(), "Consultar")]');
  if (queryButton.length === 0) throw new Error("No encuentro el botón CONSULTAR");
  
  await queryButton[0].click();


  // --- 4. ESPERAR Y VALIDAR RESULTADOS ---
  console.log(" Esperando resultados válidos (mayores a 0)...");
  
  try {
      await page.waitForSelector('#itTotalResultados', { visible: true, timeout: 180000 });
  } catch(e) {
      console.log(" ERROR: El contador nunca apareció.");
      throw new Error("ABORTANDO: Consulta fallida.");
  }

  let totalResults = 0;
  let intentosValidacion = 0;
  const maxIntentos = 24; 

  while (intentosValidacion < maxIntentos) {
      const resultsText = await page.$eval('#itTotalResultados', el => el.innerText);
      totalResults = parseInt(resultsText.replace(/,/g, ''), 10);
      
      console.log(`   Lectura #${intentosValidacion + 1}: ${totalResults} resultados.`);

      if (totalResults > 0) {
          console.log(" ¡Confirmado! Hay datos.");
          break;
      }

      console.log("   ... cargando (0) ... Esperando 5 segundos ...");
      await page.waitForTimeout(5000); 
      intentosValidacion++;
  }

  if (totalResults === 0) {
      console.log(" CONFIRMADO: El resultado final es 0.");
      return false; 
  }

  // --- 5. DESCARGAR ---
  console.log(`⬇️ Iniciando DESCARGA de ${totalResults} registros...`);
  
  const downloadBtnXPath = '//a[contains(@id, "formDescargaArchivos") and contains(text(), "DESCARGAR")]';
  
  try {
    await page.waitForXPath(downloadBtnXPath, { visible: true, timeout: 20000 });
    const downloadButton = await page.$x(downloadBtnXPath);
    await downloadButton[0].click();
    console.log(" Click en DESCARGAR realizado.");
  } catch (e) {
    console.log(" ERROR RARO: Hay resultados > 0 pero no aparece el botón.");
    return false;
  }

  // === AQUÍ EMPIEZA LA CORRECCIÓN DEL MODAL CON TU HTML ===
  console.log("⏳ Abriendo modal y buscando pestaña de descarga...");
  
  // Pausa para animación
  await page.waitForTimeout(3000);

  // Selector blindado que ignora espacios en blanco y busca la clase simulalink
  // HTML: <label class="cursor-pointer simulalink"> Descargar </label>
  const modalLabelXPath = '//label[contains(@class, "simulalink") and contains(normalize-space(.), "Descargar")]';

  try {
    await page.waitForXPath(modalLabelXPath, { visible: true, timeout: 15000 });
    const downloadLabel = await page.$x(modalLabelXPath);
    
    // Es CRUCIAL dar este click para que cargue el contenido siguiente
    await downloadLabel[0].click();
    console.log(" Pestaña 'Descargar' activada dentro del modal.");
    
  } catch(e) {
    console.log(" ERROR: No pude dar click a la pestaña 'Descargar' en el modal.");
    console.log("   Sin este click, el menú de Excel no aparecerá.");
    throw e; // Detenemos aquí porque si esto falla, lo siguiente fallará seguro
  }

  // Esperar a que el click anterior surta efecto (AJAX)
  await page.waitForTimeout(2000);

  // 3. Buscar el botón desplegable
  console.log("   Buscando menú de rangos...");
  const dropdownXPath = '//button[contains(@data-id, "formModalRangos:rangoExcel")]';
  
  try {
      await page.waitForXPath(dropdownXPath, { visible: true, timeout: 15000 });
      const dropdown = await page.$x(dropdownXPath);
      
      // Esperar a que el Select interno se pueble
      await page.waitForTimeout(1000);
      
      // Obtener opciones
      const options = await page.$x('//select[@id="formModalRangos:rangoExcel"]/option');

      console.log(` Encontré ${options.length} opciones de descarga.`);

      for (let i in options) {
        const [text, value] = await options[i].evaluate(node => [node.text, node.value]);
        if (value === '-1') continue;

        console.log(`   ⬇ Descargando parte: ${text}`);
        
        await dropdown[0].click();
        await page.waitForTimeout(500);

        const optionSpan = await page.$x(`//a/span[contains(text(), "${text}")]`);
        if (optionSpan.length > 0) {
            await optionSpan[0].click();
        } 
        
        await page.waitForTimeout(500);

        const downloadExcel = await page.waitForXPath('//input[@id="formModalRangos:btnDescargaExcel"]');
        await downloadExcel.click();

        try {
            await page.waitForResponse(async r => {
                return r.status() === 200 && (fromTargetUrl(r) || r.headers()['content-type'].includes('excel') || r.headers()['content-type'].includes('spreadsheet'));
            }, { timeout: 120000 });
        } catch(e) {
            console.log("   (La descarga se inició, avanzando...)");
        }

        if (didReload === true) {
          const continuar = await page.waitForSelector('#modalCSV > div > div > div > div:nth-child(2) > div > button');
          if(continuar) await continuar.evaluate(b => b.click());
          didReload = false;
          return true;
        }
        
        await page.waitForTimeout(3000);
      }
      
      const modal = await page.waitForSelector('#modalRangos');
      await modal.evaluate(b => b.click());

  } catch (e) {
      console.error(" Error dentro del modal de descarga:", e.message);
  }

  try { await page.waitForSelector('div.capaBloqueaPantalla', { hidden: true, timeout: 5000 }); } catch(e) {}

  return true;
}

async function navigateToInformationCard (page, year = 2021) {
  console.log("--> Buscando sección de contratos...");
  try { await page.waitForXPath('//form[@id="formListaObligaciones"]', { timeout: 30000 }); } catch (e) {}

  try { await page.waitForSelector('div.capaBloqueaPantalla', { hidden: true, timeout: 5000 }); } catch(e) {}
  
  const noContractsPopup = await page.$x('//div[contains(@id, "modalSinObligaciones") and contains(@style, "display: block")]');
  if (noContractsPopup.length) {
    await noContractsPopup[0].click();
    await page.waitForTimeout(1000);
  }

  const targetText = "CONTRATOS DE OBRAS"; 
  const xpath = `//label[contains(translate(text(), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), '${targetText}')]`;

  try {
      await page.waitForXPath(xpath, { timeout: 10000 });
  } catch(e) {
      throw new Error(`No se encontró la opción de Contratos.`);
  }

  const contractsLabel = await page.$x(xpath);
  await contractsLabel[0].click();
  console.log("✅ Entrando a carpeta de Contratos...");

  // Re-verificar año interno
  const periodSelector = '//select[contains(@id, "cboEjercicio")]';
  try {
      await page.waitForXPath(periodSelector, {timeout: 5000});
      const periodElement = await page.$x(periodSelector);
      const currentYear = await page.evaluate(el => el.value, periodElement[0]);

      if (currentYear != year) {
          console.log(`Ajustando año interno a ${year}...`);
          await page.select('select[id*="cboEjercicio"]', String(year));
          try { await page.waitForSelector('div.capaBloqueaPantalla', { hidden: true, timeout: 5000 }); } catch(e) {}
          await page.waitForTimeout(1000);
          
          const reClickLabel = await page.$x(xpath);
          if (reClickLabel.length > 0) await reClickLabel[0].click();
      }
  } catch(e) {}
}

function responseHandler (res, dest_dir) {
  if (fromTargetUrl(res)) {
    const headers = res.headers()
    if (headers['content-type'] === 'application/vnd.ms-excel' || (headers['content-type'] && headers['content-type'].includes('spreadsheet'))) {
      didReload = false
      const match = headers['content-disposition'] ? headers['content-disposition'].match(/filename\="(.*)"/) : []
      const filename = match && match[1] ? match[1] : `descarga_${Date.now()}.xls`
      console.log('📦 Archivo detectado:', filename)
      downloadsInProgress.push(toDownload(filename, dest_dir))
      return filename
    } else if (((headers['cache-control'] || '') != 'no-cache') && ((headers['content-length'] || '0') === '0') && ((headers['set-cookie'] || '').endsWith('path=/'))) {
      didReload = true
    }
  }
  return null
}

async function getPage (browser, opts) {
  const page = await browser.newPage()
  const timeout = opts.timeout || 60000
  const dest_dir = opts.downloads_dir

  page.waitForTimeout = function (ms) { return new Promise(resolve => setTimeout(resolve, ms)); };
  page.$x = async function(expression) { return await page.$$('xpath/' + expression); };
  page.waitForXPath = async function(expression, options) { return await page.waitForSelector('xpath/' + expression, options); };
  
  const client = await page.createCDPSession()
  await client.send('Page.setDownloadBehavior', { behavior: 'allow', downloadPath: dest_dir })

  await page.setRequestInterception(true)
  page.on('request', interceptedRequest => {
    if (['.jpg', '.png', '.svg', '.gif'].some(ext => interceptedRequest.url().endsWith(ext))) {
      interceptedRequest.abort()
    } else {
      interceptedRequest.continue()
    }
  })

  await page.setViewport({ width: 1200, height: 1000 })
  page.setDefaultTimeout(timeout)
  page.on('response', (response) => responseHandler(response, dest_dir))
  return page
}

async function navigateToOrganizations (page, stateCode) {
  console.log("--> Iniciando transición a Sujetos Obligados...")

  if (!page.url().includes('consultaPublica.xhtml')) {
      await page.goto('https://consultapublicamx.plataformadetransparencia.org.mx/vut-web/faces/view/consultaPublica.xhtml#sujetosObligados', { waitUntil: 'domcontentloaded' });
  } else if (!page.url().includes('#sujetosObligados')) {
      await page.evaluate(() => window.location.hash = '#sujetosObligados');
      await page.waitForTimeout(1000);
  }

  try {
    const blocker = await page.$('div.capaBloqueaPantalla');
    if (blocker) {
        await page.waitForSelector('div.capaBloqueaPantalla', { hidden: true, timeout: 30000 }).catch(()=>console.log("Blocker timeout (ignorable)"));
    }

    console.log("    Buscando filtro de Estado...");
    const dropdownXPath = '//button[contains(., "Selecciona") or contains(., "Federación") or contains(., "Estado")]';
    await page.waitForXPath(dropdownXPath, { visible: true, timeout: 60000 });
    
    const [dropdownBtn] = await page.$x(dropdownXPath);
    if (dropdownBtn) await dropdownBtn.click();

    console.log(`    Seleccionando estado ID: ${stateCode}...`);
    const listXPath = '//div[contains(@class, "btn-group") and contains(@class, "open")]//ul';
    await page.waitForXPath(listXPath, { visible: true });

    const optionXPath = `${listXPath}/li[${stateCode + 1}]/a`;
    const [stateOption] = await page.$x(optionXPath);
    
    if (stateOption) await stateOption.click();

    await page.waitForTimeout(1000);
    try { await page.waitForSelector('div.capaBloqueaPantalla', { hidden: true, timeout: 10000 }); } catch(e) {}
    console.log("--> Estado seleccionado correctamente.");

  } catch (e) {
    console.error("!!! ERROR EN NAVEGACIÓN !!!", e.message);
    throw e;
  }
}

async function selectNextOrganization (page, orgId) { return; }

async function startBrowser (params) {
  console.log("🔌 Intentando conectar a Chrome existente en puerto 9222...");
  try {
    const browser = await puppeteer.connect({
        browserURL: 'http://127.0.0.1:9222',
        defaultViewport: null
    });
    console.log(" ¡Conexión exitosa al navegador!");
    return browser;
  } catch (e) {
    console.error(" No se pudo conectar a Chrome.");
    throw e;
  }
}

function toDownload (filename, dest_dir, timeoutSeconds = 60, intervalSeconds = 1) {
  return new Promise((resolve, reject) => {
    let interval
    let timeout
    const filepath = path.join(dest_dir, filename)

    timeout = setTimeout(() => {
      clearInterval(interval)
      const error = `No hemos podido descargar ${filename} en menos de 60s`
      console.log(error)
      return reject(error)
    }, timeoutSeconds * 1000)

    interval = setInterval(async () => {
      try {
        await exists(filepath)
        clearTimeout(timeout)
        clearInterval(interval)
        const success = `Se ha descargado ${filename}`
        console.log(success)
        return resolve(success)
      } catch (e) {}
    }, intervalSeconds * 1000)
  })
}

module.exports = {
  backTo,
  downloadsInProgress,
  getContract,
  getPage,
  navigateToOrganizations,
  selectNextOrganization,
  startBrowser,
  takeTo,
  toDownload
}