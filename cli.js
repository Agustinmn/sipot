#!/usr/bin/env node
// cli.js - Versión Final para Remote Debugging (Reutiliza pestaña)

const buildOptions = require('minimist-options');

const options = buildOptions({
  downloads_dir: {
    type: 'string',
    default: process.cwd()
  },
  state: {
    type: 'number',
    default: 1
  },
  year: {
    type: 'number',
    default: 2021
  }
});

const argv = require('minimist')(process.argv.slice(2), options)
const fs = require('fs')
const path = require('path')
const scraper = require('./scraper') // Asegúrate de que scraper.js tenga la función connect()

const { promisify } = require('util')

const organization = argv.organization
const organizationList = argv.organizationList
const from = Number(argv.from || 0)
const to = Number(argv.to || 965)
const year = argv.year
const type = Number(argv.type)
const stateCode = argv.state

const startUrl = 'https://consultapublicamx.plataformadetransparencia.org.mx/vut-web/faces/view/consultaPublica.xhtml'

;(async () => {
  console.log('Nueva sesión', new Date())

  let organizations
  if (organizationList) {
    const read = promisify(fs.readFile)
    const orgData = await read(organizationList)
    try {
      organizations = JSON.parse(orgData.toString())
    } catch (e) {
      organizations = orgData.toString().split('\n')
    }
    console.log(`Se encontraron ${organizations.length} organizaciones en ${organizationList}`)
  }

  // 1. Conectarse al navegador existente
  const browser = await scraper.startBrowser({ development: !!argv.development })

  try {
    // 2. BUSCAR LA PESTAÑA ABIERTA (La clave del éxito)
    // En lugar de abrir una nueva, buscamos las que ya existen.
    const pages = await browser.pages();
    let page;

    if (pages.length > 0) {
        console.log("✅ Usando la primera pestaña abierta (donde ya pasaste Cloudflare).");
        page = pages[0];
    } else {
        console.log("⚠️ No se encontraron pestañas, creando una nueva...");
        page = await browser.newPage();
    }

    // 3. INYECTAR PARCHES (Polyfills) EN LA PESTAÑA EXISTENTE
    // Como no llamamos a scraper.getPage, tenemos que inyectar la compatibilidad aquí manualmente.
    page.waitForTimeout = function (ms) { return new Promise(resolve => setTimeout(resolve, ms)); };
    page.$x = async function(expression) { return await page.$$('xpath/' + expression); };
    page.waitForXPath = async function(expression, options) { return await page.waitForSelector('xpath/' + expression, options); };
    
    // Configurar descargas
    try {
        const client = await page.createCDPSession();
        await client.send('Page.setDownloadBehavior', {
            behavior: 'allow',
            downloadPath: argv.downloads_dir
        });
    } catch (err) {
        console.log("Nota: No se pudo configurar la ruta de descarga (quizás ya estaba lista).");
    }


    // 4. VERIFICAR DÓNDE ESTAMOS
    console.log("Verificando estado de la página...");
    
    // Solo recargamos si NO estamos en la PNT, para no molestar a Cloudflare
    if (!page.url().includes('consultaPublica.xhtml')) {
        console.log("Navegando a la URL inicial...");
        await page.goto(startUrl, { waitUntil: 'domcontentloaded' });
    } else {
        console.log("✅ Ya estamos en la URL correcta. Saltando carga inicial.");
    }

    // Asegurar que estamos en la sección #inicio
    if (!page.url().includes('#inicio')) {
         await page.goto(startUrl + '#inicio', {waitUntil : 'domcontentloaded'});
    }
    
    // Esperar un momento por seguridad
    await page.waitForTimeout(2000);


    // 5. INICIO DEL PROCESO
    console.log('Descargando documentos para el año', year)
    if (type === 1) {
      console.log('Procedimientos de adjudicación directa')
    } else {
      console.log('Procedimientos de licitación pública e invitación a cuando menos tres personas')
    }

    if (organization) {
      await scraper.takeTo(page, 'tarjetaInformativa', stateCode, { organizationName: organization, year })
      await scraper.getContract(page, organization, null, year, type)
      
      // NO cerramos el navegador al terminar, solo desconectamos
      console.log("Finalizado. Desconectando del navegador...");
      browser.disconnect(); 
      return true
    }

    // Loop de scraping para listas
    let parameters = []
    if (organizations) {
      parameters = organizations.map(o => [o, null])
    } else {
      parameters = new Array(to - from + 1).fill(0)
        .map((_, i) => [null, from + i])
    }

    for (let i = 0; i < parameters.length; i++) {
      const nextParams = parameters[i + 1]
      const invocationParams = parameters[i]
      const orgId = invocationParams[0] || invocationParams[1]

      console.log('Trabajando en la organización', orgId)
      try {
        await scraper.takeTo(page, 'tarjetaInformativa', stateCode, {
          organizationName: invocationParams[0],
          organizationIndex: invocationParams[1],
          year
        })

        const res = await scraper.getContract(page, ...invocationParams, year, type)
      } catch (e) {
        console.log(e)
        console.log(`La organización ${orgId} no se pudo escrapear; brincando...`)
        if (e.message.match('redirige')) {
          await scraper.takeTo(page, 'tarjetaInformativa', stateCode, {
            organizationName: invocationParams[0],
            organizationIndex: invocationParams[1],
            year
          })
        }
      }

      if (nextParams) {
        const nextId = nextParams[0] || nextParams[1]
        console.log('La siguiente org será', nextId)
        await scraper.selectNextOrganization(page, nextId)
      }
    }

    await Promise.all(scraper.downloadsInProgress)
    
    console.log('Se descargaron %i archivos', scraper.downloadsInProgress.length)
    console.log('Terminamos el scraping')
    browser.disconnect(); // Desconectar limpiamente

  } catch (e) {
    console.log('\n❌ ERROR FATAL:', e.message);
    // No cerramos el navegador para que puedas inspeccionar
    console.log("El script se detuvo, pero tu navegador sigue abierto para revisión.");
    browser.disconnect();
    throw e
  }
})()