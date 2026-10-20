import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, writeFile, readFile } from 'node:fs/promises'
import { createServer } from 'node:net'
import path from 'node:path'
import { pathToFileURL } from 'node:url'

const [binary, acp, playwrightModule, temporaryRoot] = process.argv.slice(2).map(value => path.resolve(value))
const { chromium } = await import(pathToFileURL(playwrightModule).href)
const directory = await mkdtemp(path.join(temporaryRoot, 'browser-qualification-'))
const reservation = createServer()
reservation.listen(0, '127.0.0.1')
await once(reservation, 'listening')
const port = reservation.address().port
await new Promise(resolve => reservation.close(resolve))
const origin = `http://127.0.0.1:${port}`
const script = path.join(directory, 'script.json')
const audit = path.join(directory, 'audit.jsonl')
const text = '## Browser answer\nhttps://maps.google.com/?q=1,2\n\n| Name | Value |\n| --- | --- |\n| safe | **bold** |\n\n<script>window.injected=true</script>'
await writeFile(script, JSON.stringify([
  { updates: [
    { update:{sessionUpdate:'agent_message_chunk', content:{type:'text',text:text.slice(0,40)}} },
    { update:{sessionUpdate:'agent_message_chunk', content:{type:'text',text:text.slice(40)}} },
  ], stopReason:'end_turn' },
  {text:'image accepted',stopReason:'end_turn'},
  {waitForFollowup:true},
  {error:true},
  {text:'recovered after cancellation and error',stopReason:'end_turn'},
]))
const configuration = {
  rateLimitSeconds:0, sessionStorePath:path.join(directory,'store.json'),
  acp:{command:acp,args:[],env:{ACP_FIXTURE_MATRIX_SCRIPT:script,
    ACP_FIXTURE_MATRIX_COUNTER:path.join(directory,'counter'),ACP_FIXTURE_RPC_AUDIT:audit}},
  web:{enabled:true,host:'127.0.0.1',port,allowedOrigins:[origin],attachments:{enabled:true,maxFilesPerMessage:1,maxWidth:64,maxHeight:64,maxPixels:4096}},
}
const configFile = path.join(directory,'config.json')
await writeFile(configFile,JSON.stringify(configuration))
let child
let browser
let stderr = ''
const outcomes = []
async function start() {
  child = spawn(binary,['--config',configFile,'--serve'],{cwd:directory,
    env:{PATH:process.env.PATH,HOME:directory,TMPDIR:temporaryRoot,TMP:temporaryRoot,TEMP:temporaryRoot,SESSION_BASE_DIR:path.join(directory,'sessions')},
    stdio:['ignore','ignore','pipe']})
  child.stderr.on('data',chunk => {stderr+=chunk})
  for (let attempt=0;attempt<100;attempt++) {
    assert.equal(child.exitCode,null,stderr)
    try { if ((await fetch(`${origin}/health`)).ok) return } catch {}
    await new Promise(resolve=>setTimeout(resolve,50))
  }
  throw new Error(`isolated server did not start: ${stderr}`)
}
async function stop() {
  if (!child || child.exitCode!==null) return
  const exit=once(child,'exit')
  child.kill('SIGINT')
  const timer=setTimeout(()=>child.kill('SIGKILL'),5000)
  try { const [code]=await exit; assert.equal(code,0,stderr) } finally {clearTimeout(timer)}
}
try {
  await start()
  browser=await chromium.launch({headless:true,args:['--no-sandbox']})
  const context=await browser.newContext()
  const page=await context.newPage()
  await page.addInitScript(()=>{
    const Native=window.WebSocket
    window.fixtureSockets=[]
    window.fixtureWireEvents=[]
    window.WebSocket=class extends Native {constructor(...args){super(...args);window.fixtureSockets.push(this);this.addEventListener('message',event=>window.fixtureWireEvents.push(JSON.parse(event.data)))}}
  })
  await page.goto(`${origin}/chat`)
  await page.waitForFunction(()=>document.querySelector('#oc-status')?.textContent==='Online')
  await page.locator('.oc-inp').fill('browser prompt')
  await page.locator('.oc-send').click()
  await page.waitForFunction(()=>document.querySelector('.oc-msg--bot h2')?.textContent==='Browser answer')
  await page.waitForFunction(()=>window.fixtureWireEvents.some(event=>event.type==='done'))
  assert.equal(await page.locator('.oc-msg--bot').count(),1,'one completed prompt must render one bot bubble')
  assert.equal(await page.locator('.oc-msg--bot a').getAttribute('href'),'https://maps.google.com/?q=1,2')
  assert.equal(await page.locator('.oc-msg--bot td strong').textContent(),'bold')
  assert.equal(await page.evaluate(()=>window.injected),undefined)
  outcomes.push('typed input, streamed split URL, Markdown/table DOM, inert HTML')
  await page.reload()
  await page.waitForFunction(()=>document.querySelector('#oc-status')?.textContent==='Online')
  assert.equal(await page.locator('.oc-msg--bot h2').textContent(),'Browser answer')
  outcomes.push('reload preserves source text and re-renders persisted links/table')
  const png=await page.evaluate(()=>{const canvas=document.createElement('canvas');canvas.width=1;canvas.height=1;return canvas.toDataURL('image/png').split(',')[1]})
  await page.locator('input[type=file]').setInputFiles({name:'pixel.png',mimeType:'image/png',buffer:Buffer.from(png,'base64')})
  await page.waitForSelector('.oc-img-preview img')
  await page.locator('.oc-inp').fill('with image')
  await page.evaluate(()=>window.fixtureWireEvents=[])
  await page.locator('.oc-send').click()
  await page.waitForFunction(()=>Array.from(document.querySelectorAll('.oc-msg--bot')).some(element=>element.textContent.includes('image accepted')))
  await page.waitForFunction(()=>window.fixtureWireEvents.some(event=>event.type==='done'))
  assert.equal(await page.locator('.oc-msg--bot').count(),2,'image prompt must add exactly one bot bubble')
  const requests=(await readFile(audit,'utf8')).trim().split('\n').map(line=>JSON.parse(line))
  const imagePrompt=requests.filter(request=>request.method==='session/prompt').at(-1)
  assert.equal(imagePrompt.params.prompt[1].type,'image')
  assert.equal(imagePrompt.params.prompt[1].mimeType,'image/png')
  const persisted=await page.evaluate(()=>JSON.parse(localStorage.getItem('oc-widget')))
  assert(!JSON.stringify(persisted).includes(png),'image bytes must not enter durable browser history')
  outcomes.push('real file selection/bitmap preview/native ACP image; no persisted image bytes')
  const tooWide=Buffer.from(png,'base64');tooWide.writeUInt32BE(50000,16)
  await page.locator('input[type=file]').setInputFiles({name:'oversize.png',mimeType:'image/png',buffer:tooWide})
  await page.waitForFunction(()=>document.querySelector('.oc-img-error')?.textContent.length>0)
  assert.equal(await page.locator('.oc-img-preview img').count(),0)
  outcomes.push('header dimensions rejected before browser bitmap decoding')
  await page.evaluate(()=>window.fixtureSockets.at(-1).close())
  await page.waitForFunction(()=>window.fixtureSockets.length>=2 && window.fixtureSockets.at(-1).readyState===1)
  assert.equal(await page.locator('.oc-msg--bot h2').textContent(),'Browser answer')
  outcomes.push('WebSocket reconnect retains history and same session')
  await page.evaluate(()=>window.fixtureWireEvents=[])
  await page.locator('.oc-inp').fill('waiting prompt')
  await page.locator('.oc-send').click()
  await page.waitForFunction(()=>window.fixtureWireEvents.some(event=>event.type==='thinking'))
  for(let attempt=0;attempt<100;attempt++) {
    const requests=(await readFile(audit,'utf8')).trim().split('\n').map(line=>JSON.parse(line))
    if(requests.filter(request=>request.method==='session/prompt').length===3) break
    assert(attempt<99,'cancel fixture must reach actual ACP prompt')
    await new Promise(resolve=>setTimeout(resolve,20))
  }
  await page.evaluate(()=>window.fixtureSockets.at(-1).send(JSON.stringify({type:'message',text:'/cancel'})))
  await page.waitForFunction(()=>{
    const terminal=window.fixtureWireEvents.findIndex(event=>event.type==='prompt_end'&&event.stopReason==='cancelled')
    return terminal>=0&&window.fixtureWireEvents.slice(terminal+1).some(event=>event.type==='done')
  })
  assert.equal(await page.locator('.oc-inp').isDisabled(),false)
  assert((await readFile(audit,'utf8')).includes('session/cancel'))
  outcomes.push('native cancellation reaches ACP and terminal done restores input')
  await page.evaluate(()=>window.fixtureWireEvents=[])
  await page.locator('.oc-inp').fill('backend error')
  await page.locator('.oc-send').click()
  await page.waitForFunction(()=>window.fixtureWireEvents.some(event=>event.type==='error')&&window.fixtureWireEvents.some(event=>event.type==='done'))
  assert.equal(await page.locator('.oc-inp').isDisabled(),false)
  outcomes.push('ACP failure terminates and restores browser input')
  await page.evaluate(()=>window.fixtureWireEvents=[])
  await page.locator('.oc-inp').fill('recovery prompt')
  await page.locator('.oc-send').click()
  await page.waitForFunction(()=>window.fixtureWireEvents.some(event=>event.type==='done'))
  assert.equal(await page.locator('.oc-msg--bot').filter({hasText:'recovered after cancellation and error'}).count(),1)
  assert.equal(await page.locator('.oc-inp').isDisabled(),false)
  outcomes.push('recovery after cancel/error renders exactly one terminal bot bubble')
  await stop()
  await start()
  await page.reload()
  await page.waitForFunction(()=>document.querySelector('#oc-status')?.textContent==='Online')
  assert.equal(await page.locator('.oc-msg--bot h2').textContent(),'Browser answer')
  outcomes.push('actual binary restart reuses stored backend mapping and browser history')
  await page.locator('button[title="Clear chat"]').click()
  await page.waitForFunction(()=>document.querySelectorAll('.oc-msg').length===0)
  outcomes.push('explicit clear confirmation removes browser history')
  for(let attempt=0;attempt<100;attempt++) {
    if(JSON.parse(await readFile(path.join(directory,'store.json'),'utf8')).sessions.length===0) break
    await new Promise(resolve=>setTimeout(resolve,20))
  }
  const clearedStore=JSON.parse(await readFile(path.join(directory,'store.json'),'utf8'))
  assert.equal(clearedStore.sessions.length,0)
  await context.close()
  console.log(JSON.stringify({directory,binary,outcomes,passed:outcomes.length}))
} finally {
  if(browser) await browser.close()
  await stop()
}
