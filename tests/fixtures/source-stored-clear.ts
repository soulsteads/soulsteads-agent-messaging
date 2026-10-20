import { readFileSync } from 'node:fs'
const [sourceRoot] = process.argv.slice(2)
const { WebConnector } = await import(`${sourceRoot}/connectors/web.ts`)
let connector: any = new WebConnector()
let events: any[] = []
const capture = () => { connector.config.rateLimitSeconds = 0; connector.wsSend = (_: string, event: any) => events.push(event) }
capture()
try {
  await connector.onWsMessage({data:{clientId:'stored'}}, JSON.stringify({type:'message',text:'before'}))
  await connector.disconnectAllSessions()
  connector = new WebConnector()
  capture()
  events = []
  await connector.onWsMessage({data:{clientId:'stored'}}, JSON.stringify({type:'message',text:'/clear'}))
  const clearEvents = events
  events = []
  await connector.onWsMessage({data:{clientId:'stored'}}, JSON.stringify({type:'message',text:'after'}))
  const requests = readFileSync('audit.jsonl','utf8').trim().split('\n').map(line=>JSON.parse(line))
  console.log(JSON.stringify({clearEvents,requests}))
} finally { await connector.disconnectAllSessions() }
