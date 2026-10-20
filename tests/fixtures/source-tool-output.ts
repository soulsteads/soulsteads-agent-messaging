const [sourceRoot, inputPath] = process.argv.slice(2)
const { ACPClient } = await import(`${sourceRoot}/src/acp-client.ts`)
const { WebConnector } = await import(`${sourceRoot}/connectors/web.ts`)
const { getConfig } = await import(`${sourceRoot}/src/config.ts`)
const cases = await Bun.file(inputPath).json()
const output = []
for (const entry of cases) {
  getConfig().toolMessages = {mode:'off',showCalls:false,showArguments:false,showOutputFor:entry.selectors}
  const client:any = new ACPClient()
  client.sessionId = 'fixture'
  client.send = async () => {
    for(const update of entry.updates) client.handleSessionUpdate({sessionId:'fixture',update})
    client.handleSessionUpdate({sessionId:'fixture',update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text:'answer'}}})
    return {stopReason:'end_turn'}
  }
  const session = {client,createdAt:new Date(),lastActivity:new Date(),messageCount:0,inputChars:0,outputChars:0}
  const messages:any[] = []
  const connector = {
    isQueryActive:()=>false, markQueryActive:()=>({sessionId:'browser',id:1,aborted:false}), markQueryDone:()=>{},
    getOrCreateSession:async()=>session,createBaseSession:()=>session,sessionManager:{get:()=>session},
    wsSend:(_id:string,event:any)=>messages.push(event),sendFileAsImage:()=>{},sendFileAsDoc:()=>{},log:()=>{},logError:()=>{},
  }
  await (WebConnector.prototype as any).processQuery.call(connector,'browser','query')
  output.push(messages.filter(event=>event.type==='tool_result'||event.type==='tool_output'))
}
console.log(JSON.stringify(output))
