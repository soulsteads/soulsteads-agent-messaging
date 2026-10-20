const [sourceRoot] = process.argv.slice(2)
const { TelegramConnector, normalizeTelegramEventContext } = await import(`${sourceRoot}/connectors/telegram.ts`)
const originalFetch = globalThis.fetch
const cases = [
  {name: 'private', input: {message_id: 10, chat:{id:7,type:'private'}, from:{id:8}, text:'!oc hello'}},
  {name: 'group', input: {message_id: 11, chat:{id:-7,type:'group'}, from:{id:8}, text:'!oc hello'}},
  {name: 'reply', input: {message_id: 12, chat:{id:-7,type:'group'}, from:{id:8}, reply_to_message:{message_id:5,from:{id:9}}, text:'!oc hello'}},
  {name: 'topic', input: {message_id: 13, chat:{id:-7,type:'supergroup',is_forum:true}, from:{id:8}, is_topic_message:true,message_thread_id:99,reply_to_message:{message_id:5}, text:'!oc hello'}},
  {name: 'reply-fallback', input: {message_id: 14, chat:{id:-7,type:'group'}, from:{id:8}, reply_to_message:{message_id:5}, text:'!oc hello'}, rejectReply:true},
  {name: 'long-reply', input: {message_id: 15, chat:{id:-7,type:'group'}, from:{id:8}, reply_to_message:{message_id:5}, text:'!oc hello'}, text:'a'.repeat(4000)+'\n'+'b'.repeat(200)},
  {name: 'editable-tools', input: {message_id: 16, chat:{id:-7,type:'supergroup',is_forum:true}, from:{id:8}, is_topic_message:true,message_thread_id:99,reply_to_message:{message_id:5},text:'!oc hello'},activity:true},
]
for (const [name,text] of [
  ['boundary-newline-inclusive','a'.repeat(2000)+'\n'+'b'.repeat(2095)+'\n'+'tail'],
  ['boundary-bom-trim','a'.repeat(4096)+'\uFEFFtail'],
  ['boundary-next-line-preserve','a'.repeat(4096)+'\u0085tail'],
  ['boundary-whole-nonbmp','😀'.repeat(2048)+'tail'],
  ['boundary-leading-newline','\n'+'a'.repeat(4096)+' tail'],
  ['boundary-ascii-whitespace','a'.repeat(4096)+' \t\r\n tail'],
  ['boundary-ecmascript-whitespace','a'.repeat(4096)+'\t\n\u000B\u000C\r \u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u2028\u2029\u202F\u205F\u3000\uFEFFtail'],
  ['boundary-not-ecmascript-whitespace','a'.repeat(4096)+'\u001C\u200B\u180Etail'],
]) {
  cases.push({name,input:{message_id:20,chat:{id:-7,type:'group'},from:{id:8},reply_to_message:{message_id:5},text:'!oc hello'},text})
}
const output = []
try {
  for (const entry of cases) {
    const requests:any[] = []
    globalThis.fetch = async (_url:any, options:any) => {
      const body = JSON.parse(options.body)
      requests.push(body)
      return new Response(JSON.stringify(entry.rejectReply && body.reply_to_message_id
        ? {ok:false,error_code:400,description:'reply rejected'} : {ok:true,result:{message_id:100}}), {status:entry.rejectReply && body.reply_to_message_id ? 400 : 200})
    }
    const input = entry.input
    const context = normalizeTelegramEventContext({...input,messageId:input.message_id}, true)
    const connector:any = Object.create(TelegramConnector.prototype)
    connector.logError = () => {}
    if(entry.activity) {
      const id=await connector.createToolActivityMessage(context,'answer')
      await connector.updateToolActivityMessage(context,id,'updated')
    } else { await connector.sendReply(context, entry.text || 'answer') }
    output.push({...entry,requests})
  }
} finally { globalThis.fetch = originalFetch }
console.log(JSON.stringify(output))
