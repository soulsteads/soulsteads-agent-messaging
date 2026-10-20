import fs from 'node:fs'
const [sourceRoot] = process.argv.slice(2)
const results=[]
for(const kind of ['slack','mattermost','discord']) {
  const source=fs.readFileSync(`${sourceRoot}/connectors/${kind}.ts`,'utf8')
  const section=source.slice(source.indexOf('const toolActivity = new ToolActivityController'))
  function callback(name:string, end:string) {
    const start=section.indexOf(`${name}: async `)+name.length+2
    const finish=section.indexOf(end,start)
    if(start<name.length+2||finish<start) throw new Error(`Source ${kind} callback boundary missing`)
    return section.slice(start,finish).trim().replace(/,$/,'')
  }
  for(const isolated of [true,false]) {
    const requests:any[]=[]
    const context={channelId:'room',replyThreadTs:'root',replyRootId:'root'}
    const slackClient={chat:{postMessage:async(body:any)=>{requests.push({method:'POST',body});return{ts:'100'}},update:async(body:any)=>{requests.push({method:'POST',body})}}}
    const mmApi=async(method:string,endpoint:string,body:any)=>{requests.push({method,endpoint,body});return{id:'100'}}
    const channel={send:async(text:string)=>{requests.push({method:'POST',text});return{id:'100',edit:async(text:string)=>{requests.push({method:'PATCH',text})}}}}
    const activityMessages=new Map()
    const create=new Function('slackClient','context','mmApi','channel','activityMessages',`return (${callback('create','\n      update:')})`).call({threadIsolation:isolated},slackClient,context,mmApi,channel,activityMessages)
    const update=new Function('slackClient','context','mmApi','channel','activityMessages',`return (${callback('update','\n      onError:')})`).call({threadIsolation:isolated},slackClient,context,mmApi,channel,activityMessages)
    const id=await create.call({threadIsolation:isolated},'answer')
    await update.call({threadIsolation:isolated},id,'updated')
    results.push({kind,isolated,requests})
  }
}
console.log(JSON.stringify(results))
