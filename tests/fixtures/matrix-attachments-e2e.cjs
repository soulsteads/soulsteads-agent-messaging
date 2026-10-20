// Optional real loopback Synapse qualification of the deployed main-room policy.
// Usage: node matrix-mvp-e2e.cjs BINARY ACP_PEER PRIVATE_ROOT [loopback-base]
const fs = require('node:fs');
const path = require('node:path');
const {spawn} = require('node:child_process');
const crypto = require('node:crypto');
const [binary,peer,root,base='http://127.0.0.1:32768'] = process.argv.slice(2);
if(!binary||!peer||!root||!['127.0.0.1','localhost','[::1]'].includes(new URL(base).hostname))throw Error('Explicit binaries/private root and loopback-only server required');
process.umask(0o077);
fs.mkdirSync(root,{mode:0o700});
const label='rust_mvp_'+crypto.randomBytes(6).toString('hex');
const checks=[];
let child;
async function api(method,route,body,auth){
 const response=await fetch(base+route,{method,headers:{'Content-Type':'application/json',...(auth?{Authorization:'Bearer '+auth.access_token}:{})},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(20000)});
 const data=await response.json();return {status:response.status,data};
}
function assert(ok,reason){if(!ok)throw Error(reason);}
async function request(method,route,body,auth){const r=await api(method,route,body,auth);assert(r.status>=200&&r.status<300,`isolated API status ${r.status}`);return r.data;}
async function register(suffix){const raw={username:label+'_'+suffix,password:crypto.randomBytes(24).toString('hex')};const first=await api('POST','/_matrix/client/v3/register',raw);assert(first.status===401,'fixture registration did not request auth');return request('POST','/_matrix/client/v3/register',{...raw,auth:{type:'m.login.dummy',session:first.data.session}});}
async function until(probe,label,timeout=45000){const end=Date.now()+timeout;while(Date.now()<end){if(await probe())return;await new Promise(r=>setTimeout(r,150));}throw Error(label);}
const audit=()=>fs.existsSync(path.join(root,'audit.jsonl'))?fs.readFileSync(path.join(root,'audit.jsonl'),'utf8').trim().split('\n').filter(Boolean).map(JSON.parse):[];
const prompts=()=>audit().filter(x=>x.method==='session/prompt');
async function stop(){if(!child)return;const c=child;child=null;const done=new Promise(resolve=>c.once('exit',(code,signal)=>resolve({code,signal})));process.kill(c.pid,'SIGTERM');const result=await Promise.race([done,new Promise(resolve=>setTimeout(()=>resolve(null),10000))]);if(!result){process.kill(-c.pid,'SIGKILL');throw Error('service stop deadline exceeded');}assert(result.code===0&&!result.signal,'SIGTERM did not complete graceful shutdown');}
(async()=>{
 const bot=await register('bot'),sender=await register('sender'),denied=await register('denied');
 const room=(await request('POST','/_matrix/client/v3/createRoom',{preset:'private_chat',name:'Isolated Rust MVP',invite:[bot.user_id,denied.user_id]},sender)).room_id;
 await request('POST','/_matrix/client/v3/join/'+encodeURIComponent(room),{},denied);
 const configuration={rateLimitSeconds:0,sessionStorePath:path.join(root,'sessions.json'),acp:{command:'python3',args:[peer,path.join(root,'audit.jsonl')],backendId:'isolated-mvp-fixture'},matrix:{enabled:true,homeserver:base,accessToken:bot.access_token,userId:bot.user_id,allowedUsers:[sender.user_id],allowedRooms:[room],strictAllowedRooms:true,threadIsolation:false,respondToAllMessages:true,interactivePermissions:false,encryption:{enabled:false}}};
 fs.writeFileSync(path.join(root,'config.json'),JSON.stringify(configuration),{mode:0o600});
 function start(){const env={PATH:process.env.PATH,HOME:path.join(root,'home'),TMPDIR:path.join(root,'tmp'),MATRIX_STORAGE_PATH:path.join(root,'rust-matrix-state'),SESSION_BASE_DIR:path.join(root,'workspaces')};for(const d of [env.HOME,env.TMPDIR])fs.mkdirSync(d,{recursive:true,mode:0o700});const log=fs.openSync(path.join(root,'bridge.log'),'a',0o600);child=spawn(binary,['--serve','--config',path.join(root,'config.json')],{cwd:root,env,detached:true,stdio:['ignore',log,log]});fs.closeSync(log);}
 const events=async()=> (await request('GET','/_matrix/client/v3/rooms/'+encodeURIComponent(room)+'/messages?dir=b&limit=100',undefined,sender)).chunk;
 const send=async(auth,text)=>request('PUT','/_matrix/client/v3/rooms/'+encodeURIComponent(room)+'/send/m.room.message/'+crypto.randomUUID(),{msgtype:'m.text',body:text},auth);
 const reply=async(text)=>{let answer;await until(async()=>{answer=(await events()).find(e=>e.sender===bot.user_id&&e.content?.body?.includes(text));return !!answer;},'native Matrix response missing');return answer;};
 // Reproduce the real switch: bot already joined, old work already answered.
 await request('POST','/_matrix/client/v3/rooms/'+encodeURIComponent(room)+'/join',{},bot);
 const old='already-completed-before-switch-'+crypto.randomUUID();await send(sender,old);await send(bot,'completed prior work');
 start();
 await until(async()=>fs.readFileSync(path.join(root,'bridge.log'),'utf8').includes('Matrix listening'),'native Matrix did not finish bootstrap');
 await new Promise(r=>setTimeout(r,500));assert(!prompts().some(x=>x.text===old),'fresh SDK state replayed historical input');checks.push('fresh native SDK state bootstraps without replaying already completed history');
 await until(async()=>{const joined=await request('GET','/_matrix/client/v3/rooms/'+encodeURIComponent(room)+'/joined_members',undefined,sender);return !!joined.joined[bot.user_id];},'allowlisted invite was not joined');
 const first='main-room-first-'+crypto.randomUUID();await send(sender,first);const initial=await reply(first);assert(!initial.content['m.relates_to']?.rel_type,'main-room response acquired thread isolation');assert(prompts().some(x=>x.text===first),'prefix-free input was not forwarded intact');checks.push('prefix-free plain Matrix input/reply through native sync and ACP');
 const follow='main-room-followup-'+crypto.randomUUID();await send(sender,follow);await reply(follow);let p=prompts();const firstSession=p.find(x=>x.text===first).sessionId;assert(p.find(x=>x.text===follow).sessionId===firstSession,'main-room continuity lost');checks.push('main-room subsequent input retains ACP session');
 const slow='QA-HANG',busy='busy-followup-'+crypto.randomUUID();await send(sender,slow);await until(async()=>prompts().some(x=>x.text===slow),'slow prompt did not reach peer');await send(sender,busy);await until(async()=>prompts().some(x=>x.text===busy),'busy follow-up did not reach peer');const frames=audit(),followIndex=frames.findIndex(x=>x.method==='session/prompt'&&x.text===busy),settleIndex=frames.findIndex(x=>x.method==='fixture/settled'&&x.text===slow);assert(settleIndex<0||followIndex<settleIndex,'SDK busy follow-up was delayed until original settlement');assert(prompts().find(x=>x.text===busy).sessionId===firstSession,'busy input opened another session');await reply(slow);checks.push('actual native SDK admits busy follow-up before original RPC settlement');
 const before=prompts().length;const forbidden='forbidden-'+crypto.randomUUID();await send(denied,forbidden);await send(bot,'self-'+crypto.randomUUID());await new Promise(r=>setTimeout(r,1800));assert(prompts().length===before,'unauthorized or self input reached ACP');checks.push('unauthorized and own-message rejection before ACP');
 const other=(await request('POST','/_matrix/client/v3/createRoom',{preset:'private_chat',invite:[bot.user_id]},sender)).room_id;await new Promise(r=>setTimeout(r,1800));const joined=await request('GET','/_matrix/client/v3/rooms/'+encodeURIComponent(other)+'/joined_members',undefined,sender);assert(!joined.joined[bot.user_id],'strict room admission failed');checks.push('strict room policy refuses unrelated invitation');

 // Real homeserver authenticated media -> deployed SDK ingress -> ACP -> reply.
 async function file(auth,filename,caption,mime,bytes,txn=crypto.randomUUID()){
  const r=await fetch(base+'/_matrix/media/v3/upload?filename='+encodeURIComponent(filename),{method:'POST',headers:{Authorization:'Bearer '+auth.access_token,'Content-Type':mime},body:bytes,signal:AbortSignal.timeout(20000)});
  const data=await r.json();assert(r.ok&&data.content_uri,'fixture upload failed');
  return request('PUT','/_matrix/client/v3/rooms/'+encodeURIComponent(room)+'/send/m.room.message/'+txn,{msgtype:'m.file',filename,body:caption,url:data.content_uri,info:{mimetype:mime,size:bytes.length}},auth);
 }
 async function attachmentAccepted(filename,caption,mime,contents){
  const count=prompts().length;await file(sender,filename,caption,mime,Buffer.from(contents));
  await until(async()=>prompts().length===count+1,'attachment did not reach ACP');
  const p=prompts().at(-1); assert(p.sessionId===firstSession,'attachment lost main-room session');
  assert(p.text.includes(filename)&&p.text.includes(caption)&&p.text.includes(contents),'filename, caption or exact file text missing');
  assert(p.text.indexOf(contents)===p.text.lastIndexOf(contents),'attachment content duplicated');
  assert(!p.text.startsWith('/'),'file caption could enter local slash-command dispatch');
  await reply(contents);return p;
 }
 const md=await attachmentAccepted('plan.md','Review this plan, and see what youn think','text/markdown','# Isolated attachment\n\nExact markdown evidence-'+crypto.randomUUID());
 checks.push('real Matrix Markdown download preserves filename, caption and exact text through ACP reply');
 await attachmentAccepted('notes.txt','Summarize this text','application/octet-stream','Exact UTF-8 text evidence-'+crypto.randomUUID());
 checks.push('real Matrix plain-text generic MIME handoff retains existing session');
 await attachmentAccepted('commands.md','/reset','text/markdown','/reset\nAttachment text must not clear the session-'+crypto.randomUUID());
 checks.push('file caption and content beginning with slash stay ordinary model input');
 async function rejectedFile(filename,mime,bytes){
  const count=prompts().length;const before=new Set((await events()).map(e=>e.event_id));
  await file(sender,filename,'Read this attachment',mime,bytes);
  await until(async()=>(await events()).some(e=>e.sender===bot.user_id&&!before.has(e.event_id)&&e.content?.body),'unsupported attachment lacked explicit reply');
  assert(prompts().length===count,'rejected file reached ACP');
 }
 await rejectedFile('document.pdf','application/pdf',Buffer.from('%PDF-synthetic unsupported'));
 await rejectedFile('binary.txt','text/plain',Buffer.from([0xff,0x00,0x01]));
 await rejectedFile('oversize.txt','text/plain',Buffer.alloc(256*1024+1,0x61));
 checks.push('unsupported type, invalid UTF-8/binary and oversized files explicitly rejected without ACP');
 const deniedCount=prompts().length;await file(denied,'denied.md','Review denied file','text/markdown',Buffer.from('must not forward'));
 await new Promise(r=>setTimeout(r,1000));assert(prompts().length===deniedCount,'unauthorized attachment reached ACP');
 checks.push('unauthorized real Matrix file never reaches ACP');

 const saved=JSON.parse(fs.readFileSync(path.join(root,'sessions.json')));await stop();checks.push('actual executable SIGTERM exits cleanly within bound');start();
 const resumed='main-room-resume-'+crypto.randomUUID();await send(sender,resumed);await reply(resumed);assert(prompts().find(x=>x.text===resumed).sessionId===firstSession,'native service restart did not resume binding');const after=JSON.parse(fs.readFileSync(path.join(root,'sessions.json')));assert(saved.sessions.length===after.sessions.length&&saved.sessions.every(x=>after.sessions.some(y=>x.sessionId===y.sessionId&&x.cwd===y.cwd&&x.backendId===y.backendId&&x.threadId===y.threadId)),'restart changed stored identities');assert(prompts().length===8&&prompts().filter(x=>x.text===first).length===1&&prompts().filter(x=>x.text===follow).length===1&&prompts().filter(x=>x.text===slow).length===1&&prompts().filter(x=>x.text===busy).length===1,'restart replayed settled input');checks.push('same SDK store restart resumes exact main-room ACP binding without replay');
 const cryptoKeys=(await request('POST','/_matrix/client/v3/keys/query',{device_keys:{[bot.user_id]:[]}},bot)).device_keys?.[bot.user_id]||{};assert(Object.keys(cryptoKeys).length===0,'plaintext artifact published crypto keys');checks.push('plaintext SDK publishes no device encryption keys');
 const cryptoBoundary=prompts().length;await request('PUT','/_matrix/client/v3/rooms/'+encodeURIComponent(room)+'/state/m.room.encryption',{algorithm:'m.megolm.v1.aes-sha2'},sender);await send(sender,'input-after-encryption-'+crypto.randomUUID());await new Promise(r=>setTimeout(r,1000));assert(prompts().length===cryptoBoundary,'encrypted room input reached ACP');checks.push('encrypted-room transition refuses new ACP input');await stop();
 const result={passed:true,checks:checks.map(name=>({name,passed:true})),binary_sha256:crypto.createHash('sha256').update(fs.readFileSync(binary)).digest('hex'),production_accounts_used:false,production_services_changed:false};fs.writeFileSync(path.join(root,'result.json'),JSON.stringify(result,null,2));console.log(JSON.stringify(result));
})().catch(async error=>{try{await stop();}catch{}fs.writeFileSync(path.join(root,'failure.json'),JSON.stringify({passed:false,reason:error.message,checks}));console.error(error.message);process.exitCode=1;});
