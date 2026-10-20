import os,pathlib,tempfile,subprocess,json,secrets,urllib.request,base64,select,time,threading,http.server
import urllib.error
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--rust-probe')
parser.add_argument('--opencode', default='opencode')
args = parser.parse_args()

# Isolated integration fixture: run on a host with OpenCode v2.0.23 installed.
# No real provider credentials or upstream model calls are used.
root=pathlib.Path(tempfile.mkdtemp(prefix='bridge-native-execution-'))

gate=threading.Event();started=threading.Event();calls=[];events=[]
class Provider(http.server.BaseHTTPRequestHandler):
 def log_message(self,format,*args):pass
 def do_POST(self):
  body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));calls.append({'path':self.path,'model':body.get('model'),'stream':body.get('stream')});n=len(calls)
  fixture='2' if 'ISOLATED FIXTURE 2' in json.dumps(body.get('messages',[])) else '1'
  if n==1:started.set();(root/'provider-started').touch();gate.wait(40)
  self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
  chunks=[{'id':'fixture'+str(n),'object':'chat.completion.chunk','created':1,'model':'fake','choices':[{'index':0,'delta':{'role':'assistant','content':'FIXTURE_REPLY_'+fixture},'finish_reason':None}]},{'id':'fixture'+str(n),'object':'chat.completion.chunk','created':1,'model':'fake','choices':[{'index':0,'delta':{},'finish_reason':'stop'}],'usage':{'prompt_tokens':1,'completion_tokens':1,'total_tokens':2}}]
  try:
   for chunk in chunks:self.wfile.write(('data: '+json.dumps(chunk)+'\n\n').encode());self.wfile.flush()
   self.wfile.write(b'data: [DONE]\n\n');self.wfile.flush()
  except BrokenPipeError:pass
provider=http.server.ThreadingHTTPServer(('127.0.0.1',0),Provider);threading.Thread(target=provider.serve_forever,daemon=True).start()
env={k:v for k,v in os.environ.items() if k in ['PATH','LANG','TMPDIR']}
for key,sub in [('HOME','home'),('XDG_CONFIG_HOME','config'),('XDG_DATA_HOME','data'),('XDG_CACHE_HOME','cache'),('XDG_STATE_HOME','state')]:
 p=root/sub;p.mkdir();env[key]=str(p)
password=secrets.token_urlsafe(32);env['OPENCODE_PASSWORD']=password
config={'enabled_providers':['fixture'],'model':'fixture/fake','small_model':'fixture/fake','autoupdate':False,'share':'disabled','snapshot':False,'provider':{'fixture':{'npm':'@ai-sdk/openai-compatible','name':'Fixture','options':{'baseURL':'http://127.0.0.1:'+str(provider.server_port)+'/v1','apiKey':'fixture-not-a-secret'},'models':{'fake':{'name':'Fake','limit':{'context':32000,'output':1000},'tool_call':False}}}}}
env['OPENCODE_CONFIG_CONTENT']=json.dumps(config)
if args.rust_probe:
 stop=threading.Event()
 def release_when_admitted():
  while not stop.wait(.02):
   if (root/'release-provider').exists():gate.set();return
 threading.Thread(target=release_when_admitted,daemon=True).start()
 try:
  probe=subprocess.run([args.rust_probe,args.opencode,str(root)],env=env,cwd=str(root),capture_output=True,text=True,timeout=80)
  if probe.returncode != 0:raise RuntimeError('Rust probe failed: '+probe.stderr[:4000])
  receipt=json.loads(probe.stdout)
  assert receipt['two_replies_verified'] and receipt['server_exit']==0 and receipt['inbox_after']==[]
  receipt.update({'scratch':str(root),'fixture_provider':True,'provider_calls':calls})
  (root/'receipt.json').write_text(json.dumps(receipt,indent=2))
  print(json.dumps(receipt));print('receipt',str(root/'receipt.json'))
 finally:
  stop.set();gate.set();provider.shutdown()
 raise SystemExit(0)
log=open(root/'server.log','w');p=subprocess.Popen([args.opencode,'serve','--stdio','--hostname','127.0.0.1','--port','0'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,env=env,cwd=str(root))
assert p.stdin is not None and p.stdout is not None
result={'scratch':str(root),'fixture_provider':True}
try:
 if not select.select([p.stdout],[],[],30)[0]:raise RuntimeError('no readiness')
 base=json.loads(p.stdout.readline())['url'];headers={'Authorization':'Basic '+base64.b64encode(('opencode:'+password).encode()).decode(),'Content-Type':'application/json'}
 def req(path,body=None):
  r=urllib.request.Request(base.rstrip('/')+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
  try:
   with urllib.request.urlopen(r,timeout=15) as f:return json.loads(f.read())
  except urllib.error.HTTPError as e:raise RuntimeError(path+' HTTP '+str(e.code)+' '+e.read().decode()[:2000])
 def collect():
  try:
   with urllib.request.urlopen(urllib.request.Request(base.rstrip('/')+'/api/event',headers=headers),timeout=60) as f:
    for line in f:
     if line.startswith(b'data:'):events.append(json.loads(line[5:]))
  except Exception:pass
 threading.Thread(target=collect,daemon=True).start()
 result['models']=req('/api/model')
 sid=req('/api/session',{'location':{'directory':str(root)},'model':{'providerID':'fixture','id':'fake'}})['data']['id'];result['session']=sid
 acks=[]
 for n in [1,2]:
  mid='msg_'+secrets.token_hex(12);ack=req('/api/session/'+sid+'/prompt',{'id':mid,'text':'ISOLATED FIXTURE '+str(n),'delivery':'queue','resume':True});acks.append({'submitted':mid,'returned':ack['data']['id']})
  if n==1:result['provider_started']=started.wait(20)
 result['inbox_before_release']=req('/api/session/'+sid+'/inbox');gate.set()
 deadline=time.monotonic()+30
 while time.monotonic()<deadline:
  messages=req('/api/session/'+sid+'/message');result['messages']=messages
  if 'FIXTURE_REPLY_2' in json.dumps(messages):break
  time.sleep(.25)
 result['acks']=acks;result['inbox_after']=req('/api/session/'+sid+'/inbox');result['provider_calls']=calls;result['events']=events
 result['two_replies_verified']=all(s in json.dumps(result['messages']) for s in ['FIXTURE_REPLY_1','FIXTURE_REPLY_2'])
except Exception as e:result['error']=str(e)
finally:
 gate.set();p.stdin.close()
 try:result['server_exit']=p.wait(timeout=20)
 except subprocess.TimeoutExpired:p.terminate();result['server_exit']=p.wait(timeout=10)
 provider.shutdown();log.close();(root/'receipt.json').write_text(json.dumps(result,indent=2))
 print(json.dumps({k:v for k,v in result.items() if k not in ['models','events','messages']}));print('messages',json.dumps(result.get('messages'))[:10000]);print('event_count',len(events));print('receipt',str(root/'receipt.json'))
assert result.get('two_replies_verified') is True, result.get('error', 'missing queued replies')
assert result.get('provider_started') is True
assert result['inbox_after'] == {'data': []}
assert result['server_exit'] == 0
assert events, 'missing native events'
assert all(a['submitted'] == a['returned'] for a in result['acks'])
assert any(item['id'] == result['acks'][1]['submitted'] for item in result['inbox_before_release']['data']), 'second prompt not observed queued while first blocked'
timeline = list(reversed(result['messages']['data']))
assert [item.get('text') for item in timeline if item['type'] == 'user'] == ['ISOLATED FIXTURE 1', 'ISOLATED FIXTURE 2']
assert [item['content'][0]['text'] for item in timeline if item['type'] == 'assistant'] == ['FIXTURE_REPLY_1', 'FIXTURE_REPLY_2']
