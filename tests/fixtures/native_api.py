import http.server
import json
import os
import pathlib
import queue
import sys
import threading
import time
import urllib.parse
import base64
import io

assert 'acp' not in sys.argv
assert sys.argv[-6:] == ['serve', '--stdio', '--hostname', '127.0.0.1', '--port', '0']
root = pathlib.Path(os.environ['NATIVE_FIXTURE_ROOT'])
state_file = root / 'native-state.json'
state = json.loads(state_file.read_text()) if state_file.exists() else {'sessions': {}}
lock = threading.RLock()
subscribers = []
backlog = []
auth = 'Basic ' + base64.b64encode(('opencode:' + os.environ['OPENCODE_PASSWORD']).encode()).decode()

def save():
    state_file.write_text(json.dumps(state))

def emit(kind, data):
    event = {'id': 'evt_' + str(time.time_ns()), 'type': kind, 'data': data}
    backlog.append(event)
    for subscriber in list(subscribers):
        subscriber.put(event)

def run(session):
    while True:
        with lock:
            if not session['inbox']:
                session['running'] = False
                session['info']['time']['idle'] = time.time_ns()
                session['info']['outcome'] = 'succeeded'
                save()
                emit('session.execution.succeeded', {'sessionID': session['info']['id']})
                return
            item = session['inbox'].pop(0)
            session['messages'].append({'id': item['id'], 'type': 'user', 'text': item['text']})
            save()
        (root / 'started').touch()
        if item['text'] == 'terminal-output':
            with lock:
                sid = session['info']['id']
                mid = 'msg_terminal_' + str(time.time_ns())
                tools = []
                session['messages'].append({'id': mid, 'type': 'assistant', 'content': tools})
                for index in range(3):
                    call = 'call_terminal_' + str(index)
                    command = {'command': 'native-command-' + str(index)}
                    content = [{'type': 'text', 'text': 'native-output-' + str(index)}]
                    tools.append({'type': 'tool', 'id': call, 'name': 'bash', 'state': {'status': 'completed', 'input': command, 'content': content}})
                    ref = {'sessionID': sid, 'assistantMessageID': mid, 'id': call}
                    emit('session.tool.input.started', dict(ref, name='bash'))
                    emit('session.tool.called', dict(ref, input=command, executed=True))
                    emit('session.tool.success', dict(ref, content=content, executed=True))
                save()
        if item['text'] == 'media-output':
            with lock:
                sid = session['info']['id']
                mid = 'msg_media_' + str(time.time_ns())
                content = json.loads(os.environ['NATIVE_FIXTURE_MEDIA_CONTENT'])
                tool = {'type': 'tool', 'id': 'call_media', 'name': 'fixture_media', 'state': {'status': 'completed', 'input': {}, 'content': content}}
                session['messages'].append({'id': mid, 'type': 'assistant', 'content': [tool]})
                ref = {'sessionID': sid, 'assistantMessageID': mid, 'id': 'call_media'}
                emit('session.tool.input.started', dict(ref, name='fixture_media'))
                emit('session.tool.called', dict(ref, input={}, executed=True))
                emit('session.tool.success', dict(ref, content=content, executed=True))
                save()
        if item['text'] in ['permission', 'stale-permission']:
            with lock:
                sid = session['info']['id']
                tool_mid = 'msg_tool_' + str(time.time_ns())
                tool = {'type': 'tool', 'id': 'call_fixture', 'name': 'bash', 'state': {'status': 'running', 'input': {'command': 'printf fixture'}, 'metadata': {}}}
                session['messages'].append({'id': tool_mid, 'type': 'assistant', 'content': [tool]})
                ref = {'sessionID': sid, 'assistantMessageID': tool_mid, 'id': 'call_fixture'}
                emit('session.tool.input.started', dict(ref, name='bash'))
                emit('session.tool.called', dict(ref, input=tool['state']['input'], executed=True))
                request = {'id': 'per_' + str(time.time_ns()), 'sessionID': sid, 'action': 'bash', 'resources': ['printf fixture'], 'source': {'type': 'tool', 'messageID': tool_mid, 'id': 'call_fixture'}}
                session['permissions'].append(request)
                save()
                emit('permission.asked', request)
            while session['permissions'] and not session.get('cancelled'):
                time.sleep(.01)
            with lock:
                if session['decisions'] and session['decisions'][-1] == 'once':
                    content = [{'type': 'text', 'text': 'fixture tool output'}]
                    tool['state'] = {'status': 'completed', 'input': {'command': 'printf fixture'}, 'content': content}
                    emit('session.tool.success', dict(ref, content=content, executed=True))
                else:
                    tool['state'] = {'status': 'error', 'input': {'command': 'printf fixture'}, 'error': {'message': 'rejected'}}
                    emit('session.tool.failed', dict(ref, error={'message': 'rejected'}, executed=False))
                save()
        if item['text'] == 'form':
            with lock:
                sid = session['info']['id']
                mid = 'msg_question_' + str(time.time_ns())
                tool = {'type': 'tool', 'id': 'call_question', 'name': 'question', 'state': {'status': 'running', 'input': {}}}
                session['messages'].append({'id': mid, 'type': 'assistant', 'content': [tool]})
                ref = {'sessionID': sid, 'assistantMessageID': mid, 'id': 'call_question'}
                emit('session.tool.input.started', dict(ref, name='question'))
                emit('session.tool.called', dict(ref, input={}, executed=True))
                form = {'id': 'frm_' + str(time.time_ns()), 'sessionID': sid, 'title': 'fixture', 'metadata': {'kind': 'question', 'tool': {'messageID': mid, 'id': 'call_question'}}, 'fields': [{'key': 'answer', 'type': 'string', 'title': 'Choose', 'description': 'Please choose an answer', 'required': True, 'custom': True}]}
                session['forms'].append(form)
                session.setdefault('formHistory', {})[form['id']] = dict(form, state={'status': 'pending'})
                emit('form.created', {'form': form})
                save()
            while session['forms'] and not session.get('cancelled'):
                time.sleep(.01)
            with lock:
                tool['state'] = {'status': 'completed', 'input': {}, 'content': [{'type': 'text', 'text': 'question settled'}]}
                emit('session.tool.success', dict(ref, content=tool['state']['content'], executed=True))
                save()
        while not (root / 'release').exists() and not session.get('cancelled'):
            time.sleep(.01)
        with lock:
            if session.get('cancelled'):
                session['running'] = False
                session['info']['time']['idle'] = time.time_ns()
                session['info']['outcome'] = 'interrupted'
                save()
                emit('session.execution.interrupted', {'sessionID': session['info']['id']})
                return
            mid = 'msg_assistant_' + str(time.time_ns())
            text = 'reply:' + item['text']
            emit('session.text.delta', {'sessionID': session['info']['id'], 'assistantMessageID': mid, 'ordinal': 0, 'delta': text})
            session['messages'].append({'id': mid, 'type': 'assistant', 'content': [{'type': 'text', 'text': text}], 'time': {'created': time.time_ns()}})
            if os.environ.get('NATIVE_FIXTURE_FAIL_AFTER_REPLY'):
                session['running'] = False
                session['info']['time']['idle'] = time.time_ns()
                session['info']['outcome'] = 'failed'
                emit('session.execution.failed', {'sessionID': session['info']['id']})
                save()
                return
            save()

class Api(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *args):
        pass
    def do_GET(self):
        self.handle_api()
    def do_POST(self):
        self.handle_api()
    def do_DELETE(self):
        self.handle_api()
    def do_PUT(self):
        self.handle_api()
    def reply(self, body=None, status=200):
        data = json.dumps(body).encode() if status != 204 else b''
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def handle_api(self):
        if self.headers.get('Authorization') != auth:
            self.reply({}, 401)
            return
        path = urllib.parse.urlsplit(self.path).path
        if os.environ.get('NATIVE_FIXTURE_REQUIRE_SCOPE') and (path in ['/api/model', '/api/model/default', '/api/agent', '/api/command', '/api/mcp'] or path.startswith('/api/experimental/mcp/')):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            if query.get('location[directory]') != [str(root)] or 'directory' in query:
                self.reply({}, 400)
                return
        if os.environ.get('NATIVE_FIXTURE_FAIL_CANCEL') and path.endswith('/permission'):
            self.reply({}, 503)
            return
        body = json.loads(self.rfile.read(int(self.headers['Content-Length']))) if int(self.headers.get('Content-Length', 0)) else {}
        if path == '/__fixture/events':
            with lock:
                if os.environ.get('NATIVE_FIXTURE_DISCONNECT'):
                    self.reply([{'_fixture_disconnect': True}])
                    return
                if os.environ.get('NATIVE_FIXTURE_REPLAY_OVERFLOW') and state['sessions'] and not state.get('overflowInjected'):
                    state['overflowInjected'] = True
                    sid = next(iter(state['sessions']))
                    original = {'id': 'evt_window_original', 'type': 'session.text.delta', 'data': {'sessionID': sid, 'assistantMessageID': 'msg_window', 'delta': 'window-text'}}
                    events = [original] + [{'id': 'evt_window_' + str(index), 'type': 'ignored', 'data': {'sessionID': sid}} for index in range(4096)] + [original] + list(backlog)
                    self.reply(events)
                    backlog.clear()
                    return
                self.reply(list(backlog))
                backlog.clear()
            return
        if path == '/api/event':
            events = queue.Queue()
            subscribers.append(events)
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            self.wfile.write(b': connected\n\n')
            self.wfile.flush()
            try:
                while True:
                    event = events.get(timeout=30)
                    self.wfile.write(('data: ' + json.dumps(event) + '\n\n').encode())
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, queue.Empty):
                subscribers.remove(events)
            return
        with lock:
            if path == '/api/session/active':
                self.reply({'data': {sid: {'type': 'running'} for sid, item in state['sessions'].items() if item.get('running')}})
                return
            if path.startswith('/api/experimental/mcp/'):
                state.setdefault('mcp', {})[path.split('/')[-1]] = body['config']
                save()
                self.reply(status=204)
                return
            if path == '/api/mcp':
                self.reply({'data': [{'name': name, 'status': {'status': 'connected'}} for name in state.get('mcp', {})]})
                return
            if path == '/api/model':
                state['modelListReads'] = state.get('modelListReads', 0) + 1
                names = ['fake', 'alt']
                if os.environ.get('NATIVE_FIXTURE_LATE_MODEL_LIST') and state['modelListReads'] < 3:
                    names = []
                if os.environ.get('NATIVE_FIXTURE_NEVER_ALT_MODEL'):
                    names = ['fake']
                if os.environ.get('NATIVE_FIXTURE_LATE_ALT_MODEL') and state['modelListReads'] < 4:
                    names = ['fake']
                self.reply({'data': [{'id': name, 'providerID': 'fixture', 'name': name, 'enabled': True, 'variants': [{'id': 'high'}], 'capabilities': {'input': ['text', 'image']}} for name in names]})
                return
            if path == '/api/model/default':
                if os.environ.get('NATIVE_FIXTURE_LATE_CATALOG'):
                    state['catalogReads'] = state.get('catalogReads', 0) + 1
                    if state['catalogReads'] < 3:
                        self.reply({'data': None})
                        return
                self.reply({'data': {'id': 'fake', 'providerID': 'fixture', 'name': 'Fake', 'enabled': True}})
                return
            if path == '/api/agent':
                self.reply({'data': [{'id': name, 'name': name, 'mode': 'primary', 'hidden': False} for name in ['build', 'plan']]})
                return
            if path == '/api/command':
                self.reply({'data': [{'name': 'fixture', 'description': 'Synthetic fixture command'}]})
                return
            if path == '/api/session':
                if self.command == 'POST':
                    sid = 'ses_' + str(time.time_ns())
                    info = {'id': sid, 'location': body['location'], 'agent': 'build', 'model': {'providerID': 'fixture', 'id': 'fake'}, 'time': {'created': time.time_ns(), 'updated': time.time_ns()}}
                    if os.environ.get('NATIVE_FIXTURE_DEFAULT_MODEL') or os.environ.get('NATIVE_FIXTURE_LATE_CATALOG'):
                        del info['model']
                    state['sessions'][sid] = {'info': info, 'messages': [], 'inbox': [], 'running': False, 'permissions': [], 'forms': [], 'decisions': [], 'submissions': 0}
                    save()
                    self.reply({'data': info})
                else:
                    rows = [item['info'] for item in state['sessions'].values()]
                    if os.environ.get('NATIVE_FIXTURE_BAD_CURSOR'):
                        self.reply({'data': rows, 'cursor': {'next': 42 if os.environ['NATIVE_FIXTURE_BAD_CURSOR'] == 'malformed' else 'repeated'}})
                    elif os.environ.get('NATIVE_FIXTURE_PAGED_LIST'):
                        if urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).get('cursor') == ['page2']:
                            self.reply({'data': [dict(rows[0], id='ses_saved', title='Saved session')], 'cursor': {}})
                        else:
                            self.reply({'data': rows, 'cursor': {'next': 'page2'}})
                    else:
                        self.reply({'data': rows})
                return
            parts = path.split('/')
            session = state['sessions'].get(parts[3]) if len(parts) >= 4 else None
            if not session:
                self.reply({}, 404)
                return
            if len(parts) == 4:
                if self.command == 'DELETE':
                    del state['sessions'][parts[3]]
                    save()
                    self.reply(status=204)
                else:
                    if os.environ.get('NATIVE_FIXTURE_LATE_TERMINAL_MEDIA') and session['info'].get('outcome') == 'succeeded' and not session.get('lateMediaInjected'):
                        session['lateMediaInjected'] = True
                        tool = {'type': 'tool', 'id': 'call_late_media', 'name': 'fixture_media', 'state': {'status': 'completed', 'input': {}, 'content': [{'type': 'file', 'mime': 'image/png', 'uri': 'https://example.invalid/private'}]}}
                        session['messages'].append({'id': 'msg_late_media', 'type': 'assistant', 'content': [tool]})
                        save()
                    self.reply({'data': session['info']})
            elif parts[4] == 'message':
                if os.environ.get('NATIVE_FIXTURE_BAD_HISTORY_CURSOR'):
                    session['historyPages'] = session.get('historyPages', 0) + 1
                    self.reply({'data': [], 'cursor': {'next': 'page_' + str(session['historyPages']) if os.environ['NATIVE_FIXTURE_BAD_HISTORY_CURSOR'] == 'limit' else 42}})
                    return
                if os.environ.get('NATIVE_FIXTURE_BAD_HISTORY'):
                    self.reply({'data': [{'id': 'msg_bad', 'type': 'user', 'text': {}}], 'cursor': {}})
                    return
                self.reply({'data': list(reversed(session['messages'])), 'cursor': {'previous': None, 'next': None}})
            elif parts[4] == 'compact':
                assert body['delivery'] == 'queue'
                session['compact'] = True
                compaction_id = 'msg_unrelated_compaction' if os.environ.get('NATIVE_FIXTURE_COMPACT_WRONG_ID') else body['id']
                session['messages'].append({'id': compaction_id, 'type': 'compaction', 'reason': 'manual', 'status': 'completed', 'summary': 'fixture summary'})
                session['info']['time']['idle'] = time.time_ns()
                session['info']['outcome'] = 'succeeded'
                save()
                self.reply({'data': {'id': body['id'], 'type': 'compaction'}})
            elif parts[4] in ['prompt', 'command']:
                assert body['delivery'] == 'queue'
                if parts[4] == 'command' and os.environ.get('NATIVE_FIXTURE_COMMAND_BEHAVIOR'):
                    behavior = os.environ['NATIVE_FIXTURE_COMMAND_BEHAVIOR']
                    self.reply({'data': {'id': 'msg_wrong'}} if behavior == 'wrong-ack' else None, 200 if behavior == 'wrong-ack' else 500 if behavior == 'rejected' else 204)
                    return
                if parts[4] == 'prompt':
                    assert body['resume'] is True
                    session['submissions'] += 1
                else:
                    session['command'] = body['name']
                    body['id'] = 'msg_command_' + str(time.time_ns())
                if body.get('files'):
                    session['submittedFiles'] = body['files']
                session['inbox'].append(body)
                session['cancelled'] = False
                if not session['running']:
                    session['running'] = True
                    threading.Thread(target=run, args=(session,), daemon=True).start()
                save()
                if parts[4] == 'command':
                    self.reply(status=204)
                else:
                    self.reply({'data': {'id': 'msg_uncertain' if os.environ.get('NATIVE_FIXTURE_UNCERTAIN_ACK') else body['id']}})
            elif parts[4] == 'inbox':
                if self.command == 'DELETE':
                    session['inbox'] = [item for item in session['inbox'] if item['id'] != parts[5]]
                    save()
                    if os.environ.get('NATIVE_FIXTURE_CANCEL_RACE'):
                        self.reply({}, 409)
                        return
                    self.reply(status=204)
                else:
                    self.reply({'data': session['inbox']})
            elif parts[4] == 'interrupt':
                assert urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)['resume'] == ['false']
                session['cancelled'] = True
                save()
                self.reply({'interrupted': session['running']})
            elif parts[4] in ['agent', 'model']:
                if parts[4] == 'model':
                    assert set(body['model']) <= {'providerID', 'id', 'variant'}, 'Model.Info is not Model.Ref'
                session['info'][parts[4]] = body[parts[4]]
                if parts[4] == 'model' and os.environ.get('NATIVE_FIXTURE_NORMALIZE_MODEL_VARIANT'):
                    session['info']['model'].setdefault('variant', 'default')
                save()
                self.reply(status=204)
            elif parts[4] == 'permission':
                if self.command == 'POST':
                    assert body['decision'] in ['once', 'reject']
                    session['decisions'].append(body['decision'])
                    session['permissions'] = [request for request in session['permissions'] if request['id'] != parts[5]]
                    save()
                    self.reply(status=204)
                else:
                    self.reply({'data': session['permissions']})
            elif parts[4] == 'form':
                if len(parts) == 5:
                    self.reply({'data': session['forms']})
                    return
                form = session.get('formHistory', {}).get(parts[5])
                if not form:
                    self.reply({}, 404)
                elif self.command == 'GET':
                    detail = dict(form)
                    if os.environ.get('NATIVE_FIXTURE_FORM_WRONG_READBACK') and detail['state']['status'] == 'answered':
                        detail['state'] = {'status': 'answered', 'answer': {'answer': 'WRONG'}}
                    self.reply({'data': detail})
                elif form['state']['status'] != 'pending':
                    self.reply({}, 409)
                elif self.command == 'DELETE':
                    session['formCancelled'] = True
                    form['state'] = {'status': 'cancelled'}
                    session['forms'] = [item for item in session['forms'] if item['id'] != parts[5]]
                    save()
                    self.reply(status=204)
                elif self.command == 'POST' and len(parts) == 7 and parts[6] == 'reply':
                    if 'answer' not in body or not isinstance(body['answer'], dict):
                        self.reply({}, 400)
                        return
                    answer = body['answer']
                    session['formReplyCount'] = session.get('formReplyCount', 0) + 1
                    session['formAnswers'] = answer
                    form['state'] = {'status': 'answered', 'answer': answer}
                    session['forms'] = [item for item in session['forms'] if item['id'] != parts[5]]
                    save()
                    self.reply(status=500 if os.environ.get('NATIVE_FIXTURE_FORM_LOST_ACK') else 204)
                else:
                    self.reply({}, 405)
            else:
                self.reply({}, 404)

if os.environ.get('BRIDGE_TEST_NATIVE_STDIO'):
    class StdioApi(Api):
        def __init__(self, request):
            self.path = request['path']
            self.command = request['method']
            body = json.dumps(request.get('body') or {}).encode()
            self.headers = {'Authorization': auth, 'Content-Length': str(len(body))}
            self.rfile = io.BytesIO(body)
            self.result = None
        def reply(self, body=None, status=200):
            self.result = {'status': status, 'body': body}
    print(json.dumps({'url': 'http://127.0.0.1:4096'}), flush=True)
    for line in sys.stdin:
        request = StdioApi(json.loads(line))
        request.handle_api()
        print(json.dumps(request.result), flush=True)
    (root / 'exited').touch()
    sys.exit(0)
server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Api)
threading.Thread(target=server.serve_forever, daemon=True).start()
print(json.dumps({'url': 'http://127.0.0.1:' + str(server.server_port)}), flush=True)
sys.stdin.read()
server.shutdown()
