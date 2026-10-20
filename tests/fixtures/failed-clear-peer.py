"""Isolated newline JSON-RPC peer for failed-clear contract tests."""
import json
import os
import sys


def send(value):
    print(json.dumps(value), flush=True)


for line in sys.stdin:
    request = json.loads(line)
    with open(os.environ["CLEAR_AUDIT"], "a", encoding="utf-8") as audit:
        audit.write(json.dumps(request) + "\n")
    method = request.get("method")
    params = request.get("params", {})
    if "id" not in request:
        continue
    if method == "initialize":
        result = {"protocolVersion": 1, "agentCapabilities": {"loadSession": True}}
    elif method == "session/new":
        result = {"sessionId": "clear-fixture"}
    elif method == "session/prompt":
        send({"jsonrpc": "2.0", "method": "session/update", "params": {
            "sessionId": params["sessionId"], "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "clear fixture answer"}}}})
        result = {"stopReason": "end_turn"}
    elif method == "session/delete":
        send({"jsonrpc": "2.0", "id": request["id"], "error": {
            "code": -32000, "message": "injected deletion failure"}})
        continue
    else:
        result = {}
    send({"jsonrpc": "2.0", "id": request["id"], "result": result})
