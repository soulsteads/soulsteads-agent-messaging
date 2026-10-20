"""Synthetic ACP tool stream; never executes the advertised commands."""
import json
import os
import sys


def send(value):
    print(json.dumps(value), flush=True)


for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    if method == "initialize":
        result = {"protocolVersion": 1, "agentCapabilities": {}}
    elif method == "session/new":
        result = {"sessionId": "terminal-fixture"}
    elif method == "session/prompt":
        for update in json.loads(os.environ["TERMINAL_UPDATES"]):
            send({"jsonrpc": "2.0", "method": "session/update", "params": {
                "sessionId": "terminal-fixture", "update": update}})
        send({"jsonrpc": "2.0", "method": "session/update", "params": {
            "sessionId": "terminal-fixture", "update": {"sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "Final fixture answer"}}}})
        result = {"stopReason": "end_turn"}
    else:
        result = {}
    if "id" in request:
        send({"jsonrpc": "2.0", "id": request["id"], "result": result})
