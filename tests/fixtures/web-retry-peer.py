"""Synthetic ACP peer: first prompt empty, second recovered; never executes tools."""
import json
import os
import sys
from pathlib import Path


def send(value):
    print(json.dumps(value), flush=True)


for line in sys.stdin:
    request = json.loads(line)
    with open(os.environ["WEB_RETRY_AUDIT"], "a", encoding="utf-8") as audit:
        audit.write(json.dumps(request) + "\n")
    if "id" not in request:
        continue
    method = request.get("method")
    params = request.get("params", {})
    if method == "initialize":
        result = {"protocolVersion": 1, "agentCapabilities": {"loadSession": True}}
    elif method == "session/new":
        result = {"sessionId": "web-fixture-" + str(os.getpid())}
    elif method == "session/prompt":
        counter = Path(os.environ["WEB_RETRY_COUNTER"])
        count = int(counter.read_text()) + 1 if counter.exists() else 1
        counter.write_text(str(count))
        if count > 1:
            send({"jsonrpc": "2.0", "method": "session/update", "params": {
                "sessionId": params["sessionId"], "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "recovered answer"}}}})
        result = {"stopReason": "end_turn"}
    else:
        result = {}
    send({"jsonrpc": "2.0", "id": request["id"], "result": result})
