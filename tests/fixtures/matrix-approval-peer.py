"""Isolated ACP peer; records actual permission replies, no shell execution."""
import json
import os
import sys


def send(value):
    print(json.dumps(value), flush=True)


pending_prompt = None
original_prompt = None
for line in sys.stdin:
    message = json.loads(line)
    with open(os.environ["APPROVAL_AUDIT"], "a", encoding="utf-8") as audit:
        audit.write(json.dumps(message) + "\n")
    method = message.get("method")
    if method == "initialize":
        result = {"protocolVersion": 1, "agentCapabilities": {}}
    elif method == "session/new":
        result = {"sessionId": "approval-session"}
    elif method == "session/prompt":
        mode = os.environ.get("APPROVAL_MODE")
        if mode == "quiet":
            continue
        if mode == "followup" and original_prompt is None:
            original_prompt = message["id"]
            send({"jsonrpc": "2.0", "method": "session/update", "params": {
                "sessionId": "approval-session", "update": {"sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "original-ready"}}}})
            continue
        pending_prompt = message["id"]
        params = {"sessionId": "approval-session", "toolCall": {
                  "toolCallId": "one-operation", "kind": "execute", "title": "fixture command",
                  "rawInput": {"command": "printf approval-fixture"}}, "options": [
                      {"kind": "allow_once", "optionId": "native-once"},
                      {"kind": "reject_once", "optionId": "native-deny"}]}
        params.update(json.loads(os.environ.get("APPROVAL_REQUEST", "{}")))
        request = {"jsonrpc": "2.0", "id": 900, "method": "session/request_permission", "params": params}
        send(request)
        send({"jsonrpc": "2.0", "method": "session/update", "params": {
            "sessionId": "approval-session", "update": {"sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": "reader-progress"}}}})
        mode = os.environ.get("APPROVAL_MODE")
        if mode == "followup":
            send({"jsonrpc": "2.0", "id": original_prompt, "result": {"stopReason": "end_turn"}})
        elif mode == "duplicate":
            send(request)
        elif mode == "complete":
            send({"jsonrpc": "2.0", "id": pending_prompt, "result": {"stopReason": "end_turn"}})
            pending_prompt = None
        elif mode == "exit":
            sys.exit(0)
        continue
    elif method == "fixture/hang":
        continue
    elif method == "session/list":
        result = {"sessions": []}
    elif method == "session/cancel":
        if pending_prompt is not None:
            send({"jsonrpc": "2.0", "id": pending_prompt, "result": {"stopReason": "cancelled"}})
            pending_prompt = None
        continue
    elif method is None and message.get("id") == 900:
        if os.environ.get("APPROVAL_MODE") == "hold_after_decision":
            continue
        if pending_prompt is not None:
            send({"jsonrpc": "2.0", "id": pending_prompt, "result": {"stopReason": "end_turn"}})
            pending_prompt = None
        continue
    else:
        result = {}
    if "id" in message:
        send({"jsonrpc": "2.0", "id": message["id"], "result": result})
