"""Audited isolated Matrix ACP peer, including a real pending prompt boundary.

This is a protocol fixture, not a model, production identity, or native history.
"""
import json
import os
import sys
import threading
import time
import uuid

fd = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
output_lock = threading.Lock()
stop = threading.Event()
workers = []

def emit(message):
    with output_lock:
        print(json.dumps(message), flush=True)

def audit(value):
    value["at"] = time.monotonic()
    os.write(fd, (json.dumps(value) + "\n").encode())

def reply(request, value):
    emit({"jsonrpc": "2.0", "id": request["id"], "result": value})

def complete(request, text):
    params = request["params"]
    if text == "ATTACHMENT-ORIGINAL":
        stop.wait(0.7)
    if text.startswith("Attached document"):
        audit({"method":"fixture/waiting-file"})
        stop.wait(5.0)
    emit({"jsonrpc": "2.0", "method": "session/update", "params": {
        "sessionId": params["sessionId"], "update": {
            "sessionUpdate": "agent_message_chunk", "content": {
                "type": "text", "text": text}}}})
    audit({"method": "fixture/settled", "text": text, "sessionId": params["sessionId"]})
    reply(request, {"stopReason": "end_turn"})

try:
    for line in sys.stdin:
        request = json.loads(line)
        method, params = request.get("method"), request.get("params", {})
        record = {"method": method, "sessionId": params.get("sessionId")}
        if method == "session/prompt":
            record["text"] = params["prompt"][0]["text"]
        if method == "session/new":
            record["sessionId"] = str(uuid.uuid4())
        audit(record)
        if method == "initialize":
            reply(request, {"protocolVersion": 1, "agentCapabilities": {
                "loadSession": True}, "agentInfo": {"name": "opencode"}})
        elif method == "session/new":
            reply(request, {"sessionId": record["sessionId"]})
        elif method in ("session/load", "session/resume"):
            reply(request, {})
        elif method == "session/prompt":
            worker = threading.Thread(target=complete, args=(request, record["text"]), daemon=True)
            workers.append(worker)
            worker.start()
        elif method == "session/cancel":
            stop.set()
        elif "id" in request:
            reply(request, {})
finally:
    stop.set()
    for worker in workers:
        worker.join(timeout=3)
    os.close(fd)
